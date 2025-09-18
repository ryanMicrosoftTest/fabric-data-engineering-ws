# Copyright (c) Microsoft Corporation. All rights reserved.
import copy
import distro
import errno
import glob
import logging
import os
import re
import shutil
import sys
import tarfile
import tempfile
import threading
import time
import ssl
from subprocess import run, PIPE
from typing import List, Optional, Tuple
from urllib import request
from urllib.error import HTTPError

logger = logging.getLogger('dotnetcore2')
logger.setLevel(logging.WARNING)
handler = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)


def _set_logging_level(level):
    logger.setLevel(level)


def _enable_debug_logging():
    _set_logging_level(logging.DEBUG)


def _disable_debug_logging():
    _set_logging_level(logging.WARNING)


__version__ = '3.1.23'  # {major dotnet version}.{minor dotnet version}.{revision}
# We can rev the revision due to patch-level change in .net or changes in dependencies

deps_url_base = 'https://azuremldownloads.azureedge.net/dotnetcore2-dependencies/' + __version__ + '/'
dist = None
version = None
if sys.platform == 'linux':
    dist = distro.id()
    version = distro.version_parts()


def _get_bin_folder() -> str:
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), 'bin')


def get_runtime_path():
    search_string = os.path.join(_get_bin_folder(), 'dotnet*')
    matches = [f for f in glob.glob(search_string, recursive=True)]
    return matches[0]


def _handle_not_writable(e: OSError):
    if e.errno == errno.EACCES or e.errno == errno.EPERM:
        logger.debug('[_handle_not_writable()] {}'.format(e))
        err_msg = "No write permission to python environment, can't download .NET Core Dependencies."
        raise RuntimeError(err_msg)


_default_deps_path = os.path.join(_get_bin_folder(), 'deps')
_backup_deps_path = os.path.join(tempfile.gettempdir(), 'azureml', 'dotnetcore2', 'deps')
_deps_path = None
_deps_path_lock = threading.Lock()

_unsupported_help_msg = \
""".NET Core 3.1 can still be used via `dotnetcore2` if the required dependencies are installed.
Visit https://aka.ms/dotnet-install-linux for Linux distro specific .NET Core install instructions.
Follow your distro specific instructions to install `dotnet-runtime-*` and replace `*` with `3.1.23`.
"""

def write_test(path: str):
    """Try Create a file at `path`"""
    os.makedirs(path, exist_ok=True)
    write_test_path = os.path.join(path, 'WRITETEST')
    with open(write_test_path, 'w+') as f:
        f.write('It worked!')


def _get_deps_folder() -> str:
    global _deps_path
    deps_path = _default_deps_path
    try:
        if _deps_path is None:
            _deps_path_lock.acquire()
            if _deps_path is None:
                write_test(deps_path)
                _deps_path = deps_path
    except Exception as e:
        import tempfile
        logger.debug('[_get_deps_folder()] Exception while trying to write to default deps path. Trying temp dir.')
        deps_path = _backup_deps_path
        try:
            write_test(deps_path)
            _deps_path = deps_path
        except OSError as e:
            _handle_not_writable(e)
            raise
        except Exception as e:
            logger.debug(
                '[_get_deps_folder()] Unexpected Exception when trying to make deps dirs in temp folder: {}'.format(e))
    finally:
        if _deps_path_lock.locked():
            _deps_path_lock.release()
    logger.debug('[_get_deps_folder()] Using deps_path: {}'.format(_deps_path))
    return _deps_path


class _FileLock:
    def __init__(self, file_path, timeout=60, raise_on_timeout=None):
        self.locked = False
        self.lockfile_path = file_path
        self.timeout = timeout
        self.raise_on_timeout = raise_on_timeout
        self.wait = 0.5

    def acquire(self):
        # Set max_retries to line up approximately with self.timeout
        max_retries = self.timeout / self.wait

        def fail():
            if self.raise_on_timeout is not None:
                raise raise_on_timeout
            else:
                return False

        while True:
            try:
                self.lockfile = os.open(self.lockfile_path, os.O_CREAT | os.O_EXCL | os.O_RDWR)
                self.locked = True
                break
            except OSError as e:
                if e.errno != errno.EEXIST:
                    logger.debug('Unexpected Exception when trying to open lockfile: {}'.format(e))
                    _handle_not_writable(e)
                    raise
                # Get last last modified time of lockfile, the call could throw if lockfile has been deleted since we tried to open it.
                lockfile_last_modified = None
                try:
                    lockfile_last_modified = os.path.getmtime(self.lockfile_path)
                except OSError as e:
                    if e.errno != errno.ENOENT:
                        logger.debug('Unexpected Exception when calling getmtime on lockfile: {}'.format(e))
                        raise
                    # lockfile no longer exists, make sure to count retries of this race condition,
                    # lets try to open it again, unless we've retried to many times.
                    max_retries -= 1
                    if max_retries <= 0:
                        fail()
                    continue
                # lockfile still exists, check if we've waited longer than timeout to acquire it.
                if (time.time() - lockfile_last_modified) > self.timeout:
                    try:
                        os.unlink(self.lockfile_path)
                    except:
                        fail()
                # Haven't waited longer than timeout, so sleep and try again.
                time.sleep(self.wait)
        return True

    def release(self):
        if self.locked:
            try:
                os.close(self.lockfile)
                os.unlink(self.lockfile_path)
            except:
                pass # Best effort for close and unlink.
            self.locked = False

    def __enter__(self):
        if not self.locked:
            self.acquire()
        return self

    def __exit__(self, type, value, traceback):
        if self.locked:
            self.release()

    def __del__(self):
        self.release()


def already_succeeded():
    """Check if a SUCCESS file has been written to expected paths"""
    success_file = os.path.join(_default_deps_path, 'SUCCESS-' + __version__)
    try:
        if os.path.exists(success_file):
            return _default_deps_path
    except:
        pass
    success_file = os.path.join(_backup_deps_path, 'SUCCESS-' + __version__)
    try:
        if os.path.exists(success_file):
            return _backup_deps_path
    except:
        pass
    return None


def _try_write_success():
    try:
        success_file = os.path.join(_get_deps_folder(), 'SUCCESS-' + __version__)
        os.makedirs(os.path.dirname(success_file), exist_ok=True)
        with open(success_file, 'a'):
            os.utime(success_file, None)
    except Exception as e:
        # There aren't any missing dependencies, but we can't write a success file, ignore.
        logger.debug('Exception while writing success file: {}'.format(e))
        pass


def ensure_dependencies() -> Optional[str]:
    if dist is None:
        return None

    # Fast path, we've already written a SUCCESS file so return deps_path
    deps_path = already_succeeded()
    if deps_path:
        return deps_path

    # Happy path, `ldd` can find  all libraries .NET Core depends on
    # Try to write SUCCESS (best effort), return empty dep_path
    bin_folder = _get_bin_folder()
    missing_pkgs = _gather_dependencies(bin_folder)
    if not missing_pkgs:
        _try_write_success()
        return ''

    # Unfortunate Path, `ldd` found missing libraries, download our pre-prepared dependency set (we might not have one)
    logger.debug('Missing pkgs: {}'.format(missing_pkgs))
    deps_path = _get_deps_folder()
    deps_tar_path = deps_path + '.tar'
    deps_lock_path = deps_path + '.lock'
    with _FileLock(deps_lock_path, raise_on_timeout=RuntimeError(
            'Unable to retrieve .NET dependencies. Another python process may be trying to retrieve them at the same time.')):
        # Check if someone else got deps while we were locking.
        succeeded_deps_path = already_succeeded()
        if succeeded_deps_path:
            return succeeded_deps_path

        # There are missing dependencies, remove any previous state and download deps.
        shutil.rmtree(deps_path, ignore_errors=True)

        deps_url = _construct_deps_url(deps_url_base)
        logger.debug("Constructed deps url: {}".format(deps_url))
        try:
            import certifi
            cert_path = os.path.join(os.path.dirname(certifi.__file__), 'cacert.pem')
            if os.path.isfile(cert_path):
                cafile = cert_path
        except Exception:
            cafile = None

        def blob_deps_to_file():
            ssl_context = ssl.create_default_context(cafile=cafile)
            blob = request.urlopen(deps_url, context=ssl_context)
            with open(deps_tar_path, 'wb') as f:
                f.write(blob.read())
                blob.close()

        def attempt_get_deps(missing_pkgs):
            success = False
            try:
                blob_deps_to_file()
                success = True
            except HTTPError as e:
                logger.debug("Error Code when accessing deps_url: {}".format(e.code))
                if e.code == 404:
                    # Requested blob not found so we don't have deps for this distribution.
                    err_msg = 'Linux distribution {0} {1}.{2} does not have automatic support. \nMissing packages: {3}'.format(
                        dist, version[0], version[1], missing_pkgs)
                    raise NotImplementedError(err_msg + '\n' + _unsupported_help_msg)
            except Exception as e:
                logger.debug("Exception when accessing blob: " + str(e))
                success = False
            return success

        if not attempt_get_deps(missing_pkgs):
            # Failed accessing blob, likely an interrupted connection. Try again once more.
            if not attempt_get_deps(missing_pkgs):
                err_msg = 'Unable to retrieve .NET dependencies. Please make sure you are connected to the Internet and have \
                    a stable network connection. \nMissing packages: {}\n{}'.format(missing_pkgs, _unsupported_help_msg)
                raise RuntimeError(err_msg)

        with tarfile.open(deps_tar_path, 'r') as tar:
            tar.extractall(path=os.path.dirname(deps_path))

        os.remove(deps_tar_path)

        _try_write_success()
    # This is for backwards compat with `azureml-dataprep` <= 2.3.* which rely on `LD_LIBRARY_PATH` being updated in global env.
    os.environ['LD_LIBRARY_PATH'] = deps_path
    return deps_path


def _construct_deps_url(base_url: str) -> str:
    return base_url + dist + '/' + version[0] + '/' + 'deps.tar'


missing_dep_re = re.compile(r'^(.+)\s*=>\s*not found\s*$', re.MULTILINE)


def _gather_dependencies(path: str, search_path: str = None) -> List[Tuple[str]]:
    libraries = glob.glob(os.path.realpath(os.path.join(path, '**', '*.so')), recursive=True)
    missing_deps = set()
    env = copy.copy(os.environ)
    if search_path is not None:
        env['LD_LIBRARY_PATH'] = search_path
    for library in libraries:
        ldd_output = run(['ldd', library], cwd=path, stdout=PIPE, env=env).stdout.decode('utf-8')
        matches = missing_dep_re.findall(ldd_output)
        missing_deps |= set(dep.strip() for dep in matches)

    return missing_deps
