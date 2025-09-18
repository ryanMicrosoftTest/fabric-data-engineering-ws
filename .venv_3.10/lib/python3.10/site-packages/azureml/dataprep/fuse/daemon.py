import gc
import os
import pickle
import shutil
import subprocess
import sys
import tempfile
import threading
from time import sleep, perf_counter
from typing import Optional

import azureml.dataprep as dprep
from azureml.dataprep import DataPrepException
from azureml.dataprep.api._loggerfactory import _LoggerFactory, session_id, track, trace
from azureml.dataprep.api.tracing._context import Context
from azureml.dataprep.fuse._logger_helper import get_trace_with_invocation_id, _log_resource_info, _log_process_info

logger = None
tracer = trace.get_tracer(__name__)

def get_logger():
    global logger
    if logger is not None:
        return logger

    logger = _LoggerFactory.get_logger("dprep.fuse")
    return logger

class MountContext(object):
    """Context manager for mounting dataflow.

    .. remarks::

        Upon entering the context manager, the dataflow will be mounted to the mount_point. Upon exit, it will
        remove the mount point and clean up the daemon process used to mount the dataflow.

        An example of how to mount a dataflow is demonstrated below:

        .. code-block:: python

            import azureml.dataprep as dprep
            from azureml.dataprep.fuse.dprepfuse import mount
            import os
            import tempfile

            mount_point = tempfile.mkdtemp()
            dataflow = dprep.Dataflow.get_files('https://dprepdata.blob.core.windows.net/demo/Titanic.csv')
            with mount(dataflow, 'Path', mount_point, foreground=False):
                print(os.listdir(mount_point))

    :param dataflow: The dataflow to be mounted.
    :param files_column: The name of the column that contains the StreamInfo.
    :param mount_point: The directory to mount the dataflow to.
    :param base_path: The base path to resolve the new relative root.
    :param options: Mount options.
    :param invocation_id: The invocation ID to correlate the subprocess and main process.
    :param destination: The destination to write the output to.
    """

    def __init__(self, dataflow, files_column: str, mount_point: str,
                 base_path: str = None, options: 'MountOptions' = None, invocation_id: str = None,
                 destination: tuple = None, span_context: Optional[Context] = None,
                 **kwargs):
        """Constructor for the context manager.

        :param dataflow: The dataflow to be mounted.
        :param files_column: The name of the column that contains the StreamInfo.
        :param mount_point: The directory to mount the dataflow to.
        :param base_path: The base path to resolve the new relative root.
        :param options: Mount options.
        :param invocation_id: The invocation ID to correlate the subprocess and main process.
        :param destination: The destination to write the output to.
        :param span_context: The span context to use to construct other spans.
        """
        from azureml.dataprep.fuse.dprepfuse import MountOptions

        self._dataflow = dataflow
        self._files_column = files_column
        self._mount_point = mount_point
        self._base_path = base_path
        self._options = options or MountOptions()  # type: MountOptions
        self._process = None  # type: Optional[subprocess.Popen]
        self._entered = False
        self._invocation_id = invocation_id
        self._trace = get_trace_with_invocation_id(get_logger(), self._invocation_id)
        self._destination = destination
        self._pickle_path = None
        self._span_context = span_context
        self._cli_file = kwargs.get('daemon_file') or os.path.join(os.path.dirname(__file__), 'cli.py')
        self._init_event_path = None

        # set this to ~5 minutes until we know the reason why it takes a long time to launch a child process in VMs
        # in specific regions.
        self._max_attempt = kwargs.get('max_attempt', 34)  # total wait time of 297.5 seconds. 297.5 = 34 * (35 + 1) / 4
        self._sleep_time = kwargs.get('sleep_time', 0.5)  # seconds

        if not self._options.data_dir:
            self._options.data_dir = tempfile.mkdtemp()

    @property
    def mount_point(self):
        """Get the mount point."""
        return self._mount_point

    def start(self):
        """Mount the file streams.

        This is equivalent to calling the MountContext.__enter__ instance method.
        """
        self.__enter__()

    def stop(self):
        """Unmount the file streams.

        This is equivalent to calling the MountContext.__exit__ instance method.
        """
        self.__exit__()

    @track(get_logger)
    def __enter__(self):
        """Mount the file streams.

        :return: The current context manager.
        :rtype: azureml.dataprep.fuse.daemon.MountContext
        """
        if self._entered:
            self._trace('already entered, skipping mounting again.')
        else:
            with tracer.start_as_current_span("mount", parent=self._span_context,
                                              user_facing_name='Launching Mount Daemon'):
                self._retry_mount()
            self._entered = True
        return self

    @track(get_logger)
    def __exit__(self, *args, **kwargs):
        """Unmount the file streams"""
        if not self._entered:
            self._trace('tried to exit without actually entering.')
            return

        try:
            self._trace('exiting MountContext')
            self._unmount()
            if self._process:
                self._trace('terminating daemon process')
                self._process.terminate()
                self._process = None
            else:
                logger.warning('daemon process not found')
            self._remove_mount()
            self._remove_data_dir()
            self._trace('finished exiting')
        except:
            logger.error('failed to unmount(%s)', self._invocation_id)
        finally:
            self._entered = False

    def _mount_using_daemon(self):
        python_path = sys.executable
        _, dataflow_path = tempfile.mkstemp()
        _, self._pickle_path = tempfile.mkstemp()
        span = trace.get_current_span()
        _, self._init_event_path = tempfile.mkstemp()

        dest = None
        if self._destination:
            dest = (
                dprep.api._datastore_helper._serialize_datastore(self._destination[0]),
                self._destination[1]
            )

        # TODO: Change this to use a mechanism that doesn't involve writing to disk
        with open(self._pickle_path, 'wb') as f:
            pickle.dump({
                'files_column': self._files_column,
                'mount_point': self._mount_point,
                'base_path': self._base_path,
                'options': self._options,
                'invocation_id': self._invocation_id,
                'caller_session_id': session_id,
                'spawn_process_timestamp': perf_counter(),
                'dest': dest,
                'span_context': span.get_context()
            }, f)
        if self._dataflow:
            self._dataflow.save(dataflow_path)

        # subprocess.Popen is using fork internally to spawn a new process.
        # Even with copy-on-write fork may try to commit for the same amount of virtual memory the parent process is using.
        # This may lead to a failure to spawn a new process because of the memory error
        # "Unable to mount due to an unknown exception OSError with message [Errno 12] Cannot allocate memory.".
        try:
            gc.collect()
        except Exception as e:
            logger.warning("GC before spawning new mount process failed with the error {}".format(repr(e)))

        self._process = subprocess.Popen([
            python_path,
            '-u',  # disable stdout buffering from subprocess to stream print from it in real time
            self._cli_file,
            '--dataflow-path', dataflow_path,
            '--mount-args-path', self._pickle_path,
            '--init-events-path', self._init_event_path
        ],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT
        )

        # Start a daemonic thread to stream stdout from subprocess so that its print will be visible in current
        # cell's output in Jupyter Notebook. This is required when running credential less mount on machine without
        # browser (e.g. AML Compute Instance) to display device code login message.
        def stream_stdout():
            received_output = False
            for line in iter(self._process.stdout.readline, b''):
                line = line.decode().rstrip()
                if not received_output:
                    self._trace('Received {} bytes from child process (pid: {}) stdout'.format(len(line), self._process.pid))
                    received_output = True
                print(line)

        threading.Thread(target=stream_stdout, daemon=True).start()
        self._log_resource_and_process_info()

    def _wait_until_mounted(self):
        attempt = 1

        from azureml.dataprep.fuse.dprepfuse import SENTINEL_FILE_NAME
        full_sentinel_path = os.path.join(self.mount_point, SENTINEL_FILE_NAME)

        start = perf_counter()
        while not os.path.exists(self.mount_point) or not os.path.exists(full_sentinel_path):
            if attempt > self._max_attempt:
                error_msg = 'Waiting for mount point to be ready has timed out. Check if fuse device is available on your system.\n' \
                            'If mounting on remote targets, see ' \
                            'https://docs.microsoft.com/azure/machine-learning/how-to-train-with-datasets#mount-files-to-remote-compute-targets'

                self._remove_pickle()

                try:
                    os_info = os.uname()
                    sysname = os_info.sysname.lower()
                    release = os_info.release.lower()
                    version = os_info.version.lower()

                    if sysname == 'linux' and ('microsoft' in release or 'wsl' in release) and (
                            'microsoft' in version or 'wsl' in version):
                        error_msg += '\nWarning: Fuse only available on Windows subsystem for Linux starting from version 2.\n'
                except:
                    pass

                error_msg += ' Session ID: {}'.format(session_id)
                logger.error(error_msg)
                raise FuseTimeoutException(error_msg)

            self._ensure_child_process()
            sleep(self._sleep_time * attempt)
            attempt += 1

        elapsed = perf_counter() - start
        daemon_session_id = "none"
        try:
            with open(full_sentinel_path) as sentinel:
                daemon_session_id = sentinel.readline()
        except Exception as e:
            logger.warning("Failed to read sentinel file: {}".format(e))
            pass
        self._trace('Launching daemon process took {} seconds\nDaemon process Session Id: {}'
                    .format(elapsed, daemon_session_id), {
                        'elapsed': elapsed,
                        'attempt': attempt,
                        'daemon_session_id': daemon_session_id,
                    })
        self._remove_pickle()

    def _remove_pickle(self):
        try:
            if self._pickle_path:
                os.remove(self._pickle_path)
                self._pickle_path = None
        except FileNotFoundError:
            logger.warning('Attempted to remove the pickled path but the path doesn\'t exist.')

    def _ensure_child_process(self):
        exit_code = self._process.poll()
        if exit_code is None:
            return
        _LoggerFactory.trace(logger, 'Mount child process has exited with code {}.'.format(exit_code))
        raise FuseProcessTerminatedException(exit_code)

    def _unmount(self):
        try:
            self._trace('trying to call fusermount. In /tmp/? {}'.format(self.mount_point.startswith('/tmp/')))
            subprocess.check_call(['fusermount', '-u', self.mount_point])
        except:
            try:
                self._trace('trying to call umount. In /tmp/? {}'.format(self.mount_point.startswith('/tmp/')))
                subprocess.check_call(['umount', self.mount_point])
            except subprocess.CalledProcessError:
                logger.info('Unable to execute umount command. Continuing normal execution as this is non fatal.')

    def _remove_mount(self):
        try:
            self._trace('trying to remove mount point')
            if not os.path.exists(self.mount_point):
                self._trace('mount point does not exist')
                return
            shutil.rmtree(self.mount_point)
            self._trace('successfully removed mount point')
        except:
            logger.warning('Non-fatal error: failed to remove mount point {}.'.format(self.mount_point))

    def _remove_data_dir(self):
        try:
            self._trace('trying to remove data dir')
            if not os.path.exists(self._options.data_dir):
                self._trace('data dir does not exist')
                return
            shutil.rmtree(self._options.data_dir)
            self._trace('successfully removed data dir')
        except:
            logger.warning('Non-fatal error: failed to remove cache directory {}.'.format(self._options.data_dir))

    def _retry_mount(self):
        max_attempts = 3
        current_attempt = 1
        wait_timeout = 10
        while current_attempt <= max_attempts:
            try:
                logger.debug('Mounting daemon process attempt {}.'.format(current_attempt))
                self._mount_using_daemon()
                self._wait_until_mounted()
                break
            except FuseTimeoutException:
                self._log_resource_and_process_info()
                self._log_init_events()

                if current_attempt == max_attempts:
                    logger.error('Failed to mount after {} attempts.'.format(current_attempt))
                    raise

                self._process.kill()
                try:
                    self._process.wait(wait_timeout)
                except subprocess.TimeoutExpired:
                    logger.warning('Warning: Unable to kill mount daemon process after waiting for {} seconds.'.format(
                        wait_timeout
                    ))
                    self._log_resource_and_process_info()

                successful_clean = threading.Event()
                t = threading.Thread(target=self.__class__._cleanup_mount_point, args=(self._mount_point, successful_clean,),
                                     daemon=True)
                t.start()
                t.join(wait_timeout)

                if not successful_clean.is_set():
                    logger.error('Failed to mount as we were unable to clean up previous mount. Tried to mount {} time(s).'.format(current_attempt))
                    raise

                current_attempt += 1
            except FuseProcessTerminatedException:
                self._log_resource_and_process_info()
                self._log_init_events()
                raise
            except Exception as e:
                logger.error('Unable to mount due to an unknown exception {} with message {}.'.format(type(e).__name__, e))
                self._log_resource_and_process_info()
                self._log_init_events()
                raise
        _LoggerFactory.trace(logger, 'Successfully mounted process after {} attempts'.format(current_attempt), {
            'attempt': current_attempt
        })

    def _log_resource_and_process_info(self):
        _log_resource_info(logger)
        _log_process_info(logger, os.getpid())
        if self._process:
            _log_process_info(logger, self._process.pid)

    def _log_init_events(self):
        try:
            child_pid = ''
            if self._process:
                child_pid = str(self._process.pid)
            with open(self._init_event_path) as f:
                for line in f.readlines():
                    _LoggerFactory.trace(logger, '[child process] - {}'.format(line), {
                        'child_pid': child_pid
                    })
        except Exception as e:
            _LoggerFactory.trace(logger, 'Failed to read init events from child process due to {}'.format(type(e).__name__))

    @staticmethod
    def _cleanup_mount_point(path: str, successful_clean: threading.Event):
        import shutil

        shutil.rmtree(path)
        os.makedirs(path, exist_ok=True)
        successful_clean.set()


class FuseTimeoutException(DataPrepException):
    def __init__(self, message, error_code=None, compliant_message='', error_data=None):
        super().__init__(message=message, error_code=error_code, compliant_message=compliant_message,
                         error_data=error_data)


class FuseProcessTerminatedException(DataPrepException):
    def __init__(self, exit_code, message='', error_code=None, compliant_message='', error_data=None):
        super().__init__(message=message, error_code=error_code, compliant_message=compliant_message,
                         error_data=error_data)
        self.exit_code = exit_code
