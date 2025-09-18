from ._local_dir import LocalDir
from azureml.dataprep.native import read_into_buffer, write_into_file
from azureml.dataprep.api._loggerfactory import _LoggerFactory
import os
import ctypes
from platform import system
from errno import EIO

log = _LoggerFactory.get_logger('dprep.local_driver')


class LocalDriver:
    def __init__(self, local_dir: LocalDir):
        self._dir = local_dir

    def get_attributes(self, path):
        target_path = self._dir.get_target_path(path)
        return os.lstat(target_path)

    def access(self, path, mode):
        target_path = self._dir.get_target_path(path)
        return os.access(target_path, mode)

    def readdir(self, path, fh):
        target_path = self._dir.get_target_path(path)
        return os.listdir(target_path)

    def read(self, path, size, offset, fh, buffer):
        log.debug('Reading file from cache: %s (handle=%s)',
                  path, fh, extra=dict(path=path, handle=fh))
        target_path = self._dir.get_target_path(path)
        log.debug('Target path is {}'.format(target_path))
        count = read_into_buffer(target_path, size, offset, ctypes.addressof(buffer.contents))
        return count

    def mkdir(self, path, mode):
        target_path = self._dir.get_target_path(path)
        os.mkdir(target_path, mode)
        return 0

    def open(self, path, flags):
        target_path = self._dir.get_target_path(path)
        return os.open(target_path, flags)

    def rmdir(self, path):
        target_path = self._dir.get_target_path(path)
        return os.rmdir(target_path)

    def mknod(self, path, mode, dev):
        target_path = self._dir.get_target_path(path)
        # mknod on macos requires root access.
        # Trying to create file in another way as this is the only usage for mknod for now
        if system() == 'Darwin':
            try:
                open(target_path, 'w').close()
                return 0
            except Exception as e:
                log.error('Failed to created a new file. Error {}'.format(e))
                return -EIO
        else:
            return os.mknod(target_path, mode, dev)

    def write(self, path, size, offset, fh, buffer):
        target_path = self._dir.get_target_path(path)
        count = write_into_file(target_path, size, offset, ctypes.addressof(buffer.contents))
        return count

    def truncate(self, path, length):
        target_path = self._dir.get_target_path(path)
        return os.truncate(target_path, length)

    def unlink(self, path):
        target_path = self._dir.get_target_path(path)
        return os.unlink(target_path)

    def rename(self, old, new):
        old_target_path = self._dir.get_target_path(old)
        new_target_path = self._dir.get_target_path(new)
        return os.rename(old_target_path, new_target_path)

    def chmod(self, path, mode):
        target_path = self._dir.get_target_path(path)
        os.chmod(target_path, mode)

    def chown(self, path, uid, gid):
        target_path = self._dir.get_target_path(path)
        os.chown(target_path, uid, gid)

    def exists(self, path):
        target_path = self._dir.get_target_path(path)
        return os.path.exists(target_path)
    
    def _makedirs(self, path, mode):
        target_path = self._dir.get_target_path(path)
        return os.makedirs(target_path, exist_ok=True) 
