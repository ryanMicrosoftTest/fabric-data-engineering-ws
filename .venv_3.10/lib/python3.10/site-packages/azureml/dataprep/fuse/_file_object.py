from typing import Optional

from azureml.dataprep.api._loggerfactory import _LoggerFactory

from ._handle_object import HandleObject
from ._local_driver import LocalDriver


log = _LoggerFactory.get_logger('dprep.writable_fuse')


class FileObject(HandleObject):
    def __init__(self, handle: int, path: str, flags: int, driver: LocalDriver):
        super().__init__(path, handle)
        self.flags = flags
        self._driver = driver
        # only set to true when creating new file
        self.is_dirty = not self._driver.exists(path)

    def read(self, size, offset, buffer):
        return self._driver.read(self.path, size, offset, self.handle, buffer)

    def write(self, size, offset, buffer):
        result = self._driver.write(self.path, size, offset, self.handle, buffer)
        self.is_dirty = True
        return result

    def release(self):
        pass

