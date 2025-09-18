from ._handle_object import HandleObject
from ._local_driver import LocalDriver


class DirObject(HandleObject):
    def __init__(self, handle: int, path: str, driver: LocalDriver):
        super().__init__(path, handle)
        self._driver = driver

    def readdir(self):
        return self._driver.readdir(self.path, self.handle)

    def release(self):
        pass
