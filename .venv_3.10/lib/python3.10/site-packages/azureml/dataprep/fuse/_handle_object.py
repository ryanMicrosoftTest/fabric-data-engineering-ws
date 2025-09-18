class HandleObject:
    _handle = 0

    def __init__(self, path: str, handle: int):
        self.path = path
        self.handle = handle

    @staticmethod
    def new_handle(fh=None):
        if fh is not None:
            return fh
        HandleObject._handle += 1
        return HandleObject._handle
