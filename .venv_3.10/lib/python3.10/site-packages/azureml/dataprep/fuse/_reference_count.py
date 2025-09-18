import threading


class ReferenceCount:
    def __init__(self):
        self._count = 1
        self._lock = threading.Lock()

    def add_reference(self):
        with self._lock:
            self._count += 1
            return self._count

    def release(self):
        with self._lock:
            if self._count != 0:
                self._count -= 1
            return self._count

    def has_reference(self):
        return self._count > 0
