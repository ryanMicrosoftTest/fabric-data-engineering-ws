from abc import ABC, abstractmethod
from datetime import datetime


class BaseInitEventLogger(ABC):
    @abstractmethod
    def log(self, message):
        raise NotImplemented()

    @abstractmethod
    def end(self):
        raise NotImplemented()


class NoopInitEventLogger(BaseInitEventLogger):
    def log(self, message):
        pass

    def end(self):
        pass


class FileInitEventLogger(BaseInitEventLogger):
    def __init__(self, file_path):
        self._file = open(file_path, mode='w')

    def log(self, message):
        self._file.write('[{}] {}\n'.format(datetime.utcnow().isoformat(), message))
        self._file.flush()

    def end(self):
        if not self._file.closed:
            self._file.close()
