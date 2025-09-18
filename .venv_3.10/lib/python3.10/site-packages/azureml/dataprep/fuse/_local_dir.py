import os


class LocalDir:
    def __init__(self, data_dir: str):
        self._data_dir = data_dir
        os.makedirs(self._data_dir, exist_ok=True)

    def get_local_root(self):
        return self._data_dir

    def get_target_path(self, path):
        target_relative_path = path.lstrip('/')
        return os.path.join(self._data_dir, target_relative_path)