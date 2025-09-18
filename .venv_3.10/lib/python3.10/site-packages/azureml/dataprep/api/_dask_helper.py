class DaskImportError(Exception):
    """
    Exception raised when dask was not able to be imported.
    """
    _message = \
        'Could not import dask. Ensure a compatible version is installed by running: pip install azureml-dataprep[dask]'

    def __init__(self):
        print('DaskImportError: ' + self._message)
        super().__init__(self._message)


_have_dask = True
# noinspection PyBroadException
try:
    import dask
except:
    _have_dask = False


def have_dask() -> bool:
    global _have_dask
    return _have_dask


def _ensure_dask():
    if not have_dask():
        raise DaskImportError()
