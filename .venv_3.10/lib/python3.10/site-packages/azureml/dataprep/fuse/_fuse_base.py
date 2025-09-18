from azureml.dataprep.api.tracing._context import Context

from .vendor.fuse import Operations
from azureml.dataprep.api.engineapi.api import get_engine_api
from ._logger_helper import get_trace_with_invocation_id
from stat import S_IFREG
import sys
import tempfile
from time import time


class FuseBase(Operations):
    def __init__(self,
                 log,
                 invocation_id: str,
                 mount_options: 'MountOptions' = None,
                 span_context: Context = None):
        from .dprepfuse import MountOptions

        if mount_options is None:
            mount_options = MountOptions()
        self._trace = get_trace_with_invocation_id(log, invocation_id)
        self._trace('Initializing mount. max_size={}, free_space_required={}'.format(
            mount_options.max_size,
            mount_options.free_space_required
        ))

        mount_options.data_dir = mount_options.data_dir or tempfile.mkdtemp()
        mount_options.max_size = mount_options.max_size or sys.maxsize
        mount_options.free_space_required = mount_options.free_space_required or 100 * 1024 * 1024
        self._mount_options = mount_options
        self._engine_api = get_engine_api()
        self._invocation_id = invocation_id
        self._mount_timestamp = int(time())
        self._span_context = span_context

    @property
    def _sentinel_attr(self):
        return {
            'st_mode': S_IFREG,
            'st_size': len(self._sentinel_contents),
            'st_atime': self._mount_timestamp,
            'st_mtime': self._mount_timestamp,
            'st_ctime': self._mount_timestamp
        }
    @property
    def _sentinel_contents(self):
        from azureml.dataprep.api._loggerfactory import session_id
        return "{}\n".format(session_id).encode('utf-8')
