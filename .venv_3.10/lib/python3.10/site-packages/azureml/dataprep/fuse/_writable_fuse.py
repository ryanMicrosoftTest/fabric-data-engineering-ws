import errno
import os
import tempfile
import threading
from typing import Union, Optional
from stat import S_IFDIR, S_IFREG

from azureml.dataprep import Dataflow, ExecutionError
from azureml.dataprep.api._datastore_helper import _to_stream_info_value
from azureml.dataprep.api._loggerfactory import _LoggerFactory, trace
from azureml.dataprep.api.engineapi.typedefinitions import UploadFileMessageArguments, DeleteMessageArguments, \
    MoveFileMessageArguments, CreateFolderMessageArguments
from azureml.dataprep.api.tracing._context import Context
from azureml.dataprep.api.tracing._open_telemetry_adapter import to_dprep_span_context

from ._dir_object import DirObject
from ._file_object import FileObject
from ._fuse_base import FuseBase
from ._handle_object import HandleObject
from ._local_dir import LocalDir
from ._local_driver import LocalDriver
from .dprepfuse import MountOptions, _SENTINEL_PATH
from .vendor.fuse import FuseOSError
from errno import ENOENT

log = _LoggerFactory.get_logger('dprep.writable_fuse')
tracer = trace.get_tracer(__name__)

DATASET_DISABLE_WRITABLE_MOUNT_METADATA_CACHE_ENV = "DATASET_DISABLE_WRITABLE_MOUNT_METADATA_CACHE"

class WritableFuse(FuseBase):
    def __init__(self,
                 destination: 'Value' = None,
                 mount_options: MountOptions = None,
                 invocation_id: str = None,
                 span_context: Optional[Context] = None,
                 caller_session_id=None):
        super().__init__(log, invocation_id, mount_options, span_context or tracer.start_span(self.__class__.__name__))
        self._datastore = None
        self._remote_base = None
        if destination is not None:
            self._datastore = destination[0]
            self._remote_base = destination[1]

            try:
                from azureml.data.datapath import DataPath
                from .dprepfuse import _DPrepFuse

                base = '{}/{}'.format(self._datastore.name, self._remote_base.lstrip('/\\'))
                files_column = 'Path'
                dataflow = Dataflow.get_files(DataPath(self._datastore, self._remote_base))
                self._mount_options._data_dir_suffix = None
                self._mount_options._cached_dataflow_override = \
                    self._mount_options._cached_dataflow_override or tempfile.mkdtemp()
                if (DATASET_DISABLE_WRITABLE_MOUNT_METADATA_CACHE_ENV in os.environ):
                    self._mount_options._disable_dataflow_cache = os.environ.get(DATASET_DISABLE_WRITABLE_MOUNT_METADATA_CACHE_ENV, "").lower() == 'true'
                self._read_fuse = _DPrepFuse(
                    dataflow, files_column, base, mount_options=self._mount_options, invocation_id=invocation_id,
                    caller_session_id=caller_session_id, span_context=span_context, show_not_found_error=False
                )
            except ImportError:
                raise ImportError('Unable to import azureml.core. Please make sure you have azureml-core installed in '
                                  'order to use Azure Machine Learning Dataset\'s mount capability.')

        self._data_dir = LocalDir(self._mount_options.final_data_dir)
        self._handle_table = {}
        self._path_to_handle_table = {}
        self._local_driver = LocalDriver(self._data_dir)
        self._file_locks = {}
        self._deleted = set()

    def _get_file_obj(self, handle) -> Union[FileObject, DirObject]:
        return self._handle_table.get(handle)

    def _remove_file_obj(self, handle):
        self._handle_table.pop(handle)

    def _get_remote_stream_info(self, path=None):
        if path is None:
            remote_path = self._remote_base
        else:
            relative_path = path.lstrip('/')
            remote_path = os.path.join(self._remote_base, relative_path)
        return _to_stream_info_value(self._datastore, remote_path)

    def access(self, path, mode):
        if path == _SENTINEL_PATH:
            return 0

        return 0 if not self._local_driver.exists(path) or self._local_driver.access(path, mode) else -1
        
    def create(self, path, mode, flags, fi=None):
        '''
        When fi is None and create should return a numerical file handle.

        When fi is not None the file handle should be set directly by create
        and return 0.
        '''
        log.debug('Creating {} with mode {} and flags {}'.format(path, mode, flags))
        if path in self._deleted:
            self._deleted.remove(path)
        # No matter what permissions were requested current user should have permission to write to the file.
        # cp command on linux copies permissions and attempt to copy the file with read-only access outside of the mount results in an error
        # because mirror the file permissions of the original file (read-only) and try to write to it after that
        self.mknod(path, mode | 0o700, 0)
        handle = self.open(path, flags, fi)
        handle_record = self._handle_table.get(handle, None)
        if handle_record is not None:
            handle_record.is_dirty = True
        return handle

    def flush(self, path, fh):
        from .dprepfuse import _SENTINEL_PATH

        with tracer.start_as_current_span('WritableFuse.flush', self._span_context) as span:
            log.debug('Flushing file {}.'.format(path))

            if path == _SENTINEL_PATH:
                log.debug('Skipping flush for sentinel file')
                return 0

            file = self._get_file_obj(fh)
            if file is None or not file.is_dirty or not self._datastore:
                log.debug('Skipping flush because. file: {}, is_dirty: {}, datastore: {}'.format(
                    path, file and file.is_dirty, self._datastore and self._datastore.name
                ))
                return 0
            file_lock = self._file_locks[path]
            with file_lock:
                try:
                    self._engine_api.upload_file(UploadFileMessageArguments(
                        base_path=self._data_dir.get_local_root(),
                        destination=self._get_remote_stream_info(),
                        local_path=self._data_dir.get_target_path(path),
                        overwrite=True,
                        span_context=to_dprep_span_context(span.get_context())
                    ))
                    log.debug('Uploaded {}.'.format(path))
                except Exception as e:
                    log.warning(e)
                    raise FuseOSError(errno.EIO)
                file.is_dirty = False
            return 0

    def getattr(self, path, fh=None):
        log.debug('Getattr {}'.format(path))
        from .dprepfuse import _SENTINEL_PATH
        if path == _SENTINEL_PATH:
            return self._sentinel_attr

        try:
            stat = self._local_driver.get_attributes(path)
            return {
                'st_mode': stat.st_mode,
                'st_size': stat.st_size,
                'st_atime': stat.st_atime,
                'st_mtime': stat.st_mtime,
                'st_ctime': stat.st_ctime,
                'st_uid': stat.st_uid,
                'st_gid': stat.st_gid
            }
        except FileNotFoundError:
            if path in self._deleted:
                raise
            try:
                attrs = self._read_fuse.getattr(path, fh)
                return attrs
            except ExecutionError as e:
                if e.error_code == 'ScriptExecution.StreamAccess.NotFound':
                    raise FuseOSError(ENOENT)
                else:
                    raise e

    def mkdir(self, path, mode):
        with tracer.start_as_current_span('WritableFuse.mkdir', self._span_context) as span:
            try:
                self._makesure_parent_exists(path, mode)
                self._local_driver.mkdir(path, mode)
                if self._datastore is not None:
                    self._engine_api.create_folder(CreateFolderMessageArguments(
                        remote_folder_path=self._get_remote_stream_info(path),
                        span_context=to_dprep_span_context(span.get_context())
                    ))
            except FileExistsError:
                raise FuseOSError(errno.EEXIST)
            except Exception as e:
                log.warning(e)
                raise FuseOSError(errno.EIO)
            return 0

    def mknod(self, path, mode, dev):
        # No matter what permissions were requested current user should have permission to write to the file.
        # cp command on linux copies permissions and attempt to copy the file with read-only access outside of the mount results in an error
        # because mirror the file permissions of the original file (read-only) and try to write to it after that
        self._makesure_parent_exists(path, mode | 0o700)
        return self._local_driver.mknod(path, mode | 0o700, dev)

    def open(self, path, flags, fh=None):
        log.debug('Opening {} with flags {}'.format(path, flags))

        if path == _SENTINEL_PATH:
            log.debug('_SENTINEL_PATH opened: %s (handle=%s)', path, 0, extra=dict(path=path, handle=0))
            return 0

        if path not in self._file_locks:
            self._file_locks[path] = threading.Lock()
        exists = self._local_driver.exists(path)
        if not exists and path not in self._deleted:
            log.debug('Opening {} from remote.'.format(path, flags))
            self._read_fuse.open(path, flags, fh)
        handle = HandleObject.new_handle(fh)
        self._handle_table[handle] = FileObject(handle, path, flags, self._local_driver)
        self._add_path_to_handle(path, handle)
        return handle

    def opendir(self, path):
        log.debug('Opening dir {}'.format(path))
        handle = HandleObject.new_handle()
        self._handle_table[handle] = DirObject(handle, path, self._local_driver)
        self._add_path_to_handle(path, handle)
        return handle

    def read(self, path, size, offset, fh, buffer):
        if path == _SENTINEL_PATH and fh == 0:
            log.debug('Reading _SENTINEL_PATH: %s (handle=%s)', path, fh, extra=dict(path=path, handle=fh))
            contents = self._sentinel_contents
            contents_len = len(contents)
            for i, c in enumerate(contents):
                buffer[offset+i] = contents[i]
            return contents_len

        file = self._get_file_obj(fh)
        if file is not None:
            return file.read(size, offset, buffer)
        raise FuseOSError(errno.EBADF)

    def readdir(self, path, fh):
        def merge(left, right):
            left_entries = set(left)
            for item in right:
                if item not in left_entries:
                    left.append(item)
            return list(filter(lambda entry: os.path.join(path, entry) not in self._deleted, left))

        log.debug('Read dir {}!'.format(path))
        directory = self._get_file_obj(fh)
        if directory is not None:
            local_entries = []
            try:
                local_entries = ['.', '..'] + directory.readdir()
            except OSError:
                log.debug('Directory being read doesn\'t exist locally.')
                pass
            
            try:
                remote_entries = self._read_fuse._list_entries(path)
            except ExecutionError as e:
                if 'NotFound' not in e.error_code:
                    raise
                remote_entries = []

            return merge(local_entries, remote_entries)
        raise FuseOSError(errno.EBADF)

    def release(self, path, fh):
        log.debug('Release file {}.'.format(path))

        if path == _SENTINEL_PATH and fh == 0:
            log.debug('Releasing _SENTINEL_PATH: %s (handle=%s)', path, fh, extra=dict(path=path, handle=fh))
            return 0

        self._remove_file_obj(fh)
        handles = self._path_to_handle_table.get(path)
        if handles and fh in handles:
            handles.remove(fh)
        return 0

    def releasedir(self, path, fh):
        log.debug('Release dir {}'.format(path))
        
        handles = self._path_to_handle_table.get(path)
        if handles and fh in handles:
            handles.remove(fh)

        return 0

    def rename(self, old, new):
        with tracer.start_as_current_span('WritableFuse.rename', self._span_context) as span:
            # can only change file name if there is no reference
            log.debug('Renaming {} to {}'.format(old, new))

            exists_locally = self._local_driver.exists(old)
            if not exists_locally and not self._exists_remote(old):
                raise FuseOSError(errno.ENOENT)

            if not exists_locally:
                # force download the file since the read fuse's cache never expires so if the file doesn't exist
                # locally we won't be able to retrieve it again
                self._read_fuse.open(old, os.O_RDONLY)

            if self._datastore is not None:
                try:
                    self._engine_api.move_file(MoveFileMessageArguments(
                        desitnation_base_path=self._get_remote_stream_info(), new_relative_path=new,
                        old_relative_path=old, overwrite=True,
                        span_context=to_dprep_span_context(span.get_context())
                    ))
                except Exception as e:
                    log.warning(e)
                    raise FuseOSError(errno.EIO)
            if not self._mount_options._disable_dataflow_cache:
                self._deleted.add(old)
                if new in self._deleted:
                    self._deleted.remove(new)
                return self._local_driver.rename(old, new)

    def rmdir(self, path):
        with tracer.start_as_current_span('WritableFuse.rmdir', self._span_context) as span:
            log.debug('Removing dir {}.'.format(path))
            exists_locally = self._local_driver.exists(path)
            if not exists_locally and not self._exists_remote(path):
                raise FuseOSError(errno.ENOENT)
            if self._datastore is not None:
                try:
                    self._engine_api.delete(DeleteMessageArguments(
                        destination_path=self._get_remote_stream_info(path),
                        span_context=to_dprep_span_context(span.get_context())
                    ))
                except Exception as e:
                    log.warning(e)
                    raise FuseOSError(errno.EIO)
            if exists_locally:
                log.debug('Removing local directory {}.'.format(path))
                return self._local_driver.rmdir(path)
            if not self._mount_options._disable_dataflow_cache:
                self._deleted.add(path)

    def truncate(self, path, length, fh=None):
        log.debug('Truncating {} to {}.'.format(path, length))
        handles = self._path_to_handle_table.get(path)
        for handle in handles:
            file = self._handle_table.get(handle)
            if file:
                file.is_dirty = True
        return self._local_driver.truncate(path, length)

    def write(self, path, size, offset, fh, buffer):
        file = self._get_file_obj(fh)
        if file is not None:
            return file.write(size, offset, buffer)
        return errno.EBADF

    def unlink(self, path):
        with tracer.start_as_current_span('WritableFuse.unlink', self._span_context) as span:
            log.debug('Removing path {}.'.format(path))
            exists_locally = self._local_driver.exists(path)
            if not exists_locally and not self._exists_remote(path):
                raise FuseOSError(errno.ENOENT)
            if self._datastore is not None:
                try:
                    self._engine_api.delete(DeleteMessageArguments(
                        destination_path=self._get_remote_stream_info(path),
                        span_context=to_dprep_span_context(span.get_context())
                    ))
                except Exception as e:
                    log.warning(e)
                    raise FuseOSError(errno.EIO)
            if exists_locally:
                self._local_driver.unlink(path)
            if not self._mount_options._disable_dataflow_cache:
                self._deleted.add(path)

    def chmod(self, path, mode):
        if path == _SENTINEL_PATH:
            return 0

        self._local_driver.chmod(path, mode)

    def chown(self, path, uid, gid):
        if path == _SENTINEL_PATH:
            return 0
            
        self._local_driver.chown(path, uid, gid)

    def _makesure_parent_exists(self, path, mode):
        try:
            parent = os.path.dirname(path.rstrip(os.path.sep))
            if (parent != os.path.sep and self.getattr(parent)['st_mode'] & S_IFDIR != 0):
                self._local_driver._makedirs(parent, mode)
        except FileNotFoundError:
            raise FuseOSError(errno.ENOENT)
        
    def _exists_remote(self, path):
        try:
            self._read_fuse.getattr(path)
            return True
        except FileNotFoundError:
            return False

    def _add_path_to_handle(self, path:str, handle: int):
        handles = self._path_to_handle_table.get(path, set())
        handles.add(handle)
        self._path_to_handle_table[path] = handles
