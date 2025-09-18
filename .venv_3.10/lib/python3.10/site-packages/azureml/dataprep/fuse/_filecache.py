from ._stat import update_stat
from ._streaminfo import StreamDetails
from azureml.dataprep import ExecutionError
from azureml.dataprep.api._loggerfactory import _LoggerFactory, trace
from azureml.dataprep.fuse._logger_helper import get_trace_with_invocation_id
from azureml.dataprep.native import read_into_buffer
from collections import OrderedDict
from errno import EFBIG, ENOENT
from .vendor.fuse import FuseOSError
import ctypes
import os
import shutil
import threading
from typing import Callable, Union, Optional

log = _LoggerFactory.get_logger('dprep.fuse.filecache')
tracer = trace.get_tracer(__name__)


class _FileOps:
    def __init__(self,
                 makedirs: Callable[[str, Optional[bool]], None],
                 rmtree: Callable[[str], None],
                 rm: Callable[[str], None],
                 stat: Callable[[str], os.stat_result],
                 get_free_space: Callable[[str], int]):
        self.makedirs = makedirs
        self.rmtree = rmtree
        self.rm = rm
        self.stat = stat
        self.get_free_space = get_free_space


def _get_free_space(path: str) -> int:
    statvfs = os.statvfs(path)
    return statvfs.f_frsize * statvfs.f_bavail


_standard_file_ops = _FileOps(lambda path, exist_ok: os.makedirs(path, exist_ok=exist_ok),
                              lambda p: shutil.rmtree(p, ignore_errors=True),
                              os.remove,
                              os.stat,
                              _get_free_space)


class _CacheEntry:
    def __init__(self,
                 path: str,
                 download_path: str,
                 attributes: os.stat_result):
        self.path = path
        self.download_path = download_path
        self.attributes = attributes


class FileCache:
    def __init__(self,
                 data_dir: str,
                 allowed_size: int,
                 required_free_space: int,
                 download_path: Callable[[StreamDetails, str], str],
                 get_handle: Callable[[], int],
                 file_ops: _FileOps = _standard_file_ops,
                 invocation_id: str = None):
        self._trace = get_trace_with_invocation_id(log, invocation_id)

        self._next_handle = 0
        self._entries_lock = threading.Lock()
        self._entries = OrderedDict()
        self._open_paths = {}
        self._streams = {}
        self._downloads_in_progress = {}
        self._total_size = 0

        self.data_dir = data_dir

        self._allowed_size = allowed_size
        self._required_free_space = required_free_space
        self._download_path = download_path
        self._get_handle = get_handle

        self._file_ops = file_ops
        self._file_ops.makedirs(self.data_dir, True)

    def clear(self):
        self._file_ops.rmtree(self.data_dir)

    def get_attributes(self, path: str) -> os.stat_result:
        entry = self._entries[path]
        log.debug('Returning attributes from cache: %s', entry.attributes)
        return entry.attributes

    def _remove_entry(self, entry: _CacheEntry):
        log.debug('Removing entry from cache: %s', entry.path)
        self._file_ops.rm(entry.download_path)
        self._total_size -= entry.attributes.st_size
        self._entries.pop(entry.path)

    def _ensure_enough_space(self, size: int, free_space: int) -> int:
        log.debug('Ensuring file fits in cache.')
        if size > self._allowed_size:
            log.info('Attempting to cache file larger than max allowed size.')
            raise FuseOSError(EFBIG)

        if free_space + self._total_size - size < self._required_free_space:
            msg = 'Attempting to cache file that does not fit in the specified volume.'
            log.debug(msg)
            self._trace(msg)
            raise FuseOSError(EFBIG)

        while free_space - size < self._required_free_space or size + self._total_size > self._allowed_size:
            if len(self._entries) == 0:
                msg = 'Unable to clear sufficient space from the cache to fit file.'
                log.debug(msg)
                self._trace(msg)
                raise FuseOSError(EFBIG)

            entry_path, entry_to_remove = \
                next((item for item in self._entries.items() if item[1].path not in self._open_paths), (None, None))
            if entry_to_remove is None:
                msg = 'Unable to clear sufficient space from the cache to fit file.'
                log.debug(msg)
                self._trace(msg)
                raise FuseOSError(EFBIG)

            self._remove_entry(entry_to_remove)
            free_space += entry_to_remove.attributes.st_size

        log.debug('Sufficient space to cache file.')
        return free_space

    def push(self, path: str, stream_details: StreamDetails, attributes: os.stat_result):
        stream_info = stream_details.stream_info
        def get_or_add_lock():
            lock = self._downloads_in_progress.get(path)
            if lock is None:
                lock = threading.Lock()
                self._downloads_in_progress[path] = lock
            else:
                log.debug('Stream is already being downloaded: %s', stream_info)
            return lock

        log.debug('Request to add stream to cache: %s. Stat: %s', stream_info, attributes)
        with tracer.start_as_current_span('FileCache.push', trace.get_current_span()) as span:
            size = attributes.st_size
            with tracer.start_as_current_span('FileCache.get_free_space', span):
                free_space = self._file_ops.get_free_space(self.data_dir)
                if size is not None:
                    free_space = self._ensure_enough_space(size, free_space)

            download_lock = get_or_add_lock()
            download_lock.acquire()
            if path in self:
                download_lock.release()
                log.debug('Stream already in cache: %s', stream_info)
                return

            log.debug('Adding stream to cache: %s', stream_info)
            try:
                target_relative_path = path[1:] if path[0] == '/' else path
                target_path = os.path.join(self.data_dir, target_relative_path)
                log.info('Downloading file into cache.')
                target_path = self._download_path(stream_details, target_path)
                log.info('Downloaded file into cache.')
                size = self._file_ops.stat(target_path).st_size
                log.debug('File downloaded. Size: %s', size)
                try:
                    self._ensure_enough_space(size, free_space)
                except FuseOSError:
                    log.info('File does not fit in cache. Deleting downloaded data.')
                    self._file_ops.rm(target_path)
                    raise

                self._total_size += size
                actual_attributes = update_stat(attributes, new_size=size)
                cache_entry = _CacheEntry(path, target_path, actual_attributes)
                self._entries[path] = cache_entry
            except ExecutionError as e:
                if e.error_code == 'NotEnoughSpace':
                    raise FuseOSError(EFBIG)
                else:
                    log.error('Execution error while downloading stream.')
                    raise FuseOSError(ENOENT)
            finally:
                download_lock.release()
                self._downloads_in_progress.pop(path)

    def open(self, path: str) -> int:
        with self._entries_lock:
            # self._entries is a LRU cache backed by OrderedDict and the cache is evicted by iterating the
            # OrderedDict where the iteration order is based on insertion order, the item that is inserted last will
            # be visited last.
            cache_entry = self._entries.pop(path)
            self._entries[path] = cache_entry
        handle = self._get_handle()
        self._streams[handle] = (cache_entry.download_path, path)
        self._open_paths[path] = self._open_paths.get(path, 0) + 1
        return handle

    def read(self, handle: int, size: int, offset: int, buffer: ctypes.POINTER(ctypes.c_byte)):
        path, _ = self._streams[handle]
        return read_into_buffer(path, size, offset, ctypes.addressof(buffer.contents))

    def release(self, handle: int):
        _, path = self._streams.pop(handle)
        open_count = self._open_paths[path] - 1
        if open_count == 0:
            self._open_paths.pop(path)
        else:
            self._open_paths[path] = open_count

    def remove_from_cache(self, path):
        if path in self._open_paths:
            raise ValueError('Path currently opened and cannot be removed from cache.')

        self._remove_entry(self._entries[path])

    def get_open_handle_count(self, path):
        return self._open_paths.get(path) or 0

    def __contains__(self, item: Union[str, int]):
        return item in self._entries
