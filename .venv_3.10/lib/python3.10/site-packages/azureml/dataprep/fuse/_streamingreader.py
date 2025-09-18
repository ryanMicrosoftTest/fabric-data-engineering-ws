from ._cached_dataflow import CachedDataflow
from ._dataflowconstants import *
from ._stat import create_stat
from ._streaminfo import get_stream_info_value, StreamDetails
from azureml.dataprep import col, ExecutionError
from azureml.dataprep.api._loggerfactory import _LoggerFactory, trace
from azureml.dataprep.api.engineapi.api import EngineAPI
from azureml.dataprep.api.engineapi.typedefinitions import ReadStreamInfoMessageArguments
from errno import ENOENT
import ctypes
import json
import os
from stat import S_IFDIR, S_IFREG
from typing import Callable
from uuid import uuid4
from threading import Lock

try:
    from .vendor.fuse import FuseOSError
except Exception:
    class FuseOSError(Exception):
        pass


log = _LoggerFactory.get_logger('dprep.fuse.streamingreader')
tracer = trace.get_tracer(__name__)
_fuse_reads = {}
_fuse_reads_lock = Lock()


def _handle_fuse_read(request, writer, socket):
    global _fuse_reads
    read_id = request.get('read_id')
    writer.write(json.dumps({'result': 'success'}) + '\n')
    writer.flush()

    byte_count = int.from_bytes(socket.recv(8), 'little')
    with socket.makefile('rb') as data_reader:
        data_read = data_reader.read(byte_count)
        _fuse_reads[read_id] = data_read

    writer.write(json.dumps({'result': 'success'}) + '\n')
    writer.flush()


class UnknownStreamSizeError(Exception):
    pass


class StreamingReader:
    def __init__(self,
                 cached_dataflow: CachedDataflow,
                 files_column: str,
                 mount_timestamp: int,
                 engine_api: EngineAPI,
                 get_handle: Callable[[], int],
                 default_permission: int):
        self._cached_dataflow = cached_dataflow
        self._files_column = files_column
        self._mount_timestamp = mount_timestamp
        self._engine_api = engine_api
        self._get_handle = get_handle
        self._open_streams = {}
        self._known_attributes = {
            '/': create_stat(S_IFDIR, 0, mount_timestamp, mount_timestamp, mount_timestamp)
        }
        self._default_permission = default_permission

        if not engine_api.requests_channel.has_handler(FUSE_READS_HANDLER):
            engine_api.requests_channel.register_handler(FUSE_READS_HANDLER, _handle_fuse_read)

    def get_attributes_and_stream_details(self, path) -> os.stat:
        attributes = self._known_attributes.get(path, None)
        if attributes is not None:
            return attributes, None

        # We'll grab a single stream under the path specified. If this path is a file, this stream should match
        # the path and we can get its stream properties. If this path is a directory, then the stream we get
        # back should be under it.
        try:
            log.debug('Retrieving attributes from StreamInfos for path %s.', path, extra=dict(path=path))
            matching_streams = self._cached_dataflow.dataflow(wait_for_cache=False).filter(col(PORTABLE_PATH).starts_with(path)) \
                .take(1) \
                ._to_pyrecords()
            if len(matching_streams) == 0:
                log.debug('Path does not exist.', extra=dict(path=path))
                raise FuseOSError(ENOENT)

            matching_stream = matching_streams[0]
            if len(matching_stream[PORTABLE_PATH]) > len(path):
                # Directory
                if matching_stream[PORTABLE_PATH][len(path)] == '/':
                    stat = create_stat(S_IFDIR, 0, self._mount_timestamp, self._mount_timestamp, self._mount_timestamp)
                    log.debug('Path is a directory. Returning attributes.', extra=dict(path=path, stat=stat))
                    return stat, None

                if path.endswith("/"):
                    log.debug('Path does not exist.', extra=dict(path=path))
                    raise FuseOSError(ENOENT)

                matching_streams = self._cached_dataflow.dataflow(wait_for_cache=False).filter(col(PORTABLE_PATH).starts_with(path+"/")) \
                    .take(1) \
                    ._to_pyrecords()

                if len(matching_streams) == 0:
                    log.debug('Path does not exist.', extra=dict(path=path))
                    raise FuseOSError(ENOENT)

                stat = create_stat(S_IFDIR, 0, self._mount_timestamp, self._mount_timestamp, self._mount_timestamp)
                log.debug('Path is a directory. Returning attributes.', extra=dict(path=path, stat=stat))
                return stat, None
            else:
                # File
                stream_properties = matching_stream[STREAM_PROPERTIES]
                if stream_properties.get(STREAM_SIZE) is None:
                    raise UnknownStreamSizeError()

                stream_details = StreamDetails(matching_stream[self._files_column],
                                               matching_stream[PORTABLE_PATH],
                                               stream_properties[STREAM_SIZE],
                                               stream_properties[LAST_MODIFIED],
                                               stream_properties[CAN_SEEK])
                stream_last_modified = int(stream_details.last_modified.timestamp()) \
                    if stream_details.last_modified is not None else self._mount_timestamp
                stat = create_stat(self._default_permission | S_IFREG,
                                   stream_details.size,
                                   stream_last_modified,
                                   stream_last_modified,
                                   stream_last_modified)

                log.debug('Path is a file. Returning attributes and stream details.', extra=dict(path=path, stat=stat))
                return stat, stream_details
        except UnknownStreamSizeError:
            raise
        except FuseOSError:
            raise
        except ExecutionError:
            log.error('Dataflow execution error during getattr.')
            raise FuseOSError(ENOENT)
        except Exception:
            log.error('Unexpected error during getattr.')
            raise FuseOSError(ENOENT)

    def open(self, path: str) -> int:
        with tracer.start_as_current_span('StreamingReader.open', trace.get_current_span()):
            handle = self._get_handle()
            try:
                matching_rows = self._cached_dataflow.dataflow(wait_for_cache=False).filter(col(PORTABLE_PATH) == path).take(1)._to_pyrecords()
                if len(matching_rows) == 0:
                    log.debug('File not found while opening for streaming.', extra=dict(path=path))
                    raise FuseOSError(ENOENT)

                matching_stream = matching_rows[0][self._files_column]
                # noinspection PyTypeChecker
                stream_id = self._engine_api.open_stream_info(get_stream_info_value(matching_stream))

                self._open_streams[handle] = stream_id
                log.debug('Stream to file opened.', extra=dict(path=path, handle=handle, stream_id=stream_id))
                return handle

            except FuseOSError:
                raise
            except Exception:
                log.error('Unexpected error during open.')
                raise FuseOSError(ENOENT)

    def read(self, handle: int, size: int, offset: int, buffer: ctypes.POINTER(ctypes.c_byte)):
        try:
            global _fuse_reads
            log.debug('read(handle=%s, size=%s, offset=%s)', handle, size, offset, extra=dict(handle=handle,
                                                                                              offset=offset,
                                                                                              size=size))
            stream_id = self._open_streams[handle]
            read_id = str(uuid4())
            message_args = ReadStreamInfoMessageArguments(stream_info_id=stream_id,
                                                          read_id=read_id,
                                                          offset=offset,
                                                          size=size)
            self._engine_api.read_stream_info(message_args)
            data = _fuse_reads[read_id]
            bytes_read = len(data)
            ctypes.memmove(buffer, data, bytes_read)
            with _fuse_reads_lock:
                if read_id in _fuse_reads:
                    del _fuse_reads[read_id]
            return bytes_read
        except Exception:
            log.error('Unexpected error while reading stream for handle %s.',
                      handle,
                      extra=dict(handle=handle))
            raise FuseOSError(ENOENT)

    def release(self, handle: int):
        # noinspection PyBroadException
        try:
            log.info('release(handle=%s)', handle, extra=dict(handle=handle))
            stream_id = self._open_streams[handle]
            self._engine_api.close_stream_info(stream_id)
        except Exception:
            log.warning('Unexpected exception while releasing stream.',
                        extra=dict(handle=handle))
