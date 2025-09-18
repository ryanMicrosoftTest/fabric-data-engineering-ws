from ._pandas_helper import have_numpy, have_pandas, have_pyarrow, ensure_df_native_compat, PandasImportError, NumpyImportError
from .engineapi.api import get_engine_api
from .engineapi.engine import CancellationToken
from .engineapi.typedefinitions import ExecuteAnonymousActivityMessageArguments, AnonymousActivityData
from .errorhandlers import OperationCanceled
from .step import steps_to_block_datas
import io
import json
import math
from threading import Event, Thread
from typing import Union
from uuid import uuid4
from ._loggerfactory import _LoggerFactory

logger = _LoggerFactory.get_logger("_partitionsreader")


# 20,000 rows gives a good balance between memory requirement and throughput by requiring that only
# (20000 * CPU_CORES) rows are materialized at once while giving each core a sufficient amount of
# work.
PARTITION_SIZE = 20000


class _InconsistentSchemaError(Exception):
    def __init__(self):
        super().__init__('Inconsistent schemas encountered between partitions.')


class PartitionIterator:
    def __init__(self, dataflow, on_error, cancellation_token: CancellationToken):
        self._iterator_id = str(uuid4())
        self._partition_available_event = Event()
        self._partitions = {}
        self._current_partition = None
        self._next_partition = 0
        self._done = False
        self._cancellation_token = cancellation_token
        get_partitions_reader().register_iterator(self._iterator_id, self)
        _LoggerFactory.trace(
            logger, "PartitionIterator_created", {
                "iterator_id": self._iterator_id})

        def start_iteration():
            dataflow_to_execute = dataflow.add_step('Microsoft.DPrep.WritePartitionsToSocketBlock', {
                'requestId': self._iterator_id,
                'errorStrategy': dataflow._on_error_to_enum_value(on_error)
            })

            try:
                get_engine_api().execute_anonymous_activity(
                    ExecuteAnonymousActivityMessageArguments(anonymous_activity=AnonymousActivityData(blocks=steps_to_block_datas(dataflow_to_execute._steps))),
                    cancellation_token=self._cancellation_token)
            except OperationCanceled:
                pass
            self._clean_up()

        iteration_thread = Thread(target=start_iteration, daemon=True)
        iteration_thread.start()

        cancellation_token.register(self.cancel_iteration)

    def __next__(self):
        while True:
            if self._done and self._current_partition is None and len(self._partitions) == 0:
                raise StopIteration()

            if self._current_partition is None:
                if self._next_partition not in self._partitions:
                    self._partition_available_event.wait()
                    self._partition_available_event.clear()
                    continue
                else:
                    self._current_partition = self._partitions[self._next_partition]
                    self._next_partition = self._next_partition + 1

            if self._current_partition is not None:
                if not have_pyarrow():
                    raise ImportError('PyArrow is not installed.')
                else:
                    import pyarrow
                import pandas
                try:
                    if isinstance(self._current_partition, pyarrow.Table):
                        df = self._current_partition.to_pandas()
                        return df
                    elif isinstance(self._current_partition, pandas.DataFrame):
                        return self._current_partition
                    else:
                        matrix = self._current_partition
                        return matrix
                finally:
                    self._partitions.pop(self._next_partition - 1)
                    self._current_partition = None

    def cancel_iteration(self):
        self._clean_up()

    def process_partition(self, index: int, data: Union['pyarrow.Table', 'scipy.sparse.csr_matrix', 'pandas.DataFrame']):
        if self._cancellation_token.is_canceled:
            raise RuntimeError('IteratorClosed')

        self._partitions[index] = data
        self._partition_available_event.set()

    def _clean_up(self):
        get_partitions_reader().complete_iterator(self._iterator_id)
        self._done = True
        self._partition_available_event.set()


class PartitionIterable:
    def __init__(self, dataflow, on_error):
        self._dataflow = dataflow
        self._cancellation_token = CancellationToken()
        self._on_error = on_error

    def __iter__(self) -> PartitionIterator:
        return PartitionIterator(self._dataflow, self._on_error, self._cancellation_token)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def close(self):
        self._cancellation_token.cancel()


class _PartitionsReader:
    def __init__(self):
        self._iterators = {}

    def register_iterator(self, iterator_id: str, iterator: PartitionIterator):
        self._iterators[iterator_id] = iterator

    def complete_iterator(self, iterator_id: str):
        if iterator_id in self._iterators:
            self._iterators.pop(iterator_id)

    def _read_incoming_partition(self, request_id: str, partition: int, partition_bytes: bytes, data_format: int):
        if request_id not in self._iterators:
            print(request_id)
            print(self._iterators.keys())
            raise ValueError('Invalid request_id')
            
        if data_format == 0: # PREPPY
            if not have_pyarrow():
                raise ImportError('PyArrow is not installed.')
            else:
                from pyarrow import feather
            table = feather.read_table(io.BytesIO(partition_bytes))
            self._iterators[request_id].process_partition(partition, table)
        elif data_format == 2: # No Columns with rows
            row_count = int.from_bytes(partition_bytes, 'little')
            import pandas as pd
            import pyarrow
            table = pd.DataFrame(index=pd.RangeIndex(row_count))
            self._iterators[request_id].process_partition(partition, table)
        else: # NPZ
            try:
                import scipy.sparse as sp
            except:
                raise ImportError('PyArrow is not installed.')
            matrix = sp.load_npz(io.BytesIO(partition_bytes))
            self._iterators[request_id].process_partition(partition, matrix)
            
    def _cancel(self, dataframe_id: str):
        if dataframe_id in self._iterators:
            self._iterators[dataframe_id].cancel_iteration()

_partitions_reader = None


def get_partitions_reader():
    global _partitions_reader
    if _partitions_reader is None:
        _partitions_reader = _PartitionsReader()

    return _partitions_reader


def ensure_partitions_reader_handlers(requests_channel):
    requests_channel.register_handler('send_partition_with_format', process_send_partition_with_format)


def process_send_partition_with_format(request, writer, socket):
    request_id = request.get('request_id')
    partition = request.get('partition')
    try:
        writer.write(json.dumps({'result': 'success'}) + '\n')
        writer.flush()
        data_format = int.from_bytes(socket.recv(4), 'little')
        byte_count = int.from_bytes(socket.recv(8), 'little')
        with socket.makefile('rb') as input:
            partition_bytes = input.read(byte_count)
            get_partitions_reader()._read_incoming_partition(request_id, partition, partition_bytes, data_format)
            writer.write(json.dumps({'result': 'success'}) + '\n')
    except ImportError:
        get_partitions_reader()._cancel(request_id)
        writer.write(json.dumps({'result': 'error', 'error': 'PyArrowMissing'}))
    except Exception as e:
        get_partitions_reader()._cancel(request_id)
        writer.write(json.dumps({'result': 'error', 'error': str(e)}))
