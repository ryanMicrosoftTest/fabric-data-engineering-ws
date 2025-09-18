import io
import os
import json
import math
import warnings
from shutil import rmtree
from threading import Event, Thread, RLock
from typing import List, Tuple, Optional
from uuid import uuid4
from ._pandas_helper import (
    have_pandas,
    have_pyarrow,
    ensure_df_native_compat,
    PandasImportError,
    pyarrow_supports_cdata
)
from ._dataflow_script_resolver import resolve_dataflow
from .engineapi.api import get_engine_api
from .engineapi.engine import CancellationToken
from .engineapi.typedefinitions import (
    ExecuteAnonymousActivityMessageArguments,
    AnonymousActivityData,
    IfDestinationExists,
)
from .errorhandlers import (
    OperationCanceled,
    DataPrepException,
    UnexpectedError,
    StorageAccountLimit,
)
from .step import steps_to_block_datas
from ._rslex_executor import get_rslex_executor, _RsLexDisabledException
from ._loggerfactory import _LoggerFactory, trace

logger = _LoggerFactory.get_logger("DataframeReader")
tracer = trace.get_tracer(__name__)


# 20,000 rows gives a good balance between memory requirement and throughput by requiring that only
# (20000 * CPU_CORES) rows are materialized at once while giving each core a sufficient amount of
# work.
PARTITION_SIZE = 20000


class _InconsistentSchemaError(Exception):
    def __init__(self, reason: str):
        super().__init__(
            "Inconsistent or mixed schemas detected across partitions: " + reason
        )


# noinspection PyPackageRequirements
class _PartitionIterator:
    def __init__(self, partition_id, table):
        self.id = partition_id
        self.is_canceled = False
        self._completion_event = Event()
        self._current_idx = 0
        import pandas as pd

        self._dataframe = (
            table.to_pandas() if not isinstance(table, pd.DataFrame) else table
        )

    def __next__(self):
        if self._current_idx == len(self._dataframe):
            self._completion_event.set()
            raise StopIteration

        value = self._dataframe.iloc[self._current_idx]
        self._current_idx = self._current_idx + 1
        return value

    def wait_for_completion(self):
        self._completion_event.wait()

    def cancel(self):
        self.is_canceled = True
        self._completion_event.set()


# noinspection PyProtectedMember
class RecordIterator:
    def __init__(
        self,
        dataflow: "azureml.dataprep.Dataflow",
        cancellation_token: CancellationToken,
    ):
        self._iterator_id = str(uuid4())
        self._partition_available_event = Event()
        self._partitions = {}
        self._current_partition = None
        self._next_partition = 0
        self._done = False
        self._cancellation_token = cancellation_token
        get_dataframe_reader().register_iterator(self._iterator_id, self)
        _LoggerFactory.trace(
            logger, "RecordIterator_created", {
                "iterator_id": self._iterator_id})

        def start_iteration():
            dataflow_to_execute = dataflow.add_step(
                "Microsoft.DPrep.WriteFeatherToSocketBlock",
                {
                    "dataframeId": self._iterator_id,
                },
            )

            try:
                get_engine_api().execute_anonymous_activity(
                    ExecuteAnonymousActivityMessageArguments(
                        anonymous_activity=AnonymousActivityData(
                            blocks=steps_to_block_datas(
                                dataflow_to_execute._steps))),
                    cancellation_token=self._cancellation_token,
                )
            except OperationCanceled:
                pass
            self._clean_up()

        iteration_thread = Thread(target=start_iteration, daemon=True)
        iteration_thread.start()

        cancellation_token.register(self.cancel_iteration)

    def __next__(self):
        while True:
            if (
                self._done
                and self._current_partition is None
                and len(self._partitions) == 0
            ):
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
                try:
                    return next(self._current_partition)
                except StopIteration:
                    self._partitions.pop(self._current_partition.id)
                    self._current_partition = None

    def cancel_iteration(self):
        for partition in self._partitions.values():
            partition.cancel()
        self._clean_up()

    def process_partition(self, partition: int, table: "pyarrow.Table"):
        if self._cancellation_token.is_canceled:
            raise RuntimeError("IteratorClosed")

        partition_iter = _PartitionIterator(partition, table)
        self._partitions[partition] = partition_iter
        self._partition_available_event.set()
        partition_iter.wait_for_completion()
        if partition_iter.is_canceled:
            raise RuntimeError("IteratorClosed")

    def _clean_up(self):
        _LoggerFactory.trace(
            logger, "RecordIterator_cleanup", {
                "iterator_id": self._iterator_id})
        get_dataframe_reader().complete_iterator(self._iterator_id)
        self._done = True
        self._partition_available_event.set()


class RecordIterable:
    def __init__(self, dataflow):
        self._dataflow = dataflow
        self._cancellation_token = CancellationToken()

    def __iter__(self) -> RecordIterator:
        return RecordIterator(self._dataflow, self._cancellation_token)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def close(self):
        self._cancellation_token.cancel()


def _log_dataflow_execution_activity(
    activity, clex_forced, fallback_allowed, rslex_failed, rslex_error, conversion_failed, conversion_error, clex_failed, clex_error, execution_succeeded, preppy
):
    builder = {}
    builder["activity"] = activity
    if clex_forced is not None:
        builder["clex_forced"] = clex_forced

    if fallback_allowed is not None:
        builder["fallback_allowed"] = fallback_allowed

    if conversion_failed is not None:
        builder["conversion_failed"] = conversion_failed

    if conversion_error is not None:
        builder["conversion_error"] = str(conversion_error)

    if rslex_failed is not None:
        builder['rslex_failed'] = rslex_failed

    if rslex_error is not None:
        builder["rslex_error"] = str(rslex_error)

    if clex_failed is not None:
        builder["clex_failed"] = clex_failed

    if clex_error is not None:
        builder["clex_error"] = str(clex_error)

    if preppy is not None:
        for key in preppy:
            builder['preppy_' + key] = preppy[key]

    if execution_succeeded is not None:
        builder["execution_succeeded"] = execution_succeeded
    try:
        _LoggerFactory.trace(logger, "dataflow_execution", builder)
    except Exception:
        pass


def _write_preppy(
    activity,
    dataflow,
    force_clex=False,
    allow_fallback_to_clex=True,
    span_context=None,
    telemetry_dict=None,
):
    import tempfile
    from pathlib import Path
    import os

    random_id = uuid4()
    subfolder = "{}_{}".format(random_id, os.getpid())
    intermediate_path = Path(os.path.join(tempfile.gettempdir(), subfolder))
    if isinstance(dataflow, str):
        from azureml.dataprep.rslex import PyRsDataflow

        rs_dataflow = PyRsDataflow(dataflow)
        dataflow = str(
            rs_dataflow.add_transformation(
                "write_files",
                {
                    "writer": "preppy",
                    "destination": {
                        "directory": str(intermediate_path),
                        "handler": "Local",
                    },
                    "writer_arguments": {
                        "profiling_fields": ["Kinds", "MissingAndEmpty"]
                    },
                    "existing_file_handling": "replace",
                },
            ).to_yaml_string()
        )
    else:
        dataflow = dataflow.add_step(
            "Microsoft.DPrep.WritePreppyBlock",
            {
                "outputPath": {
                    "target": 0,
                    "resourceDetails": [{"path": str(intermediate_path)}],
                },
                "profilingFields": ["Kinds", "MissingAndEmpty"],
                "ifDestinationExists": IfDestinationExists.REPLACE,
            },
        )

    def cleanup():
        try:
            rmtree(intermediate_path, ignore_errors=True)
        except BaseException:
            pass  # ignore exception

    try:
        _execute(
            activity,
            dataflow,
            force_clex=force_clex,
            span_context=span_context,
            allow_fallback_to_clex=allow_fallback_to_clex,
            cleanup=cleanup,
            telemetry_dict=telemetry_dict,
        )
    except Exception as e:
        cleanup()
        raise e

    if not (intermediate_path / "_SUCCESS").exists():
        error = "Missing _SUCCESS sentinel in preppy folder."
        _LoggerFactory.trace_error(logger, error)
        cleanup()
        raise UnexpectedError(error)

    intermediate_files = sorted([str(p)
                                for p in intermediate_path.glob("part-*")])
    return intermediate_files


def to_pyrecords_with_preppy(activity, dataflow):
    return _execute(activity=activity,
                    dataflow=dataflow,
                    force_preppy=True,
                    convert_preppy_to_pyrecords=True,
                    allow_fallback_to_clex=False)


def _execute(
    activity,
    dataflow,
    is_to_pandas_dataframe=False,
    force_clex=False,
    allow_fallback_to_clex=True,
    force_preppy=False,
    collect_results=False,
    fail_on_error=False,
    fail_on_mixed_types=False,
    fail_on_out_of_range_datetime=False,
    partition_ids=None,
    traceparent="",
    span_context=None,
    cleanup=None,
    extended_types: bool = False,
    nulls_as_nan: bool = True,
    telemetry_dict=None,
    convert_preppy_to_pyrecords=False,
):
    execution_succeeded = False,
    conversion_failed = None
    conversion_error = None
    rslex_failed = None
    rslex_error = None
    clex_failed = None
    clex_error = None
    preppy_telemetry = None

    # turn off fallback if dataflow is yaml script
    if isinstance(dataflow, str):
        allow_fallback_to_clex = False

    dataframe_reader = get_dataframe_reader()

    def rslex_execute():
        nonlocal execution_succeeded
        nonlocal conversion_error
        nonlocal rslex_failed
        nonlocal conversion_failed
        executor = get_rslex_executor()
        try:
            script = (
                resolve_dataflow(dataflow)
                if not isinstance(dataflow, str)
                else dataflow
            )
        except Exception as ex:
            conversion_failed = True
            conversion_error = ex
            raise ex

        # we only want to set conversion_failed to something if we've attempted conversion
        if not isinstance(dataflow, str):
            conversion_failed = False

        (batches, num_partitions, stream_columns) = executor.execute_dataflow(
            script,
            collect_results,
            fail_on_error,
            fail_on_mixed_types,
            fail_on_out_of_range_datetime,
            traceparent,
            partition_ids
        )

        if is_to_pandas_dataframe:
            random_id = str(uuid4())
            dataframe_reader.register_incoming_dataframe(random_id)
            if batches is None:
                raise RuntimeError(
                    "Got no record batches from rslex execution.")

            incoming_dfs = {}
            import pyarrow

            dataframe_reader._incoming_dataframes[random_id] = incoming_dfs
            for i in range(0, len(batches)):
                incoming_dfs[i] = pyarrow.Table.from_batches([batches[i]])
            dataframe = dataframe_reader.complete_incoming_dataframe(
                random_id, partition_stream_columns=stream_columns
            )
            rslex_failed = False
            execution_succeeded = True
            return dataframe
        else:
            rslex_failed = False
            execution_succeeded = True
            return (batches, num_partitions, stream_columns)

    def clex_execute():
        nonlocal execution_succeeded
        nonlocal clex_failed
        from .dataflow import Dataflow
        dataflow_for_execution = dataflow
        if is_to_pandas_dataframe:
            import pandas as pd

            random_id = str(uuid4())
            dataframe_reader.register_incoming_dataframe(random_id)
            dataflow_for_execution = dataflow.add_step(
                "Microsoft.DPrep.WriteFeatherToSocketBlock",
                {
                    "dataframeId": random_id,
                    "errorStrategy": 1 if fail_on_error else 0,
                    "dateTimeSettings": {
                        "minValue": {
                            "timestamp": int(pd.Timestamp.min.value) // 1000000
                        },
                        "maxValue": {
                            "timestamp": int(pd.Timestamp.max.value) // 1000000
                        },
                        "errorStrategy": 1 if fail_on_out_of_range_datetime else 0,
                    },
                },
            )
        activity_data = Dataflow._dataflow_to_anonymous_activity_data(dataflow_for_execution)
        dataflow_for_execution._engine_api.execute_anonymous_activity(
            ExecuteAnonymousActivityMessageArguments(
                anonymous_activity=activity_data, span_context=span_context
            )
        )

        if is_to_pandas_dataframe:
            dataframe = dataframe_reader.complete_incoming_dataframe(random_id)
            clex_failed = False
            execution_succeeded = True
            return dataframe
        else:
            clex_failed = False
            execution_succeeded = True
            return None

    def preppy_execution():
        from collections import OrderedDict

        if not extended_types:
            warnings.warn(
                "Please install pyarrow>=0.16.0 for improved performance of to_pandas_dataframe. "
                "You can ensure the correct version is installed by running: pip install "
                "pyarrow>=0.16.0 --upgrade")
        inner_telemetry_dict = {}
        if not force_clex:
            try:
                # force rslex execution
                intermediate_files = _write_preppy(
                    "_DataframeReader.preppy_execution",
                    dataflow,
                    False,
                    False,
                    span_context,
                    telemetry_dict=inner_telemetry_dict,
                )

                if convert_preppy_to_pyrecords:
                    from azureml.dataprep.native import preppy_to_pyrecords
                    inner_telemetry_dict["to_pyrecords_failed"] = False
                    dataset = preppy_to_pyrecords(intermediate_files)
                    dataset = [{k: pyrec[k] for k in pyrec}
                               for pyrec in dataset]
                else:
                    import pandas
                    from azureml.dataprep.native import preppy_to_ndarrays

                    dataset = preppy_to_ndarrays(
                        intermediate_files, extended_types, nulls_as_nan
                    )
                    inner_telemetry_dict["to_ndarray_failed"] = False
                    dataset = pandas.DataFrame.from_dict(OrderedDict(dataset))

                return (dataset, inner_telemetry_dict)
                
            except Exception as ex:
                error = "Error from preppy_execution: {}".format(repr(ex))
                _LoggerFactory.trace_error(logger, error)

                if convert_preppy_to_pyrecords:
                    inner_telemetry_dict["to_pyrecords_failed"] = True
                    inner_telemetry_dict["to_pyrecords_error"] = repr(ex)
                else:
                    # if inner_telemetry_dict does not have rslex_error and
                    # conversion_error, we failed while converting to ndarrays
                    if (
                        "rslex_error" not in inner_telemetry_dict
                        and "conversion_error" not in inner_telemetry_dict
                    ):
                        inner_telemetry_dict["to_ndarray_failed"] = True
                        inner_telemetry_dict["to_ndarray_error"] = repr(ex)

                if not allow_fallback_to_clex:
                    return ex if isinstance(ex, DataPrepException) else UnexpectedError(ex), inner_telemetry_dict

        try:
            intermediate_files = _write_preppy(
                "write_preppy",
                dataflow,
                True,
                False,
                span_context,
                telemetry_dict=inner_telemetry_dict,
            )

            import pandas
            from azureml.dataprep.native import preppy_to_ndarrays

            dataset = preppy_to_ndarrays(
                intermediate_files, extended_types, nulls_as_nan
            )
            inner_telemetry_dict["to_ndarray_with_clex_forced_failed"] = False
            return pandas.DataFrame.from_dict(
                OrderedDict(dataset)), inner_telemetry_dict
        except Exception as e:
            error = "Error from preppy_execution with clex forced: {}".format(
                repr(e))
            _LoggerFactory.trace_error(logger, error)
            if "clex_error" not in inner_telemetry_dict:
                inner_telemetry_dict["to_ndarray_with_clex_forced_failed"] = True
                inner_telemetry_dict["to_ndarray_with_clex_forced_error"] = repr(
                    e)
            return e if isinstance(e, DataPrepException) else UnexpectedError(e), inner_telemetry_dict

    try:
        if force_preppy:
            df_or_err, preppy_telemetry = preppy_execution()
            if isinstance(df_or_err, Exception):
                execution_succeeded = False
                raise df_or_err
            else:
                execution_succeeded = True
                return df_or_err
        if not force_clex:
            try:
                return rslex_execute()
            except _RsLexDisabledException as ex:
                if not allow_fallback_to_clex:
                    _LoggerFactory.trace(
                        logger,
                        "RsLex is disabled but is being forced.",
                        {"activity": activity},
                    )
                    raise RuntimeError(
                        "RsLex is disabled but is being forced.")
                _LoggerFactory.trace(
                    logger,
                    "RustLex disabled. Falling back to CLex.",
                    {"activity": activity},
                )
                rslex_failed = True
                rslex_error = ex
                force_clex = True
            except _InconsistentSchemaError as e:
                rslex_failed = True
                rslex_error = e
                reason = e.args[0]
                warnings.warn("Using alternate reader. " + reason)
                df_or_err, preppy_telemetry = preppy_execution()
                if isinstance(df_or_err, Exception):
                    execution_succeeded = False
                    raise df_or_err
                else:
                    execution_succeeded = True
                    return df_or_err
            except Exception as e:
                if conversion_error is None:
                    rslex_failed = True
                    rslex_error = e
                if "is over the account limit" in str(rslex_error):
                    execution_succeeded = False
                    raise StorageAccountLimit(str(rslex_error))
                if not allow_fallback_to_clex:
                    execution_succeeded = False
                    raise e
                _LoggerFactory.trace_warn(logger, "rslex failed, falling back to clex.")
                pass

        # if clex is forced or if rslex failed and fallback is enabled or if
        # conversion failed and fallback is enabled, execute with clex
        if (
            force_clex
            or rslex_error is not None and allow_fallback_to_clex
            or conversion_error is not None and allow_fallback_to_clex
        ):
            cleanup() if cleanup is not None else None
            try:
                return clex_execute()
            except _InconsistentSchemaError as e:
                clex_failed = True
                clex_error = e
                reason = e.args[0]
                warnings.warn("Using alternate reader. " + reason)
                df_or_err, preppy_telemetry = preppy_execution()
                if isinstance(df_or_err, Exception):
                    execution_succeeded = False
                    raise df_or_err
                else:
                    execution_succeeded = True
                    return df_or_err
            except Exception as e:
                clex_failed = True
                clex_error = e
                execution_succeeded = False
                # clex doesn't support many parquet files and if fallback fails with this error it actually masks the underlying cause
                # of rslex execution failure which is more useful to the user
                if (
                    "Current parquet file is not supported" in str(clex_error)
                    and rslex_error is not None
                ):
                    raise rslex_error if isinstance(rslex_error, DataPrepException) else UnexpectedError(rslex_error, str(rslex_error))
                else:
                    raise e
    finally:
        if telemetry_dict is not None:
            if rslex_failed is not None:
                telemetry_dict["rslex_failed"] = rslex_failed
            if rslex_error is not None:
                telemetry_dict["rslex_error"] = repr(rslex_error)
            if conversion_failed is not None:
                telemetry_dict["conversion_failed"] = conversion_failed
            if conversion_error is not None:
                telemetry_dict["conversion_error"] = repr(conversion_error)
            if clex_failed is not None:
                telemetry_dict["clex_failed"] = clex_failed
            if clex_error is not None:
                telemetry_dict["clex_error"] = repr(clex_error)
            if execution_succeeded is not None:
                telemetry_dict["execution_succeeded"] = execution_succeeded
        else:
            _log_dataflow_execution_activity(
                activity,
                force_clex,
                allow_fallback_to_clex,
                rslex_failed,
                rslex_error,
                conversion_failed,
                conversion_error,
                clex_failed,
                clex_error,
                execution_succeeded,
                preppy_telemetry)

    return None


def get_partition_count_with_rslex(dataflow, span_context=None):
    _, partition_count, _ = _execute('_DataframeReader.get_partition_count_with_rslex', dataflow,
                                     span_context=span_context, collect_results=False, allow_fallback_to_clex=False)
    return partition_count

def get_partition_info_with_fallback(dataflow, span_context=None) -> Tuple[int, List[Tuple['StreamInfo', int]]]:
    execution_succeeded = False
    conversion_error = None
    rslex_failed = None
    conversion_failed = None
    rslex_error = None
    force_clex = False
    allow_fallback_to_clex = True
    clex_failed = None
    clex_error = None

    def rslex_execute():
        nonlocal conversion_failed
        nonlocal conversion_error
        nonlocal rslex_failed
        nonlocal rslex_error
        executor = get_rslex_executor()
        try:
            script = (
                resolve_dataflow(dataflow)
                if not isinstance(dataflow, str)
                else dataflow
            )
        except Exception as ex:
            conversion_failed = True
            conversion_error = ex
            raise ex

        try:
            (num_partitions, partitions_streams_and_counts) = executor.get_partition_info(script, '')
            return (num_partitions, partitions_streams_and_counts)
        except BaseException as ex:
            rslex_failed = True
            rslex_error = ex
            raise ex

    def clex_execute():
        nonlocal execution_succeeded
        dataflow._raise_if_missing_secrets()
        count = dataflow._engine_api.get_partition_count(steps_to_block_datas(dataflow._steps))
        execution_succeeded = True
        return (count, None)

    if '_TEST_USE_CLEX' in os.environ and os.environ['_TEST_USE_CLEX'] == 'True' or not pyarrow_supports_cdata():
        force_clex = True
    elif '_TEST_USE_CLEX' in os.environ and os.environ['_TEST_USE_CLEX'] == 'False':
        allow_fallback_to_clex = False
    try:
        if not force_clex:
            try:
                return rslex_execute()
            except _RsLexDisabledException as ex:
                if not allow_fallback_to_clex:
                    _LoggerFactory.trace(
                        logger,
                        'RsLex is disabled but is being forced.',
                        {'activity': 'get_partition_info'},
                    )
                    rslex_error = ex
                    rslex_failed = True
                    raise RuntimeError(
                        '[get_partition_info()] RsLex is disabled but is being forced.')
                _LoggerFactory.trace(
                    logger,
                    'RustLex disabled. Falling back to CLex.',
                    {'activity': 'get_partition_info'},
                )
                rslex_failed = True
                rslex_error = ex
                force_clex = True
            except Exception as e:
                if conversion_error is None:
                    rslex_failed = True
                    rslex_error = e
                if "is over the account limit" in str(rslex_error):
                    execution_succeeded = False
                    raise StorageAccountLimit(str(rslex_error))
                if not allow_fallback_to_clex:
                    execution_succeeded = False
                    rslex_error = e
                    raise e
                _LoggerFactory.trace_warn(logger, "rslex failed, falling back to clex.")
                pass
        
        if (
            force_clex
            or rslex_error is not None and allow_fallback_to_clex
            or conversion_error is not None and allow_fallback_to_clex
        ):
            try:
                return clex_execute()
            except Exception as e:
                clex_failed = True
                clex_error = e
                execution_succeeded = False
                # clex doesn't support many parquet files and if fallback fails with this error it actually masks the underlying cause
                # of rslex execution failure which is more useful to the user
                if (
                    "Current parquet file is not supported" in str(clex_error)
                    and rslex_error is not None
                ):
                    raise rslex_error if isinstance(rslex_error, DataPrepException) else UnexpectedError(rslex_error, str(rslex_error))
                else:
                    raise e

    finally:
        _log_dataflow_execution_activity(
                activity='_DataframeReader.get_partition_info',
                clex_forced=False,
                fallback_allowed=True,
                rslex_failed=rslex_failed,
                rslex_error=rslex_error,
                conversion_failed=conversion_failed,
                conversion_error=conversion_error,
                clex_failed=clex_failed,
                clex_error=clex_error,
                execution_succeeded=execution_succeeded,
                preppy=None)



# noinspection PyProtectedMember,PyPackageRequirements
class _DataFrameReader:
    def __init__(self):
        self._outgoing_dataframes = {}
        self._incoming_dataframes = {}
        self._iterators = {}
        _LoggerFactory.trace(logger, "DataframeReader_create")

    def to_pandas_dataframe(
        self,
        dataflow,
        extended_types: bool = False,
        nulls_as_nan: bool = True,
        on_error: str = "null",
        out_of_range_datetime: str = "null",
        span_context: "DPrepSpanContext" = None,
    ) -> "pandas.DataFrame":
        if not have_pandas():
            raise PandasImportError()

        allow_fallback_to_clex = True
        force_clex = False
        force_preppy = False

        if not have_pyarrow() or extended_types:
            force_preppy = True

        if '_TEST_USE_CLEX' in os.environ and os.environ['_TEST_USE_CLEX'] == 'True' or not pyarrow_supports_cdata():
            force_clex = True
        elif '_TEST_USE_CLEX' in os.environ and os.environ['_TEST_USE_CLEX'] == 'False':
            allow_fallback_to_clex = False

        return _execute(
            '_DataframeReader.to_pandas_dataframe',
            dataflow=dataflow,
            force_clex=force_clex,
            is_to_pandas_dataframe=True,
            force_preppy=force_preppy,
            allow_fallback_to_clex=allow_fallback_to_clex,
            collect_results=True,
            fail_on_error=on_error != "null",
            fail_on_mixed_types=on_error != "null",
            fail_on_out_of_range_datetime=out_of_range_datetime != "null",
            traceparent=span_context.span_id if span_context is not None else "",
            span_context=span_context,
            cleanup=None,
            extended_types=extended_types,
            nulls_as_nan=nulls_as_nan,
        )

    def _rslex_to_pandas_with_fallback(
        self,
        dataflow
    ):
        if not have_pandas():
            raise PandasImportError()

        if not pyarrow_supports_cdata():
            raise UnexpectedError("pyarrow does not support cdata")

        force_clex = False
        force_preppy = False

        if not have_pyarrow():
            force_preppy = True

        return _execute(
            '_DataframeReader._rslex_to_pandas_with_fallback',
            dataflow=dataflow,
            force_clex=force_clex,
            is_to_pandas_dataframe=True,
            force_preppy=force_preppy,
            allow_fallback_to_clex=False,
            collect_results=True
        )

    def register_outgoing_dataframe(
        self, dataframe: "pandas.DataFrame", dataframe_id: str
    ):
        _LoggerFactory.trace(
            logger, "register_outgoing_dataframes", {
                "dataframe_id": dataframe_id})
        self._outgoing_dataframes[dataframe_id] = dataframe

    def unregister_outgoing_dataframe(self, dataframe_id: str):
        self._outgoing_dataframes.pop(dataframe_id)

    def _get_partitions(self, dataframe_id: str) -> int:
        dataframe = self._outgoing_dataframes[dataframe_id]
        partition_count = math.ceil(len(dataframe) / PARTITION_SIZE)
        return partition_count

    def _get_data(self, dataframe_id: str, partition: int) -> bytes:
        from azureml.dataprep import native

        dataframe = self._outgoing_dataframes[dataframe_id]
        start = partition * PARTITION_SIZE
        end = min(len(dataframe), start + PARTITION_SIZE)
        dataframe = dataframe.iloc[start:end]

        (new_schema, new_values) = ensure_df_native_compat(dataframe)

        return native.preppy_from_ndarrays(new_values, new_schema)

    def register_incoming_dataframe(self, dataframe_id: str):
        _LoggerFactory.trace(
            logger, "register_incoming_dataframes", {
                "dataframe_id": dataframe_id})
        self._incoming_dataframes[dataframe_id] = {}

    def complete_incoming_dataframe(
        self, dataframe_id: str, partition_stream_columns=None
    ) -> "pandas.DataFrame":
        import pyarrow
        import pandas as pd

        partitions_dfs = self._incoming_dataframes[dataframe_id]
        if any(
            isinstance(
                partitions_dfs[key],
                pd.DataFrame) for key in partitions_dfs):
            raise _InconsistentSchemaError("A partition has no columns.")

        partitions_dfs = [
            partitions_dfs[key]
            for key in sorted(partitions_dfs.keys())
            if partitions_dfs[key].num_rows > 0
        ]
        _LoggerFactory.trace(
            logger,
            "complete_incoming_dataframes",
            {"dataframe_id": dataframe_id, "count": len(partitions_dfs)},
        )
        self._incoming_dataframes.pop(dataframe_id)

        if len(partitions_dfs) == 0:
            return pd.DataFrame({})

        def get_column_names(partition: pyarrow.Table) -> List[str]:
            return partition.schema.names

        def verify_column_names():
            def make_schema_error(prefix, p1_cols, p2_cols):
                return _InconsistentSchemaError(
                    "{0} The first partition has {1} columns. Found partition has {2} columns.\n".format(
                        prefix,
                        len(p1_cols),
                        len(p2_cols)) +
                    "First partition columns (ordered): {0}\n".format(p1_cols) +
                    "Found Partition has columns (ordered): {0}".format(p2_cols))

            expected_names = get_column_names(partitions_dfs[0])
            expected_count = partitions_dfs[0].num_columns
            row_count = 0
            size = 0
            for partition in partitions_dfs:
                row_count += partition.num_rows
                size += partition.nbytes
                found_names = get_column_names(partition)
                if partition.num_columns != expected_count:
                    _LoggerFactory.trace(
                        logger,
                        "complete_incoming_dataframes.column_count_mismatch",
                        {"dataframe_id": dataframe_id},
                    )
                    raise make_schema_error(
                        "partition had different number of columns.",
                        expected_names,
                        found_names,
                    )
                for (a, b) in zip(expected_names, found_names):
                    if a != b:
                        _LoggerFactory.trace(
                            logger, "complete_incoming_dataframes.column_names_mismatch", {
                                "dataframe_id": dataframe_id}, )
                        raise make_schema_error(
                            "partition column had different name than expected.",
                            expected_names,
                            found_names,
                        )

            _LoggerFactory.trace(
                logger,
                "complete_incoming_dataframes.info",
                {
                    "dataframe_id": dataframe_id,
                    "count": len(partitions_dfs),
                    "row_count": row_count,
                    "size_bytes": size,
                },
            )

        def determine_column_type(index: int) -> pyarrow.DataType:
            for partition in partitions_dfs:
                column = partition.column(index)
                if (
                    column.type != pyarrow.bool_()
                    or column.null_count != column.length()
                ):
                    return column.type
            return pyarrow.bool_()

        def apply_column_types(fields: List[pyarrow.Field]):
            for i in range(0, len(partitions_dfs)):
                partition = partitions_dfs[i]
                column_types = partition.schema.types
                for j in range(0, len(fields)):
                    column_type = column_types[j]
                    if column_type != fields[j].type:
                        if column_type == pyarrow.bool_():
                            column = partition.column(j)
                            import numpy as np

                            def gen_n_of_x(n, x):
                                k = 0
                                while k < n:
                                    yield x
                                    k = k + 1

                            if isinstance(column, pyarrow.ChunkedArray):
                                typed_chunks = []
                                for chunk in column.chunks:
                                    typed_chunks.append(
                                        pyarrow.array(
                                            gen_n_of_x(
                                                chunk.null_count, None), fields[j].type, mask=np.full(
                                                chunk.null_count, True), ))

                                partition = partition.remove_column(j)
                                try:
                                    partition = partition.add_column(
                                        j,
                                        fields[j],
                                        pyarrow.chunked_array(typed_chunks),
                                    )
                                except Exception as e:
                                    message = "Failed to add column to partition. Target type: {}, actual type: bool, partition id: {}, column idx: {}, error: {}, ".format(
                                        fields[j].type, i, j, e)
                                    _LoggerFactory.trace_error(logger, message)
                                    raise _InconsistentSchemaError(
                                        "A partition has a column with a different type than expected during append.\nThe type of column "
                                        "'{0}' in the first partition is {1}. In partition '{2}' found type is {3}.".format(
                                            partition.schema.names[j], str(
                                                fields[j].type), i, str(column_type), ))
                            else:
                                new_col = pyarrow.column(
                                    fields[j],
                                    pyarrow.array(
                                        gen_n_of_x(column.null_count, None),
                                        fields[j].type,
                                        mask=np.full(column.null_count, True),
                                    ),
                                )
                                partition = partition.remove_column(j)
                                partition = partition.add_column(j, new_col)
                            partitions_dfs[i] = partition
                        elif column_type != pyarrow.null():
                            if fields[j].type == pyarrow.null():
                                fields[j] = pyarrow.field(
                                    fields[j].name, column_type)
                            else:
                                _LoggerFactory.trace(
                                    logger, "complete_incoming_dataframes.column_type_mismatch", {
                                        "dataframe_id": dataframe_id}, )
                                raise _InconsistentSchemaError(
                                    "A partition has a column with a different type than expected.\nThe type of column "
                                    "'{0}' in the first partition is {1}. In partition '{2}' found type is {3}.".format(
                                        partition.schema.names[j], str(
                                            fields[j].type), i, str(column_type), ))

        def get_concatenated_stream_columns():
            first_partition_stream_columns = partition_stream_columns[0]
            stream_columns = {}
            # initialize dictionary
            for (paths, values) in first_partition_stream_columns:
                if len(paths) == 1:
                    stream_columns[paths[0]] = values
                else:
                    _LoggerFactory.trace(
                        logger,
                        "get_concatenated_stream_columns.failure_path_count",
                        {"path_count": len(paths), "partition": 0},
                    )
                    return None
            stream_column_count = len(stream_columns.keys())
            for i in range(1, len(partition_stream_columns)):
                if len(partition_stream_columns[i]) != stream_column_count:
                    # found different count of stream columns as compared to
                    # first partition
                    _LoggerFactory.trace(
                        logger,
                        "get_concatenated_stream_columns.failure_stream_count",
                        {
                            "stream_count": len(partition_stream_columns[i]),
                            "partition": i,
                            "first_partition_count": stream_column_count,
                        },
                    )
                    return None
                for (paths, values) in partition_stream_columns[i]:
                    if len(
                            paths) != 1 or paths[0] not in stream_columns.keys():
                        _LoggerFactory.trace(
                            logger, "get_concatenated_stream_columns.failure_column_mismatch", {
                                "path_count": len(paths), "partition": i}, )
                        return None
                    stream_columns[paths[0]].extend(values)

            return stream_columns

        def set_stream_columns(df, stream_columns):
            if stream_columns is not None:
                value_count = 0
                for (column, values) in stream_columns.items():
                    df[column] = values
                    value_count = len(values)
                _LoggerFactory.trace(
                    logger,
                    "set_stream_columns.success",
                    {
                        "shape": "({},{})".format(
                            len(stream_columns.keys()), value_count
                        )
                    },
                )

        with tracer.start_as_current_span(
                "_DataFrameReader.complete_incoming_dataframe", trace.get_current_span()
        ):
            verify_column_names()
            first_partition = partitions_dfs[0]
            column_fields = []
            names = first_partition.schema.names
            for i in range(0, first_partition.num_columns):
                f = pyarrow.field(names[i], determine_column_type(i))
                column_fields.append(f)
            apply_column_types(column_fields)

            import pyarrow

            df = pyarrow.concat_tables(partitions_dfs, promote=True).to_pandas(
                use_threads=True
            )
            if partition_stream_columns:
                stream_columns = get_concatenated_stream_columns()
                set_stream_columns(df, stream_columns)
            _LoggerFactory.trace(
                logger,
                "complete_incoming_dataframes.success",
                {"dataframe_id": dataframe_id, "shape": str(df.shape)},
            )
            return df

    def register_iterator(self, iterator_id: str, iterator: RecordIterator):
        _LoggerFactory.trace(
            logger, "register_iterator", {
                "iterator_id": iterator_id})
        self._iterators[iterator_id] = iterator

    def complete_iterator(self, iterator_id: str):
        _LoggerFactory.trace(
            logger, "complete_iterator", {
                "iterator_id": iterator_id})
        if iterator_id in self._iterators:
            self._iterators.pop(iterator_id)

    def _read_incoming_partition(
            self,
            dataframe_id: str,
            partition: int,
            partition_bytes: bytes,
            is_from_file: bool,
    ):
        if not have_pyarrow():
            raise ImportError("PyArrow is not installed.")
        else:
            from pyarrow import feather, ArrowInvalid
        _LoggerFactory.trace(
            logger,
            "read_incoming_partition",
            {"dataframe_id": dataframe_id, "partition": partition},
        )

        if is_from_file:
            import os

            name = partition_bytes.decode("utf-8")
            try:
                table = feather.read_table(name)
            except ArrowInvalid as e:
                size = os.path.getsize(name)
                if size != 8:
                    raise e

                with open(name, "rb") as file:
                    count_bytes = file.read(8)
                    row_count = int.from_bytes(count_bytes, "little")
                    import pandas as pd

                    table = pd.DataFrame(index=pd.RangeIndex(row_count))
            finally:
                # noinspection PyBroadException
                try:
                    os.remove(name)
                except BaseException:
                    pass
        else:
            # Data is transferred as either Feather or just a count of rows when the partition consisted of records with
            # no columns. Feather streams are always larger than 8 bytes, so we can detect that we are dealing with only
            # a row count by checking if we received exactly 8 bytes.
            if len(partition_bytes) == 8:  # No Columns partition.
                row_count = int.from_bytes(partition_bytes, "little")
                import pandas as pd

                table = pd.DataFrame(index=pd.RangeIndex(row_count))
            else:
                table = feather.read_table(io.BytesIO(partition_bytes))

        if dataframe_id in self._incoming_dataframes:
            partitions_dfs = self._incoming_dataframes[dataframe_id]
            partitions_dfs[partition] = table
        elif dataframe_id in self._iterators:
            self._iterators[dataframe_id].process_partition(partition, table)
        else:
            _LoggerFactory.trace(
                logger,
                "dataframe_id_not_found",
                {
                    "dataframe_id": dataframe_id,
                    "current_dataframe_ids": str(
                        list(self._incoming_dataframes.keys())
                    ),
                },
            )
            raise ValueError("Invalid dataframe_id: {}".format(dataframe_id))

    def _cancel(self, dataframe_id: str):
        if dataframe_id in self._iterators:
            self._iterators[dataframe_id].cancel_iteration()
        elif dataframe_id in self._incoming_dataframes:
            self._incoming_dataframes[dataframe_id] = {}


_dataframe_reader = None
_dataframe_reader_with_script = None
_dataframe_reader_lock = RLock()


def get_dataframe_reader():
    global _dataframe_reader
    if _dataframe_reader is None:
        with _dataframe_reader_lock:
            if _dataframe_reader is None:
                _dataframe_reader = _DataFrameReader()

    return _dataframe_reader


def ensure_dataframe_reader_handlers(requests_channel):
    requests_channel.register_handler(
        "get_dataframe_partitions", process_get_partitions
    )
    requests_channel.register_handler(
        "get_dataframe_partition_data",
        process_get_data)
    requests_channel.register_handler(
        "send_dataframe_partition", process_send_partition
    )


def process_get_partitions(request, writer, socket):
    dataframe_id = request.get("dataframe_id")
    try:
        partition_count = get_dataframe_reader()._get_partitions(dataframe_id)
        writer.write(json.dumps(
            {"result": "success", "partitions": partition_count}))
    except Exception as e:
        writer.write(json.dumps({"result": "error", "error": repr(e)}))


def process_get_data(request, writer, socket):
    dataframe_id = request.get("dataframe_id")
    partition = request.get("partition")
    try:
        partition_bytes = get_dataframe_reader()._get_data(dataframe_id, partition)
        byte_count = len(partition_bytes)
        byte_count_bytes = byte_count.to_bytes(4, "little")
        socket.send(byte_count_bytes)
        socket.send(partition_bytes)
    except Exception as ex:
        writer.write(json.dumps({"result": "error", "error": repr(ex)}))


def process_send_partition(request, writer, socket):
    dataframe_id = request.get("dataframe_id")
    partition = request.get("partition")
    is_from_file = request.get("is_from_file")
    try:
        writer.write(json.dumps({"result": "success"}) + "\n")
        writer.flush()
        byte_count = int.from_bytes(socket.recv(8), "little")
        with socket.makefile("rb") as input:
            partition_bytes = input.read(byte_count)
            get_dataframe_reader()._read_incoming_partition(
                dataframe_id, partition, partition_bytes, is_from_file
            )
            writer.write(json.dumps({"result": "success"}) + "\n")
    except Exception as e:
        get_dataframe_reader()._cancel(dataframe_id)
        writer.write(json.dumps(
            {"result": "error", "error": _get_error_details(e)}))


def _get_error_details(e):
    errorCode = type(e).__name__
    errorMessage = str(e)
    return {"errorCode": errorCode, "errorMessage": errorMessage}
