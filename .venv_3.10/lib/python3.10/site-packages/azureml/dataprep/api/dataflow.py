# Copyright (c) Microsoft Corporation. All rights reserved.
from sys import excepthook
from .engineapi.api import EngineAPI, get_engine_api
from .engineapi import typedefinitions
from .engineapi.typedefinitions import (FileEncoding, InvalidLineHandling,
                                        ExecuteAnonymousActivityMessageArguments,
                                        ColumnsSelectorDetails, ColumnsSelectorType, SingleColumnSelectorDetails,
                                        ColumnsSelector, CodeBlockType, FieldType,
                                        ActivityReference, GetSecretsMessageArguments, PropertyValues,
                                        AssertPolicy, AddBlockToListMessageArguments, BlockArguments, ValueKind,
                                        InspectorArguments, ExecuteInspectorCommonResponse, AnonymousActivityData,
                                        AnonymousBlockData, SaveActivityFromDataMessageArguments,
                                        GetSourceDataHashMessageArguments, _get_prop_descriptions,
                                        ComparePolicy, ValidateActivitySourceMessageArguments,
                                        ActivitySourceValidationResult, IfDestinationExists)
from .errorhandlers import EmptyStepsError, DataPrepException
from .dataprofile import DataProfile
from .inspector import _Inspector, _InspectorBatch, BaseInspector
from ._pandas_helper import have_pandas, have_pyarrow, PandasImportError
from .datasources import MSSQLDataSource, PostgreSQLDataSource, LocalDataSource, DataSource, FileOutput, DataDestination, FileDataSource
from .references import make_activity_reference, ExternalReference, _to_anonymous_reference
from .step import (Step, steps_to_block_datas, single_column_to_selector_value, MultiColumnSelection,
                   column_selection_to_selector_value)
from .builders import Builders
from .typeconversions import (TypeConverter, DateTimeConverter, FloatConverter, StreamInfoConverter)
from .types import SplitExample, Delimiters
from .expressions import Expression, col, _ensure_expression
from .value import value_from_field, to_dprep_value
from ._archiveoption import ArchiveOptions
from ._datastore_helper import (datastore_to_dataflow, get_datastore_value, _is_datapath, _is_datapaths, Datastore,
                                Datastores)
from ._dataframereader import (get_dataframe_reader, get_partition_count_with_rslex, get_partition_info_with_fallback, RecordIterable, _write_preppy,
                               _execute)
from ._partitionsreader import PartitionIterable
from ._loggerfactory import _LoggerFactory, track, trace
from .sparkexecution import SparkExecutor
from .tracing._open_telemetry_adapter import to_dprep_span_context
from typing import List, Dict, cast, Tuple, TypeVar, Optional, Any, Union, Callable
from pathlib import Path
from textwrap import dedent, indent
from copy import copy, deepcopy
try:
    # Importing ABCs from collections is removed in PY3.10
    from collections.abc import Iterable
except ImportError:
    from collections import Iterable
import random
import tempfile
import datetime
from uuid import uuid4
from shutil import rmtree
import os
from enum import Enum
import warnings

FilePath = TypeVar('FilePath', FileDataSource, str, List[str], Datastores)
DatabaseSource = TypeVar('DatabaseSource', MSSQLDataSource, PostgreSQLDataSource, Datastore)
DataflowReference = TypeVar('DataflowReference', 'Dataflow', ExternalReference, 'Dataset', 'DatasetDefinition')
DatabaseDestination = TypeVar('DatabaseDestination', MSSQLDataSource, Datastore)

logger = None
tracer = trace.get_tracer(__name__)

def get_logger():
    global logger
    if logger is not None:
        return logger

    logger = _LoggerFactory.get_logger("Dataflow")
    return logger


class DataflowValidationError(DataPrepException):
    def __init__(self, message: str):
        super().__init__(message, "Empty", message)


# >>> BEGIN GENERATED CLASSES
class SummaryFunction(Enum):
    """
    Enum SummaryFunction.

    .. remarks::

        Possible enum values are:
        - MIN: Minimum value.
        - MAX: Maximum value.
        - MEAN: Mean value.
        - MEDIAN: Median value.
        - VAR: Variance value.
        - SD: Standard deviance value.
        - COUNT: Total count.
        - SUM: Sum of all value.
        - SKEWNESS: Skewness value.
        - KURTOSIS: Kurtosis value.
        - TOLIST: Aggregate values into a list.
        - TOPVALUES: Top values.
        - BOTTOMVALUES: Bottom values.
        - SINGLE: Single value.
        - COMMONPATH: Common Path.
    """
    MIN = 0
    MAX = 1
    MEAN = 2
    MEDIAN = 3
    VAR = 4
    SD = 5
    COUNT = 8
    SUM = 11
    SKEWNESS = 18
    KURTOSIS = 19
    TOLIST = 25
    TOPVALUES = 30
    BOTTOMVALUES = 31
    SINGLE = 32
    COMMONPATH = 33


class MismatchAsOption(Enum):
    """
    Enum MismatchAsOption.

    .. remarks::

        Possible enum values are:
        - ASTRUE: Mismatch value as true.
        - ASFALSE: Mismatch value as false.
        - ASERROR: Mismatch value as error.
    """
    ASTRUE = 0
    ASFALSE = 1
    ASERROR = 2


class TrimType(Enum):
    """
    Enum TrimType.

    .. remarks::

        Possible enum values are:
        - WHITESPACE: Trim white space.
        - CUSTOM: Trim custom characters.
    """
    WHITESPACE = 0
    CUSTOM = 1


class DecimalMark(Enum):
    """
    Enum DecimalMark.

    .. remarks::

        Possible enum values are:
        - DOT: Using dot as decimal mark.
        - COMMA: Using comma as decimal mark.
    """
    DOT = 0
    COMMA = 1


class JoinType(Enum):
    """
    Describes different possible types of join.

    .. remarks::

        Possible enum values are:
        - MATCH: Only records with matching join keys will be returned.
        - INNER: Only records with matching join keys will be returned.
        - UNMATCHLEFT: Records from left data set that did not match with anything.
        - LEFTANTI: Records from left data set that did not match with anything.
        - LEFTOUTER: All records from left data set and only matching records from the right data set.
        - UNMATCHRIGHT: Records from right data set that did not match with anything.
        - RIGHTANTI: Records from right data set that did not match with anything.
        - RIGHTOUTER: All records from right data set and only matching records from the right data set.
        - FULLANTI: Only unmatched records from left and right data sets.
        - FULL: All records from both left and right data sets.
    """
    NONE = 0
    MATCH = 2
    INNER = 2
    UNMATCHLEFT = 4
    LEFTANTI = 4
    LEFTOUTER = 6
    UNMATCHRIGHT = 8
    RIGHTANTI = 8
    RIGHTOUTER = 10
    FULLANTI = 12
    FULL = 14


class SkipMode(Enum):
    """
    Defines a strategy to skip rows when reading files

    .. remarks::

        Possible enum values are:
        - NONE: Don't skip any rows.
        - UNGROUPED: Skip rows from the first file only.
        - FIRSTFILE: Skip rows from the first file only.
        - GROUPED: Skip rows from every file.
        - ALLFILES: Skip rows from every file.
    """
    NONE = 0
    UNGROUPED = 1
    FIRSTFILE = 1
    GROUPED = 2
    ALLFILES = 2


class PromoteHeadersMode(Enum):
    """
    Defines strategy to promote headers when reading files

    .. remarks::

        Possible enum values are:
        - NONE: Do not promote headers. Use when file(s) has no headers.
        - UNGROUPED: Promote headers from the first file. All subsequent files are considered to be just data.
        - FIRSTFILE: Promote headers from the first file. All subsequent files are considered to be just data.
        - GROUPED: Promote headers from every file. Allows to read files with inconsistent schema.
        - ALLFILES: Promote headers from every file. Allows to read files with inconsistent schema.
        - CONSTANTGROUPED: Optimized option for the case when all the files have exactly same headers. In effect this will promote headers from the first file and skip a row for every other file.
        - SAMEALLFILES: Optimized option for the case when all the files have exactly same headers. In effect this will promote headers from the first file and skip a row for every other file.
    """
    NONE = 0
    UNGROUPED = 1
    FIRSTFILE = 1
    GROUPED = 2
    ALLFILES = 2
    CONSTANTGROUPED = 3
    SAMEALLFILES = 3


class SType(Enum):
    """
    Defines suported semantic types

    .. remarks::

        Possible enum values are:
        - EMAILADDRESS: Email address.
        - GEOGRAPHICCOORDINATE: Common representations of geographic coordinates.
        - IPV4ADDRESS: IPv4 address.
        - IPV6ADDRESS: IPv6 address.
        - USPHONENUMBER: Common formats of US phone numbers.
        - ZIPCODE: Common formats of US ZIP code.
    """
    EMAILADDRESS = 0
    GEOGRAPHICCOORDINATE = 1
    IPV4ADDRESS = 2
    IPV6ADDRESS = 3
    USPHONENUMBER = 4
    ZIPCODE = 5


class DatastoreValue:
    """
    Properties uniquely identifying a datastore.

    :param subscription: The subscription the workspace belongs to.
    :param resource_group: The resource group the workspace belongs to.
    :param workspace_name: The workspace the datastore belongs to.
    :param datastore_name: The datastore to read the data from.
    :param path: The path on the datastore.
    """
    def __init__(self,
                 subscription: str,
                 resource_group: str,
                 workspace_name: str,
                 datastore_name: str,
                 path: str = ''):
        self.subscription = subscription
        self.resource_group = resource_group
        self.workspace_name = workspace_name
        self.datastore_name = datastore_name
        self.path = path

    def _to_pod(self) -> Dict[str, Any]:
        return {
            'subscription': self.subscription,
            'resourceGroup': self.resource_group,
            'workspaceName': self.workspace_name,
            'datastoreName': self.datastore_name,
            'path': self.path,
        }


class _HistogramArgumentsValue:
    """
    Additional arguments required for Histogram summary function.

    :param histogram_bucket_count: Number of buckets to use.
    """
    def __init__(self,
                 histogram_bucket_count: int):
        self.histogram_bucket_count = histogram_bucket_count

    def _to_pod(self) -> Dict[str, Any]:
        return {
            'histogramBucketCount': self.histogram_bucket_count,
        }


class _KernelDensityArgumentsValue:
    """
    Additional arguments required for KernelDensity summary function.

    :param kernel_density_point_count: Number of kernel density points to calculate.
    :param kernel_density_bandwidth: Kernel density bandwidth.
    """
    def __init__(self,
                 kernel_density_point_count: int,
                 kernel_density_bandwidth: float):
        self.kernel_density_point_count = kernel_density_point_count
        self.kernel_density_bandwidth = kernel_density_bandwidth

    def _to_pod(self) -> Dict[str, Any]:
        return {
            'kernelDensityPointCount': self.kernel_density_point_count,
            'kernelDensityBandwidth': self.kernel_density_bandwidth,
        }


class _SummaryColumnsValue:
    """
    Column summarization definition.

    :param column_id: Column to summarize.
    :param summary_function: Aggregation function to use.
    :param summary_column_name: Name of the new column holding the aggregate values.
    :param histogram_arguments: Additional arguments required for Histogram summary function.
    :param kernel_density_arguments: Additional arguments required for KernelDensity summary function.
    :param quantiles: Quantile boundary values required for Quantiles summary function.
    """
    def __init__(self,
                 column_id: str,
                 summary_function: SummaryFunction,
                 summary_column_name: str,
                 histogram_arguments: Optional[_HistogramArgumentsValue] = None,
                 kernel_density_arguments: Optional[_KernelDensityArgumentsValue] = None,
                 quantiles: Optional[List[float]] = None):
        self.column_id = column_id
        self.summary_function = summary_function
        self.summary_column_name = summary_column_name
        self.histogram_arguments = histogram_arguments
        self.kernel_density_arguments = kernel_density_arguments
        self.quantiles = quantiles

    def _to_pod(self) -> Dict[str, Any]:
        return {
            'columnId': self.column_id,
            'summaryFunction': self.summary_function,
            'summaryColumnName': self.summary_column_name,
            'histogramArguments': self.histogram_arguments,
            'kernelDensityArguments': self.kernel_density_arguments,
            'quantiles': self.quantiles,
        }


class _SummaryFunctionsValue:
    """
    Summarization definition for each column.

    :param summary_function: Aggregation function to use.
    :param histogram_arguments: Additional arguments required for Histogram summary function.
    :param kernel_density_arguments: Additional arguments required for KernelDensity summary function.
    :param quantiles: Quantile boundary values required for Quantiles summary function.
    """
    def __init__(self,
                 summary_function: SummaryFunction,
                 histogram_arguments: Optional[_HistogramArgumentsValue] = None,
                 kernel_density_arguments: Optional[_KernelDensityArgumentsValue] = None,
                 quantiles: Optional[List[float]] = None):
        self.summary_function = summary_function
        self.histogram_arguments = histogram_arguments
        self.kernel_density_arguments = kernel_density_arguments
        self.quantiles = quantiles

    def _to_pod(self) -> Dict[str, Any]:
        return {
            'summaryFunction': self.summary_function,
            'histogramArguments': self.histogram_arguments,
            'kernelDensityArguments': self.kernel_density_arguments,
            'quantiles': self.quantiles,
        }
# <<< END GENERATED CLASSES


# Export as public classes
HistogramArgumentsValue = _HistogramArgumentsValue
KernelDensityArgumentsValue = _KernelDensityArgumentsValue
SummaryColumnsValue = _SummaryColumnsValue


class Dataflow:
    """
    A Dataflow represents a series of lazily-evaluated, immutable operations on data.
        It is only an execution plan. No data is loaded from the source until you get data from the Dataflow using one of `head`, `to_pandas_dataframe`, `get_profile` or the write methods.

    .. remarks::

        Dataflows are usually created by supplying a data source. Once the data source has been provided, operations
            can be added by invoking the different transformation methods available on this class. The result of adding
            an operation to a Dataflow is always a new Dataflow.

        The actual loading of the data and execution of the transformations is delayed as much as possible and will not
            occur until a 'pull' takes place. A pull is the action of reading data from a Dataflow, whether by asking to
            look at the first N records in it or by transferring the data in the Dataflow to another storage mechanism
            (a Pandas Dataframe, a CSV file, or a Spark Dataframe).

        The operations available on the Dataflow are runtime-agnostic. This allows the transformation pipelines
            contained in them to be portable between a regular Python environment and Spark.
    """

    def __init__(self, engine_api: EngineAPI, steps: List[Step] = None, meta: Dict[str, str] = None):
        self._engine_api = engine_api
        self._steps = steps if steps is not None else []

        self.builders = Builders(self, engine_api)
        self._spark_executor = SparkExecutor(engine_api)
        self._meta = meta if meta is not None else {}
        self._rs_dataflow_yaml = None

    def add_step(self,
                 step_type: str,
                 arguments: Dict[str, Any],
                 local_data: Dict[str, Any] = None) -> 'Dataflow':
        self._rs_dataflow_yaml = None
        new_step = Step(step_type, arguments, local_data)
        meta = self._copy_and_update_step_count(self._meta)
        return Dataflow(self._engine_api, self._steps + [new_step], meta)

    def _copy_and_update_step_count(self, prev_meta: Dict[str, Any]) -> Dict[str, Any]:
        meta = deepcopy(prev_meta)
        try:
            count = int(meta['steps_added'])
            meta['steps_added'] = str(count + 1)
        except:
            # either key not found or found with something other than count
            meta['steps_added'] = '1'
        return meta

    def _with_partition_size(self, partition_size: int) -> 'Dataflow':
        if len(self._steps) == 0:
            raise ValueError('Partition size can\'t be changed on an empty Dataflow.')
        
        # put an arbitrary limit on partition size. Large partitions are likely to cause OOM so capping accepted value here.
        if partition_size > 2*1024*1024*1024:
            raise ValueError('Partition size can\'t exceed 2 GB.')

        def get_partition_step_and_descriptions():
            i = -1
            for step in self._steps:
                i += 1
                step_descriptions = _get_prop_descriptions(step.step_type)
                for prop_description in step_descriptions:
                    if prop_description.name == 'partitionSize':
                        return step, i, step_descriptions
            return None, None, None

        partition_step, step_idx, descriptions = get_partition_step_and_descriptions()
        if partition_step is None:
            return self

        args_pod = deepcopy(partition_step.arguments.to_pod())
        args_pod['partitionSize'] = partition_size
        new_step = Step(partition_step.step_type,
                        PropertyValues.from_pod(args_pod, descriptions),
                        deepcopy(partition_step.local_data))
        return Dataflow(self._engine_api, self._steps[0:step_idx] + [new_step] + self._steps[step_idx + 1:])

    @staticmethod
    def _get_property_value(property, property_name: str):
        return property[property_name] if isinstance(property, dict) else property.to_pod()[property_name]

    def _get_steps(self) -> List[Step]:
        return [s for s in self._steps]

    def execute_inspector(self, inspector: BaseInspector) -> ExecuteInspectorCommonResponse:
        self._raise_if_missing_secrets()
        return _Inspector._from_execution(
            self._engine_api,
            make_activity_reference(self),
            inspector)

    def _execute_inspector(self, inspector: Union[str, InspectorArguments]) -> ExecuteInspectorCommonResponse:
        self._raise_if_missing_secrets()
        return _Inspector._from_execution(
            self._engine_api,
            make_activity_reference(self),
            inspector)

    def execute_inspectors(self, inspectors: List[BaseInspector]) -> Dict[InspectorArguments, ExecuteInspectorCommonResponse]:
        self._raise_if_missing_secrets()
        return _InspectorBatch._from_execution(
            self._engine_api,
            make_activity_reference(self),
            inspectors)

    def _execute_inspectors(self, inspectors: Union[str, List[InspectorArguments]]) -> Dict[InspectorArguments, ExecuteInspectorCommonResponse]:
        self._raise_if_missing_secrets()
        return _InspectorBatch._from_execution(
            self._engine_api,
            make_activity_reference(self),
            inspectors)

    # noinspection PyUnresolvedReferences
    @track(get_logger)
    def get_profile(self, include_stype_counts: bool = False, number_of_histogram_bins: int = 10) -> DataProfile:
        """
        Requests the data profile which collects summary statistics on the full data produced by the Dataflow.
            A data profile can be very useful to understand the input data, identify anomalies and missing values,
            and verify that data preparation operations produced the desired result.

        :param include_stype_counts: Whether to include checking if values look like some well known semantic types of
            information. For Example, "email address". Turning this on will impact performance.
        :type include_stype_counts: bool
        :param number_of_histogram_bins: Number of bins in a histogram. If not specified will be set to 10.
        :type number_of_histogram_bins: int
        :return: DataProfile object
        :rtype: azureml.dataprep.DataProfile
        """
        return self._get_profile(include_stype_counts, number_of_histogram_bins)

    def _get_profile(self,
                     include_stype_counts: bool = False,
                     number_of_histogram_bins: int = 10,
                     include_average_spaces_count: bool = False,
                     include_string_lengths: bool = False) -> DataProfile:
        """
        Actual get_profile implementation
        """
        self._raise_if_missing_secrets()
        activity_ref = _to_anonymous_reference(self)
        return DataProfile._from_execution(self._engine_api,
                                           activity_ref,
                                           include_stype_counts,
                                           number_of_histogram_bins,
                                           include_average_spaces_count,
                                           include_string_lengths)

    @property
    def dtypes(self) -> Dict[str, FieldType]:
        """
        Gets column data types for the current dataset by calling :meth:`get_profile` and extracting just dtypes from
            the resulting DataProfile.

        .. note:

            This will trigger a data profile calculation.
            To avoid calculating profile multiple times, get the profile first by calling \
            `get_profile()` and then inspect it.

        :return: A dictionary, where key is the column name and value is :class:`azureml.dataprep.FieldType`.
        """
        profile = self._get_profile()
        return profile.dtypes

    @property
    def row_count(self) -> int:
        """
        Count of rows in this Dataflow.

        .. note::

            This will trigger a data profile calculation. To avoid calculating profile multiple times, get the profile first by calling \
            `get_profile()` and then inspect it.

        .. note::

            If current Dataflow contains `take_sample` step or 'take' step, this will return number of rows in the \
            subset defined by those steps.

        :return: Count of rows.
        :rtype: int
        """
        profile = self._get_profile()
        return profile.row_count

    @property
    def shape(self) -> Tuple[int,int]:
        """
        Shape of the data produced by the Dataflow.

        .. note::

            This will trigger a data profile calculation. To avoid calculating profile multiple times, get the profile first by calling \
            `get_profile()` and then inspect it.

        .. note::

            If current Dataflow contains `take_sample` step or 'take' step, this will return number of rows in the \
            subset defined by those steps.

        :return: Tuple of row count and column count.
        """
        profile = self._get_profile()
        return profile.shape

    def head(self, count: int = 10) -> 'pandas.DataFrame':
        """
        Pulls the number of records specified from the top of this Dataflow and returns them as a `Link pandas.DataFrame <https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html>`_.

        :param count: The number of records to pull. 10 is default.
        :return: A Pandas `Link pandas.DataFrame <https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html>`_.
        """
        return self.take(count).to_pandas_dataframe(extended_types=True)

    @track(get_logger)
    def get_partition_count(self) -> int:
        """
        Calculates the partitions for the current Dataflow and returns their count. Partitioning is guaranteed to be stable for a specific execution mode.

        :return: The count of partitions.
        """
        parent = trace.get_current_span()
        try:
            with tracer.start_as_current_span('Dataflow.get_partition_count', parent) as span:
                return get_partition_count_with_rslex(self, span_context=to_dprep_span_context(span.get_context()))
        except BaseException as e:
            logger.warning('failed to get partiton count with rslex, falling back to clex: {}'.format(e))
            pass
        self._raise_if_missing_secrets()
        return self._engine_api.get_partition_count(steps_to_block_datas(self._steps))

    @track(get_logger)
    def get_partition_info(self) -> Tuple[int, List[Tuple['StreamInfo', int]]]:
        """
        Calculates the partitions for the current Dataflow and returns total count and a list of partition source and partition count in that source.
        Partitioning is guaranteed to be stable for a specific execution mode.
        Partition source list would be a tuple of (StreamInfo, count) if it can be tied directly to a file or None in other cases.

        :return: The count of partitions.
        """
        parent = trace.get_current_span()
        with tracer.start_as_current_span('Dataflow.get_partition_info', parent) as span:
            return get_partition_info_with_fallback(self, span_context=to_dprep_span_context(span.get_context()))

    @track(get_logger)
    def run_local(self) -> None:
        """
        Runs the current Dataflow using the local execution runtime.
        """
        force_clex = False
        allow_fallback_to_clex = True
        if '_TEST_USE_CLEX' in os.environ and os.environ['_TEST_USE_CLEX'] == 'True':
            force_clex = True
        elif '_TEST_USE_CLEX' in os.environ and os.environ['_TEST_USE_CLEX'] == 'False':
            allow_fallback_to_clex = False
        parent = trace.get_current_span()
        with tracer.start_as_current_span('Dataflow.run_local', parent) as span:
            self._raise_if_missing_secrets()
            _execute('Dataflow.run_local', self, force_clex=force_clex, allow_fallback_to_clex=allow_fallback_to_clex, span_context=to_dprep_span_context(span.get_context()))

    @track(get_logger)
    def run_spark(self) -> None:
        """
        Runs the current Dataflow using the Spark runtime.
        """
        self._raise_if_missing_secrets()
        return self._spark_executor.execute(steps_to_block_datas(self._steps), use_sampling=False)

    @staticmethod
    def _on_error_to_enum_value(on_error: str) -> int:
        if on_error == 'null':
            return 0
        elif on_error == 'fail':
            return 1
        else:
            raise ValueError('Invalid value for on_error. Expected \'null\' or \'fail\'.')

    @staticmethod
    def _out_of_range_datetime_to_block_value(out_of_range_datetime: str) -> dict:
        import pandas as pd
        error_strategy = Dataflow._on_error_to_enum_value(out_of_range_datetime)
        return {
            'minValue': {'timestamp': int(pd.Timestamp.min.value) // 1000000},
            'maxValue': {'timestamp': int(pd.Timestamp.max.value) // 1000000},
            'errorStrategy': error_strategy
        }

    # noinspection PyUnresolvedReferences
    @track(get_logger)
    def to_pandas_dataframe(self,
                            extended_types: bool = False,
                            nulls_as_nan: bool = True,
                            on_error: str = 'null',
                            out_of_range_datetime: str = 'null') -> 'pandas.DataFrame':
        """
        Pulls all of the data and returns it as a Pandas `Link pandas.DataFrame <https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html>`_.

        .. remarks::

            This method will load all the data returned by this Dataflow into memory.

            Since Dataflows do not require a fixed, tabular schema but Pandas DataFrames do, an implicit tabularization
                step will be executed as part of this action. The resulting schema will be the union of the schemas of all
                records produced by this Dataflow.

        :param extended_types: Whether to keep extended DataPrep types such as DataPrepError in the DataFrame. If False,
            these values will be replaced with None.
        :param nulls_as_nan: Whether to interpret nulls (or missing values) in number typed columns as NaN. This is
            done by pandas for performance reasons; it can result in a loss of fidelity in the data.
        :param on_error: How to handle any error values in the Dataflow, such as those produced by an error while parsing values.
            Valid values are 'null' which replaces them with null; and 'fail' which will result in an exception.
        :param out_of_range_datetime: How to handle date-time values that are outside the range supported by Pandas.
            Valid values are 'null' which replaces them with null; and 'fail' which will result in an exception.
        :return: A Pandas `Link pandas.DataFrame <https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html>`_.
        """
        with tracer.start_as_current_span('Dataflow.to_pandas_dataframe', trace.get_current_span()) as span:
            span_context = to_dprep_span_context(span.get_context())
            return get_dataframe_reader().to_pandas_dataframe(self,
                                                              extended_types,
                                                              nulls_as_nan,
                                                              on_error,
                                                              out_of_range_datetime,
                                                              span_context)

    @track(get_logger)
    def to_dask_dataframe(self,
                          sample_size: int = 10000,
                          dtypes: dict = None,
                          extended_types: bool = False,
                          nulls_as_nan: bool = True,
                          on_error: str = 'null',
                          out_of_range_datetime: str = 'null'):
        """
        Returns a Dask DataFrame that can lazily read the data in the Dataflow.

        .. remarks::
            Dask DataFrames allow for parallel and lazy processing of data by splitting the data into multiple
                partitions. Because Dask DataFrames don't actually read any data until a computation is requested,
                it is necessary to determine what the schema and types of the data will be ahead of time. This is done
                by reading a specific number of records from the Dataflow (specified by the `sample_size` parameter).
                However, it is possible for these initial records to have incomplete information. In those cases, it is
                possible to explicitly specify the expected columns and their types by providing a dict of the shape
                `{column_name: dtype}` in the `dtypes` parameter.

        :param sample_size: The number of records to read to determine schema and types.
        :param dtypes: An optional dict specifying the expected columns and their dtypes.
            `sample_size` is ignored if this is provided.
        :param extended_types: Whether to keep extended DataPrep types such as DataPrepError in the DataFrame. If False,
            these values will be replaced with None.
        :param nulls_as_nan: Whether to interpret nulls (or missing values) in number typed columns as NaN. This is
            done by pandas for performance reasons; it can result in a loss of fidelity in the data.
        :param on_error: How to handle any error values in the Dataflow, such as those produced by an error while parsing values.
            Valid values are 'null' which replaces them with null; and 'fail' which will result in an exception.
        :param out_of_range_datetime: How to handle date-time values that are outside the range supported by Pandas.
            Valid values are 'null' which replaces them with null; and 'fail' which will result in an exception.
        :return: A Dask DataFrame.
        """
        from ._dask_helper import have_dask, DaskImportError
        if not (have_dask() and have_pandas()):
            raise DaskImportError()
        else:
            import dask.dataframe as dd
            from dask.delayed import delayed
            import numpy as np
            import pandas

        def is_optimization_supported(source_dataflow):
            if len(source_dataflow._steps) < 1:
                return False
            if 'DATASET_DISABLE_DASK_OPTIMIZATION' in os.environ and os.environ['DATASET_DISABLE_DASK_OPTIMIZATION'] == 'True':
                _LoggerFactory.trace(get_logger(), '[to_dask_dataframe()] Optimization is disabled with env var: DATASET_DISABLE_DASK_OPTIMIZATION')
                import warnings
                warnings.warn("Experimental large dataset performance optimization is disabled with env var: DATASET_DISABLE_DASK_OPTIMIZATION")
                return False 
            step_type = source_dataflow._steps[0].step_type
            if step_type == 'Microsoft.DPrep.GetDatastoreFilesBlock' or \
               step_type == 'Microsoft.DPrep.GetFilesBlock' or \
               step_type == 'Microsoft.DPrep.CreateDatasetBlock' or \
               step_type == 'Microsoft.DPrep.CreateDatasetFilesBlock':
                return True
            else:
                _LoggerFactory.trace(get_logger(), '[to_dask_dataframe()] Optimization of the dataflow is not supported for the step type: {}'.format(step_type))
                return False
        
        partition_count = None
        sources = None
        if is_optimization_supported(self):
            try:
                (partition_count, sources) = self.get_partition_info()
            except Exception as e:
                _LoggerFactory.trace_error(get_logger(), '[to_dask_dataframe()] Failed to get partition info: {}'.format(e))
                pass
        #  If the optimization is not supported or getting partition_info has failed, we will fallback to getting partition count. 
        if partition_count is None:
            partition_count = self.get_partition_count()

        if partition_count <= 0:
            import pandas
            return dd.from_pandas(pandas.DataFrame(), chunksize=1)

        field_type_to_dtypes = {
            FieldType.STRING: object,
            FieldType.BOOLEAN: bool,
            FieldType.INTEGER: int,
            FieldType.DECIMAL: float,
            FieldType.DATE: 'datetime64[ns]',
            FieldType.UNKNOWN: object,
            FieldType.ERROR: object,
            FieldType.NULL: object,
            FieldType.DATAROW: object,
            FieldType.LIST: object,
            FieldType.STREAM: object
        }

        dataflow_dtypes = dtypes or {k: field_type_to_dtypes[v] for k, v in self.take(sample_size).dtypes.items()}

        delayed_functions = None
        if sources is not None:
            try:
                import warnings
                warnings.warn("Using experimental performance optimization for large datasets. To disable set 'DATASET_DISABLE_DASK_OPTIMIZATION' env var to True.")
                delayed_functions = [delayed(self._optimized_get_pandas_dataframe_from_partition_info)(source.to_pod(), index_in_source, extended_types, nulls_as_nan, on_error, out_of_range_datetime) 
                                        for source, count_in_source in sources
                                        for index_in_source in range(count_in_source)]
                _LoggerFactory.trace(get_logger(), '[to_dask_dataframe()] Using optimized pandas dataframe conversion for {} partitions'.format(len(delayed_functions)))
            except Exception as e:
                _LoggerFactory.trace_error(get_logger(), '[to_dask_dataframe()] Failed to use optimized pandas dataframe conversion. Falling back to non-optimized conversion. Error: {}'.format(e))
                
        # either no sources or failed to use optimized conversion, fall back to non-optimized conversion
        if delayed_functions is None:
            delayed_functions = [delayed(self._partition_to_pandas_dataframe)(i, extended_types, nulls_as_nan, on_error, out_of_range_datetime) for i in range(0, partition_count)]

        ddf = dd.from_delayed(delayed_functions, meta=dataflow_dtypes)
        return ddf

    def _optimized_get_pandas_dataframe_from_partition_info(self, source, index_in_source, extended_types, nulls_as_nan, on_error, out_of_range_datetime):
            # replace first block in the source dataflow with a single partition StreamInfo, allowing direct file access without any listing.
            new_source_step = Step(step_type='Microsoft.DPrep.CreateDatasetBlock',
                                   arguments={'streamInfos': [source]})
            optimized_dataflow = Dataflow(self._engine_api, [new_source_step] + self._steps[1:], self._meta)
            return optimized_dataflow.select_partitions([index_in_source]).to_pandas_dataframe(extended_types, nulls_as_nan, on_error, out_of_range_datetime)

    def _to_pyrecords(self):
        from azureml.dataprep.native import preppy_to_pyrecords

        with tracer.start_as_current_span('Dataflow._to_pyrecords', trace.get_current_span()) as span:
            self._raise_if_missing_secrets()
            force_clex = False
            allow_fallback_to_clex = True
            if '_TEST_USE_CLEX' in os.environ:
                if os.environ['_TEST_USE_CLEX'] == 'True':
                    force_clex = True
                elif os.environ['_TEST_USE_CLEX'] == 'False':
                    allow_fallback_to_clex = False
            intermediate_files = _write_preppy('Dataflow.to_pyrecords', self, span_context=to_dprep_span_context(span.get_context()), allow_fallback_to_clex=allow_fallback_to_clex, force_clex=force_clex)
            if len(intermediate_files) == 0:
                return []
            dataset = preppy_to_pyrecords(intermediate_files)
            realized_records = []
            for pyrec in dataset:
                pyrec_dict = {}
                for key in pyrec.keys():
                    pyrec_dict[key] = pyrec[key]
                realized_records.append(pyrec_dict)
            return realized_records

    # noinspection PyUnresolvedReferences
    @track(get_logger)
    def to_spark_dataframe(self, spark_session: 'pyspark.sql.SparkSession' = None) -> 'pyspark.sql.DataFrame':
        """
        Creates a Spark `Link pyspark.sql.DataFrame <https://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.DataFrame>`_ that can execute the transformation pipeline defined by this Dataflow.

        .. remarks::

            Since Dataflows do not require a fixed, tabular schema but Spark Dataframes do, an implicit tabularization
                step will be executed as part of this action. The resulting schema will be the union of the schemas of all
                records produced by this Dataflow. This tabularization step will result in a pull of the data.

            .. note::

                The Spark Dataframe returned is only an execution plan and doesn't actually contain any data, since Spark Dataframes are also lazily evaluated.

        :return: A Spark `Link pyspark.sql.DataFrame <https://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.DataFrame>`_.
        """

        # Try to convert this dataflow to an rsdataflow and get a spark dataframe that way.
        with tracer.start_as_current_span('Dataflow.to_spark_dataframe::v2', trace.get_current_span()) as span:
            try:
                from ._dataflow_script_resolver import resolve_dataflow
                from ._spark_helper import read_spark_dataframe

                if not spark_session:
                    from pyspark.sql import SparkSession
                    try:
                        spark_session = SparkSession.getActiveSession()
                    except AttributeError:
                        spark_session = SparkSession.builder.getOrCreate()

                if not spark_session:
                    raise EnvironmentError("Failed to acquire a spark session. Either populate the spark_session parameter, or invoke this method from a thread that has an active spark session.")

                return read_spark_dataframe(self, spark_session)
            except:
                pass

        # That didn't work. Try using the legacy path to get a spark dataframe from this dataflow.
        with tracer.start_as_current_span('Dataflow.to_spark_dataframe::legacy', trace.get_current_span()) as span:
            self._raise_if_missing_secrets()
            return self._spark_executor.get_dataframe(steps_to_block_datas(self._steps), use_sampling=False)

    @track(get_logger)
    def to_record_iterator(self) -> RecordIterable:
        """
        Creates an iterable object that returns the records produced by this Dataflow in sequence. This iterable
        must be closed before any other executions can run.

        :return: A RecordIterable object that can be used to iterate over the records in this Dataflow.
        """
        if not have_pandas():
            raise PandasImportError()

        return RecordIterable(self)

    @track(get_logger)
    def to_partition_iterator(self, on_error: str = 'null') -> PartitionIterable:
        """
        Creates an iterable object that returns the partitions produced by this Dataflow in sequence. This iterable
        must be closed before any other executions can run.

        :param on_error: How to handle any error values in the Dataflow, such as those produced by an error while parsing values.
            Valid values are 'null' which replaces them with null; and 'fail' which will result in an exception.
        :return: A PartitionsIterable object that can be used to iterate over the partitions in this Dataflow.
        """
        if not have_pandas():
            raise PandasImportError()

        return PartitionIterable(self, on_error=on_error)

    # noinspection PyUnresolvedReferences
    @track(get_logger)
    def verify_has_data(self):
        """
        Verifies that this Dataflow would produce records if executed. An exception will be thrown otherwise.
        """
        with tracer.start_as_current_span('Dataflow.verify_has_data', trace.get_current_span()):
            if len(self._steps) == 0:
                raise EmptyStepsError()

            if len(self.take(1)._to_pyrecords()) == 0:
                raise DataflowValidationError("The Dataflow produced no records.")

    # noinspection PyUnresolvedReferences
    @track(get_logger)
    def _verify_has_data_using_profile(self):
        if len(self._steps) == 0:
            raise EmptyStepsError()

        profile = self.take(1)._get_profile()
        if profile.row_count == 0 or profile.row_count is None:
            raise DataflowValidationError("The Dataflow produced no records.")

    @track(get_logger)
    def has_invalid_source(self, return_validation_error=False):
        """
        Verifies if the Dataflow has invalid source.

        :param return_validation_error: Action to take when source is know to be invalid. Options are:
            `True` Returns error message. This is useful to gather more information regarding the failure
            occurred while checking whether the source is valid or not.
            `False` return True.
        :return: Return following based on the parameter checked
            - returns error message if show_error_message == True and source known to be invalid.
            - returns True if show_error_message == False and source known to be invalid.
            - returns False if source known to valid or unknown.
        """
        with tracer.start_as_current_span('Dataflow.has_invalid_source', trace.get_current_span()) as span:
            result = self._engine_api.validate_activity_source(ValidateActivitySourceMessageArguments(
                anonymous_activity=Dataflow._dataflow_to_anonymous_activity_data(self),
                span_context=to_dprep_span_context(span.get_context())
            ))

            if result.state != ActivitySourceValidationResult.INVALID:
                return False

            if return_validation_error:
                return result.error_message

            return True

    def _raise_if_multi_char(self, name: str, value: str):
        if value and len(value) > 1:
           raise ValueError('Only single character is supported for argument: ' + name)

    def _validate_partition_size(self, partition_size: Optional[int] = None):
        if partition_size is not None and partition_size < 1024 * 1024 * 4:
            raise ValueError('partition_size must be at least 4 MB (4 * 1024 * 1024).')

    def parse_delimited(self,
                        separator: str,
                        headers_mode: PromoteHeadersMode,
                        encoding: FileEncoding,
                        quoting: bool,
                        skip_rows: int,
                        skip_mode: SkipMode,
                        comment: str,
                        partition_size: Optional[int] = None,
                        empty_as_string: bool = False) -> 'Dataflow':
        """
        Adds step to parse CSV data.

        :param separator: The separator to use to split columns.
        :param headers_mode: How to determine column headers.
        :param encoding: The encoding of the files being read.
        :param quoting: Whether to handle new line characters within quotes. This option will impact performance.
        :param skip_rows: How many rows to skip.
        :param skip_mode: The mode in which rows are skipped.
        :param comment: Character used to indicate a line is a comment instead of data in the files being read.
        :param partition_size: Desired partition size.
        :param empty_as_string: Whether to keep empty field values as empty strings. Default is read them as null.
        :return: A new Dataflow with Parse Delimited Step added.
        """
        self._raise_if_multi_char('separator', separator)
        self._raise_if_multi_char('comment', comment)
        self._validate_partition_size(partition_size)
        return self.add_step('Microsoft.DPrep.ParseDelimitedBlock', {
            'columnHeadersMode': headers_mode,
            'separator': separator,
            'commentLineCharacter': comment,
            'fileEncoding': encoding,
            'skipRowsMode': skip_mode,
            'skipRows': skip_rows,
            'handleQuotedLineBreaks': quoting,
            'partitionSize': partition_size,
            'emptyAsString': empty_as_string
        })

    def parse_fwf(self,
                  offsets: List[int],
                  headers_mode: PromoteHeadersMode,
                  encoding: FileEncoding,
                  skip_rows: int,
                  skip_mode: SkipMode) -> 'Dataflow':
        """
        Adds step to parse fixed-width data.

        :param offsets: The offsets at which to split columns. The first column is always assumed to start at offset 0.
        :param headers_mode: How to determine column headers.
        :param encoding: The encoding of the files being read.
        :param skip_rows: How many rows to skip.
        :param skip_mode: The mode in which rows are skipped.
        :return: A new Dataflow with Parse FixedWidthFile Step added.
        """
        return self.add_step('Microsoft.DPrep.ParseFixedWidthColumns', {
            'columnHeadersMode': headers_mode,
            'columnOffsets': offsets,
            'fileEncoding': encoding,
            'skipRowsMode': skip_mode,
            'skipRows': skip_rows,
        })

    def parse_lines(self,
                    headers_mode: PromoteHeadersMode,
                    encoding: FileEncoding,
                    skip_rows: int,
                    skip_mode: SkipMode,
                    comment: str,
                    partition_size: Optional[int] = None) -> 'Dataflow':
        """
        Adds step to parse text files and split them into lines.

        :param headers_mode: How to determine column headers.
        :param encoding: The encoding of the files being read.
        :param skip_rows: How many rows to skip.
        :param skip_mode: The mode in which rows are skipped.
        :param comment: Character used to indicate a line is a comment instead of data in the files being read.
        :param partition_size: Desired partition size.
        :return: A new Dataflow with Parse Lines Step added.
        """
        self._raise_if_multi_char('comment', comment)
        self._validate_partition_size(partition_size)
        return self.add_step('Microsoft.DPrep.ParsePlainTextBlock', {
            'columnHeadersMode': headers_mode,
            'commentLineCharacter': comment,
            'fileEncoding': encoding,
            'skipRowsMode': skip_mode,
            'skipRows': skip_rows,
            'partitionSize': partition_size
        })

    def parse_json_lines(self,
                         encoding: FileEncoding,
                         partition_size: Optional[int] = None,
                         invalid_lines: InvalidLineHandling = InvalidLineHandling.ERROR) -> 'Dataflow':
        """
        Creates a new Dataflow with the operations required to read JSON lines files.

        :param invalid_lines: How to handle invalid JSON lines.
        :param encoding: The encoding of the files being read.
        :param partition_size: Desired partition size.
        :return: A new Dataflow with Read JSON line Step added.
        """
        return self.add_step('Microsoft.DPrep.ParseJsonLinesBlock', {
            'invalidLines': invalid_lines,
            'fileEncoding': encoding,
            'partitionSize': partition_size
        })

    def read_sql(self, data_source: MSSQLDataSource, query: str, query_timeout: int = 30) -> 'Dataflow':
        """
        Adds step that can read data from an MS SQL database by executing the query specified.

        :param data_source: The details of the MS SQL database.
        :param query: The query to execute to read data.
        :param query_timeout: Sets the wait time (in seconds) before terminating the attempt to execute a command
            and generating an error. The default is 30 seconds.
        :return: A new Dataflow with Read SQL Step added.
        """

        if not isinstance(data_source, MSSQLDataSource):
            raise ValueError('Please provide MSSQLDataSource.')

        return self.add_step('Database', {
            'server': data_source.server,
            'database': data_source.database,
            'credentialsType': data_source.credentials_type,
            'credentials': {
                'userName': data_source.user_name,
                'password': data_source.password
            },
            'query': query,
            'trustServer': data_source.trust_server,
            'databaseType': data_source.database_type,
            'queryTimeout': query_timeout
        })

    def write_to_sql(self,
        destination: DatabaseDestination,
        table: str,
        batch_size: int = 500,
        default_string_length: int = None) -> 'Dataflow':
        """
        Adds step that writes out the data in the Dataflow into a table in MS SQL database.

        :param destination: The details of the MS SQL database or AzureSqlDatabaseDatastore.
        :param table: Name of the table to write data to.
        :param batch_size: Size of a batch of records to commit to SQL server in a single request.
        :param default_string_length: Length of string to be supported when creating a table.

        :return: A new Dataflow with write step added.
        """

        if not table or len(table) == 0:
            raise ValueError('Please provide valid table name.')

        if isinstance(destination, MSSQLDataSource):
            return self.add_step('Microsoft.DPrep.WriteToDatabaseTableBlock', {
                'server': destination.server,
                'database': destination.database,
                'credentialsType': destination.credentials_type,
                'credentials': {
                    'userName': destination.user_name,
                    'password': destination.password
                },
                'trustServer': destination.trust_server,
                'databaseType': destination.database_type,
                'table': table,
                'batchSize': batch_size,
                'defaultStringLength' : default_string_length
            })

        try:
            from azureml.data.azure_sql_database_datastore import AzureSqlDatabaseDatastore
            if isinstance(destination, AzureSqlDatabaseDatastore):
                _, datastore_value = get_datastore_value(destination)
                datastore_value.path = table
                return self.add_step('Microsoft.DPrep.WriteToDatabaseDatastoreBlock', {
                    'datastore': datastore_value._to_pod(),
                    'batchSize': batch_size,
                    'defaultStringLength' : default_string_length
                    })
        except ImportError:
            raise ValueError('Please provide MSSQLDataSource or install "azureml-sdk" and provide a AzureSqlDatabaseDatastore.')

        raise ValueError('Please provide MSSQLDataSource or AzureSqlDatabaseDatastore')

    def read_postgresql(self, data_source: PostgreSQLDataSource, query: str, query_timeout: int = 20) -> 'Dataflow':
        """
        Adds step that can read data from a PostgreSQL database by executing the query specified.

        :param data_source: The details of the PostgreSQL database.
        :param query: The query to execute to read data.
        :param query_timeout: Sets the wait time (in seconds) before terminating the attempt to execute a command
            and generating an error. The default is 20 seconds.
        :return: A new Dataflow with Read SQL Step added.
        """
        return self.add_step('Database', {
            'server': data_source.server,
            'database': data_source.database,
            'credentialsType': data_source.credentials_type,
            'credentials': {
                'userName': data_source.user_name,
                'password': data_source.password
            },
            'query': query,
            'databaseType': data_source.database_type,
            'port': data_source.port_number,
            'sslMode': data_source.ssl_mode,
            'queryTimeout': query_timeout
         })


    def read_parquet_file(self) -> 'Dataflow':
        """
        Adds step to parse Parquet files.

        :return: A new Dataflow with Parse Parquet File Step added.
        """
        return self.add_step('Microsoft.DPrep.ReadParquetFileBlock', {})


    def read_preppy(self) -> 'Dataflow':
        """
        Adds step to read a directory containing Preppy files.

        :return: A new Dataflow with Read Preppy Step added.
        """
        return self.add_step('Microsoft.DPrep.ReadPreppyBlock', {})


    def read_excel(self,
                   sheet_name: Optional[str] = None,
                   use_column_headers: bool = False,
                   skip_rows: int = 0) -> 'Dataflow':
        """
        Adds step to read and parse Excel files.

        :param sheet_name: The name of the sheet to load. The first sheet is loaded if none is provided.
        :param use_column_headers: Whether to use the first row as column headers.
        :param skip_rows: Number of rows to skip when loading the data.
        :return: A new Dataflow with Read Excel Step added.
        """
        return self.add_step('Microsoft.DPrep.ReadExcelBlock', {
                               'sheetName': sheet_name,
                               'useColumnHeaders': use_column_headers,
                               'skipRows': skip_rows
                            })

    def read_json(self,
                  json_extract_program: str,
                  encoding: FileEncoding):
        """
        Creates a new Dataflow with the operations required to read JSON files.

        :param json_extract_program: PROSE program that will be used to parse JSON.
        :param encoding: The encoding of the files being read.
        :return: A new Dataflow with Read JSON Step added.
        """
        return self.add_step('JSONFile', {
            'dsl': json_extract_program,
            'fileEncoding': encoding
        })

    TypeConversionInfo = TypeVar('TypeConversionInfo',
                                 FieldType,
                                 TypeConverter,
                                 Tuple[FieldType, List[str], Tuple[FieldType, str]])

    def read_npz_file(self) -> 'Dataflow':
        """
        Adds step to parse npz files.

        :return: A new Dataflow with Read Npz File Step added.
        """
        return self.add_step('Microsoft.DPrep.ReadNpzFileBlock', {})

    def set_column_types(self, type_conversions: Dict[str, TypeConversionInfo]) -> 'Dataflow':
        """
        Converts values in specified columns to the corresponding data types.

        .. remarks::

            The values in the type_conversions dictionary could be of several types:

            * :class:`azureml.dataprep.FieldType`
            * :class:`azureml.dataprep.TypeConverter`
            * Tuple of DATE (:class:`azureml.dataprep.FieldType`) and List of format strings (single format string is also supported)

            .. code-block:: python

                import azureml.dataprep as dprep

                dataflow = dprep.read_csv(path='./some/path')
                dataflow = dataflow.set_column_types({'MyNumericColumn': dprep.FieldType.DECIMAL,
                                                   'MyBoolColumn': dprep.FieldType.BOOLEAN,
                                                   'MyAutodetectDateColumn': dprep.FieldType.DATE,
                                                   'MyDateColumnWithFormat': (dprep.FieldType.DATE, ['%m-%d-%Y']),
                                                   'MyOtherDateColumn': dprep.DateTimeConverter(['%d-%m-%Y'])})

            .. note::

                If you choose to convert a column to DATE and do not provide \
                **format(s)** to use, DataPrep will attempt to infer a format to use by pulling on the data. \
                If a format could be inferred from the data, it will be used to convert values in the corresponding
                column. However, if a format could not be inferred, this step will fail and you will need to either \
                provide the format to use or use the interactive builder \
                :class:`azureml.dataprep.api.builders.ColumnTypesBuilder` by calling \
                'dataflow.builders.set_column_types()'

        :param type_conversions: A dictionary where key is column name and value is either
            :class:`azureml.dataprep.FieldType` or :class:`azureml.dataprep.TypeConverter` or a Tuple of
            DATE (:class:`azureml.dataprep.FieldType`) and List of format strings

        :return: The modified Dataflow.
        """

        def _get_type_arguments(converter: TypeConverter):
            if isinstance(converter, DateTimeConverter):
                return {'dateTimeFormats': converter.formats}
            if isinstance(converter, FloatConverter):
                return {'decimalMark': converter.decimal_mark}
            if isinstance(converter, StreamInfoConverter) and converter.workspace is not None:
                return {
                    'subscription': converter.workspace.subscription_id,
                    'resourceGroup': converter.workspace.resource_group,
                    'workspaceName': converter.workspace.name,
                    'escaped': converter.escaped
                }
            return None

        def _are_all_date_formats_available(conversions: List[Any]) -> bool:
            for conversion in conversions:
                if conversion['typeProperty'] == FieldType.DATE or conversion['typeProperty'] == FieldType.DATE.value:
                    if conversion.get('typeArguments') is None \
                            or conversion['typeArguments']['dateTimeFormats'] is None \
                            or len(conversion['typeArguments']['dateTimeFormats']) == 0:
                        return False

            return True

        # normalize type_conversion argument
        for col in type_conversions.keys():
            conv_info = type_conversions.get(col)
            if not isinstance(conv_info, TypeConverter):
                # if a 2 value tuple and first value is FieldType.Date
                if isinstance(conv_info, tuple) and conv_info[0] == FieldType.DATE and len(conv_info) == 2:
                    type_conversions[col] = DateTimeConverter(conv_info[1] if isinstance(conv_info[1], List)
                                                              else [conv_info[1]])
                elif isinstance(conv_info, tuple) and conv_info[0] == FieldType.DECIMAL and len(conv_info) == 2:
                    type_conversions[col] = FloatConverter(conv_info[1])
                elif isinstance(conv_info, tuple) and conv_info[0] == FieldType.STREAM and len(conv_info) == 2:
                    type_conversions[col] = StreamInfoConverter(conv_info[1])
                elif conv_info in FieldType and conv_info.value < FieldType.UNKNOWN.value:
                    type_conversions[col] = TypeConverter(conv_info)
                else:
                    raise ValueError('Unexpected conversion info for column: ' + col)

        # construct block arguments
        arguments = {'columnConversion': [{
            'column': ColumnsSelector(type=ColumnsSelectorType.SINGLECOLUMN,
                                      details=cast(ColumnsSelectorDetails, SingleColumnSelectorDetails(col))),
            'typeProperty': converter.data_type,
            'typeArguments': _get_type_arguments(converter)
        } for col, converter in type_conversions.items()]}

        need_to_learn = not _are_all_date_formats_available(arguments['columnConversion'])

        if need_to_learn:
            block_args = BlockArguments(
                PropertyValues.from_pod(arguments, _get_prop_descriptions('Microsoft.DPrep.SetColumnTypesBlock')),
                'Microsoft.DPrep.SetColumnTypesBlock')
            blocks = steps_to_block_datas(self._get_steps())
            set_column_types_block = self._engine_api.add_block_to_list(
                AddBlockToListMessageArguments(blocks=blocks,
                                               new_block_arguments=block_args))
            learned_arguments = set_column_types_block.arguments.to_pod()
            success = _are_all_date_formats_available(learned_arguments['columnConversion'])
            if not success:
                raise ValueError('Can\'t detect date_time_formats automatically, please provide desired formats.')

            arguments = learned_arguments

        return self.add_step('Microsoft.DPrep.SetColumnTypesBlock', arguments) if len(arguments['columnConversion']) > 0 else self

    def take_sample(self,
                    probability: float,
                    seed: Optional[int] = None) -> 'Dataflow':
        """
        Takes a random sample of the available records.

        :param probability: The probability of a record being included in the sample.
        :param seed: The seed to use for the random generator.
        :return: The modified Dataflow.
        """
        return self.add_step('Microsoft.DPrep.TakeSampleBlock', {
            'probability': probability,
            'seed': seed or random.randint(0, 4294967295)
        })

    def take_stratified_sample(self,
                               columns: MultiColumnSelection,
                               fractions: Dict[Tuple, int],
                               seed: Optional[int] = None) -> 'Dataflow':
        """
        Takes a random stratified sample of the available records according to input fractions.

        :param columns: The strata columns.
        :param fractions: The strata to strata weights.
        :param seed: The seed to use for the random generator.
        :return: The modified Dataflow.
        """
        _fractions = []
        for stratum, weight in fractions.items():
            stratum_weight = {}
            stratum_as_any = []
            for column_val in stratum:
                _key = self._get_field_type(column_val).name.lower()
                _val = self._ticks(column_val) if isinstance(column_val, datetime.datetime) else column_val
                stratum_as_any.append({'{}'.format(_key): _val})
            stratum_weight['stratum'] = stratum_as_any
            stratum_weight['weight'] = weight
            _fractions.append(stratum_weight)
        return self.add_step('Microsoft.DPrep.TakeStratifiedSampleBlock', {
            'seed': seed or random.randint(0, 4294967295),
            'columns': column_selection_to_selector_value(columns),
            'fractions': _fractions
        })

    def filter(self, expression: Expression) -> 'Dataflow':
        """
        Filters the data, leaving only the records that match the specified expression.

        .. remarks::

            Expressions are started by indexing the Dataflow with the name of a column. They support a variety of
                functions and operators and can be combined using logical operators. The resulting expression will be
                lazily evaluated for each record when a data pull occurs and not where it is defined.

            .. code-block:: python

                dataflow['myColumn'] > dataflow['columnToCompareAgainst']
                dataflow['myColumn'].starts_with('prefix')

        :param expression: The expression to evaluate.
        :return: The modified Dataflow.
        """
        expression = _ensure_expression(expression)
        return self.add_step('Microsoft.DPrep.ExpressionFilterBlock', {'expression': expression.underlying_data})

    def _add_columns_from_record(self,
                                expression: Expression,
                                prior_column: str = None,
                                new_column_prefix: str = None) -> 'Dataflow':
        """
        Add new columns to the dataset based on the record generated by expression

        .. remarks::

           Expressions are built using the expression builders in the expressions module and the functions in
           the functions module. The resulting expression will be lazily evaluated for each record when a data pull
           occurs and not where it is defined.

        :param expression: The expression to evaluate to generate the values in the column.
        :param prior_column: The name of the column after which the new column should be added. The default is to add
            the new column as the last column.
        :param new_column_prefix: string value to be prepend in the name of the new columns added to dataset. This
            might be needed to avoid column name conflicts.
        :return: The modified Dataflow.
        """

        return self.add_step('Microsoft.DPrep.AddColumnsFromRecordBlock', {
            'expression': expression.underlying_data,
            'priorColumn': single_column_to_selector_value(prior_column) if prior_column is not None else None,
            'newColumnPrefix': new_column_prefix
        })

    def _add_columns_from_partition_format(self,
                                column: str,
                                partition_format: str,
                                ignore_error: bool) -> 'Dataflow':
        """
        Add new columns to the dataset based on matching the partition format on 'Path' column.

        :param partition_format: The partition format matching the 'Path' column to create columns.
        :param ignore_error: Indicate whether or not to fail the execution if there is any error.
        :return: The modified Dataflow.
        """

        return self.add_step('Microsoft.DPrep.AddColumnsFromPartitionFormatBlock', {
            'column': single_column_to_selector_value(column),
            'partitionFormat': partition_format,
            'ignoreError': ignore_error
        })

    def add_column(self, expression: Expression, new_column_name: str, prior_column: str) -> 'Dataflow':
        """
        Adds a new column to the dataset. The values in the new column will be the result of invoking the specified
        expression.

        .. remarks::

            Expressions are built using the expression builders in the expressions module and the functions in
            the functions module. The resulting expression will be lazily evaluated for each record when a data pull
            occurs and not where it is defined.

        :param expression: The expression to evaluate to generate the values in the column.
        :param new_column_name: The name of the new column.
        :param prior_column: The name of the column after which the new column should be added. The default is to add
            the new column as the last column.
        :return: The modified Dataflow.
        """
        return self.add_step('Microsoft.DPrep.ExpressionAddColumnBlock', {
            'expression': expression.underlying_data,
            'newColumnName': new_column_name,
            'priorColumn': single_column_to_selector_value(prior_column) if prior_column is not None else None
        })

    ExampleData = TypeVar('ExampleData', Tuple[str, str], Tuple[Dict[str, str], str], List[Tuple[str, str]],
                          List[Tuple[Dict[str, str], str]])
    SourceColumns = TypeVar('SourceColumns', str, List[str])

    def derive_column_by_example(self,
                                 source_columns: SourceColumns,
                                 new_column_name: str,
                                 example_data: ExampleData) -> 'Dataflow':
        """
        Inserts a column by learning a program based on a set of source columns and provided examples.
            Dataprep will attempt to achieve the intended derivation inferred from provided examples.

        .. remarks::

            If you need more control of examples and generated program, create DeriveColumnByExampleBuilder instead.

        :param source_columns: Names of the columns from which the new column will be derived.
        :param new_column_name: Name of the new column to add.
        :param example_data: Examples to use as input for program generation.
            In case there is only one column to be used as source, examples could be Tuples of source value and intended
            target value. For example, you can have "example_data=[("2013-08-22", "Thursday"), ("2013-11-03", "Sunday")]".
            When multiple columns should be considered as source, each example should be a Tuple of dict-like sources
            and intended target value, where sources have column names as keys and column values as values.
        :return: The modified Dataflow.
        """

        if isinstance(source_columns, str):
            # for single source column case create a list for uniform processing
            source_columns = [source_columns]
        else:
            try:
                source_columns = [s for s in source_columns]
            except TypeError:
                source_columns = []

        if len(source_columns) == 0:
            raise ValueError('Please provide columns to derive from')

        if not isinstance(example_data, List):
            # for single example case create a list for uniform processing
            example_data = [example_data]

        source_data_and_examples = []
        for example_data_entry in example_data:
            if not isinstance(example_data_entry[1], str):
                raise ValueError('Incorrect example data. "example_data" should be either '
                                 'Tuple[str|Dict[str, str], str], or List[Tuple[str|Dict[str, str], str]]')
            if isinstance(example_data_entry[0], Dict):
                source_data_and_examples.append(example_data_entry)
            elif len(source_columns) == 1:
                source_data_and_examples.append((
                    {source_columns[0]: str(example_data_entry[0])},
                    example_data_entry[1]))
            else:
                raise ValueError('Incorrect example data. When you derive from multiple columns "example_data" should '
                                 'be either Tuple[Dict[str, str], str] or List[Tuple[Dict[str, str], str]]')

        derive_column_builder = self.builders.derive_column_by_example(source_columns, new_column_name)

        for item in source_data_and_examples:
            derive_column_builder.add_example(source_data=item[0], example_value=item[1])

        return derive_column_builder.to_dataflow()

    def pivot(self,
              columns_to_pivot: List[str],
              value_column: str,
              summary_function: SummaryFunction = None,
              group_by_columns: List[str] = None,
              null_value_replacement: str = None,
              error_value_replacement: str= None) -> 'Dataflow':
        """
        Returns a new Dataflow with columns generated from the values in the selected columns to pivot.

        .. remarks::

            The values of the new dataflow come from the value column selected.
            Additionally there is an optional summarization that consists of an aggregation and a group by.

        :param columns_to_pivot: The columns used to get the values from which the new dataflow's new columns are generated.
        :param value_column: The column used to get the values that will populate the new dataflow's rows.
        :summary_function: The summary function used to aggregate the values.
        :group_by_columns: The columns used to group the new dataflow rows.
        :null_value_replacement: String value to replace null values in columns_to_pivot. If unspecified, the string "NULL" will be used.
        :error_value_replacement: String value to replace error values in columns_to_pivot. If unspecified, the string "ERROR" will be used.
        """
        builder = self.builders.pivot(columns_to_pivot,
                                      value_column,
                                      summary_function,
                                      group_by_columns,
                                      null_value_replacement,
                                      error_value_replacement)
        return builder.to_dataflow()

    def random_split(self,
                     percentage: float,
                     seed: Optional[int] = None) -> ('Dataflow', 'Dataflow'):
        """
        Returns two Dataflows from the original Dataflow, with records randomly and approximately split by the percentage
            specified (using a seed). If the percentage specified is p (where p is between 0 and 1), the first returned dataflow
            will contain approximately p*100% records from the original dataflow, and the second dataflow will contain all the
            remaining records that were not included in the first. A random seed will be used if none is provided.
            Actual split does not happen when this transformation is applied, it only happens when the dataflow is executed.
            Note: even with a seed actual split depends on the platform dataflow is executed on and could differ across platforms and
            sdk versions. 

        :param percentage: The approximate percentage to split the Dataflow by. This must be a number between 0.0 and 1.0.
        :param seed: The seed to use for the random split.
        :return: Two Dataflows containing records randomly split from the original Dataflow. If the percentage specified is p,
            the first dataflow contains approximately p*100% of the records from the original dataflow.

        """
        if percentage < 0.0 or percentage > 1.0:
            raise ValueError("percentage must be a number between 0.0 and 1.0.")

        seed = seed or random.randint(0, 4294967295)
        dflow_1 = self.add_step('Microsoft.DPrep.TakeSampleBlock', {
            'seed': seed,
            'probabilityLowerBound': 0.0,
            'probability': percentage
        })
        dflow_2 = self.add_step('Microsoft.DPrep.TakeSampleBlock', {
            'seed': seed,
            'probabilityLowerBound': percentage,
            'probability': 1.0
        })

        return dflow_1, dflow_2

    def multi_split(self, splits: int, seed: Optional[int] = None):
        """Split a Dataflow into multiple other Dataflows, each containing a random but exclusive sub-set of the data.

        :param splits: The number of splits.
        :param seed: The seed to use for the random split.
        :return: A list containing one Dataflow per split.
        """
        if splits <= 0:
            raise ValueError('At least one split is required.')

        seed = seed or random.randint(0, 4294967295)
        probability = 1 / splits

        def create_split_dataflow(i):
            lower_bound = i * probability
            upper_bound = lower_bound + probability if i != splits - 1 else 1.0
            return self.add_step('Microsoft.DPrep.TakeSampleBlock', {
                'seed': seed,
                'probabilityLowerBound': lower_bound,
                'probability': upper_bound
            })

        dataflows = [create_split_dataflow(i) for i in range(0, splits)]
        return dataflows

    def replace_datasource(self, new_datasource: DataSource) -> 'Dataflow':
        """
        Returns new Dataflow with its DataSource replaced by the given one.

        .. remarks::

            The given 'new_datasource' must match the type of datasource already in the Dataflow.
            For example a MSSQLDataSource cannot be replaced with a FileDataSource.

        :param new_datasource: DataSource to substitute into new Dataflow.
        :return: The modified Dataflow.
        """
        if isinstance(new_datasource, MSSQLDataSource):
            if not self._steps[0].step_type == 'Database':
                raise ValueError("Can't replace '" + self._steps[0].step_type + "' Datasource with MSSQLDataSource.")
            from .readers import read_sql
            new_datasource_step = read_sql(new_datasource, self._steps[0].arguments['query'])._get_steps()[0]
            new_datasource_step.id = self._steps[0].id
        else:
            if self._steps[0].step_type == 'Database':
                raise ValueError("Can only replace 'Database' Datasource with MSSQLDataSource.")
            import copy
            new_datasource_step = copy.deepcopy(self._steps[0])
            new_datasource_step.arguments['path'] = new_datasource.underlying_value
        return Dataflow(self._engine_api, [new_datasource_step] + self._steps[1:])

    def replace_reference(self, new_reference: DataflowReference) -> 'Dataflow':
        """
        Returns new Dataflow with its reference DataSource replaced by the given one.

        :param new_reference: Reference to be substituted for current Reference in Dataflow.
        :return: The modified Dataflow.
        """
        if self._steps[0].step_type != 'Microsoft.DPrep.ReferenceBlock':
            raise ValueError("Can only replace 'Reference' Datasource with 'DataflowReference', found: " +
                             self._steps[0].step_type)
        new_reference_step = Dataflow.reference(new_reference)._get_steps()[0]
        new_reference_step.id = self._steps[0].id
        return Dataflow(self._engine_api, [new_reference_step] + self._steps[1:])

    def cache(self, directory_path: str) -> 'Dataflow':
        """
        Pulls all the records from this Dataflow and cache the result to disk.

        .. remarks::

            This is very useful when data is accessed repeatedly, as future executions will reuse
            the cached result without pulling the same Dataflow again.

        :param directory_path: The directory to save cache files.
        :return: The modified Dataflow.
        """
        df = self._add_cache_step(directory_path)
        df.head(1)
        return df

    def _add_cache_step(self, directory_path: str) -> 'Dataflow':
        return self.add_step('Microsoft.DPrep.CacheBlock', {
            'cachePath': LocalDataSource(directory_path).underlying_value
        })

    def new_script_column(self,
                          new_column_name: str,
                          insert_after: str,
                          script: str) -> 'Dataflow':
        """
        Adds a new column to the Dataflow using the passed in Python script.

        .. remarks::

            The Python script must define a function called newvalue that takes a single argument, typically
                called row. The row argument is a dict (key is column name, value is current value) and will be passed
                to this function for each row in the dataset. This function must return a value to be used in the new column.

            .. note::

                Any libraries that the Python script imports must exist in the environment where the dataflow is run.

            .. code-block:: python

                import numpy as np
                def newvalue(row):
                    return np.log(row['Metric'])

        :param new_column_name: The name of the new column being created.
        :param insert_after: The column after which the new column will be inserted.
        :param script: The script that will be used to create this new column.
        :return: The modified Dataflow.
        """
        return self.add_step(step_type='Microsoft.DPrep.AddCustomColumnBlock', arguments={
            "codeBlockType": CodeBlockType.MODULE,
            "columnId": new_column_name,
            "customExpression": script,
            "priorColumnId": ColumnsSelector(
                                type=ColumnsSelectorType.SINGLECOLUMN,
                                details=cast(ColumnsSelectorDetails, SingleColumnSelectorDetails(insert_after)))
          })

    def new_script_filter(self, script: str) -> 'Dataflow':
        """
        Filters the Dataflow using the passed in Python script.

        .. remarks::

            The Python script must define a function called includerow that takes a single argument, typically
                called row. The row argument is a dict (key is column name, value is current value) and will be passed
                to this function for each row in the dataset. This function must return True or False depending on whether
                the row should be included in the dataflow. Any libraries that the Python script imports must exist in the
                environment where the dataflow is run.

            .. code-block:: python

                def includerow(row):
                    return row['Metric'] > 100

        :param script: The script that will be used to filter the dataflow.
        :return: The modified Dataflow.
        """
        return self.add_step(step_type='Microsoft.DPrep.FilterBlock', arguments={
            "codeBlockType": CodeBlockType.MODULE,
            "filterExpression": script
          })

    def map_partition(self,
                      fn: Callable[[Union['pandas.DataFrame', 'scipy.sparse.csr_matrix'], int], Union['pandas.DataFrame', 'scipy.sparse.csr_matrix']],
                      data_format: str = 'lastProcessed') -> 'Dataflow':
        """
        Applies a transformation to each partition in the Dataflow.

        .. remarks::
            The function passed in must take in two parameters: a `Link pandas.DataFrame <https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html>`_ or
                `Link scipy.sparse.csr_matrix <https://docs.scipy.org/doc/scipy/reference/generated/scipy.sparse.csr_matrix.html>`_ containing the data for the partition and an index.
                The return value must be a `Link pandas.DataFrame <https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html>`_ or a
                `Link scipy.sparse.csr_matrix <https://docs.scipy.org/doc/scipy/reference/generated/scipy.sparse.csr_matrix.html>`_ containing the transformed data.
                By default the 'lastProcessed' format is passed into `fn`, i.e. if the data coming into map_partitions is Sparse it will be sent as a
                `Link scipy.sparse.csr_matrix <https://docs.scipy.org/doc/scipy/reference/generated/scipy.sparse.csr_matrix.html>`_, if it is dense it will
                be sent as a `Link pandas.DataFrame <https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html>`_. The desired
                input data format can be set explicitly using the `data_format` parameter.

            .. note::

                `df` does not usually contain all of the data in the dataflow, but a partition of the data as it is being processed in the \
                runtime. The number and contents of each partition is not guaranteed across runs.

            The transform function can fully edit the passed in dataframe or even create a new one, but must return a
                dataframe. Any libraries that the Python script imports must exist in the environment where the dataflow
                is run.

        :param fn: A callable that will be invoked to transform each partition.
        :param dataFormat: A optional string specifying the input format to fn. Supported Formats: 'dataframe', 'csr', 'lastProcessed'.
        :return: The modified Dataflow.
        """
        import cloudpickle
        import os
        import tempfile

        pickled_data = cloudpickle.dumps(fn)
        file, path = tempfile.mkstemp()
        with os.fdopen(file, 'wb') as f:
            f.write(pickled_data)

        return self.add_step(step_type='Microsoft.DPrep.MapPartitionsBlock', arguments={
            "dataFormat": data_format,
            "pickledFile": path
        })

    def transform_partition(self, script: 'str') -> 'Dataflow':
        """
        Applies a transformation to each partition in the Dataflow.

        * This function has been deprecated and will be removed in a future version. Please use `map_partition` instead.

        .. remarks::

            The Python script must define a function called transform that takes two arguments, typically called `df` and
                `index`. The `df` argument will be a Pandas Dataframe passed to this function that contains the data for the
                partition and the `index` argument is a unique identifier of the partition.

            .. note::

                `df` does not usually contain all of the data in the dataflow, but a partition of the data as it is being processed in the \
                runtime. The number and contents of each partition is not guaranteed across runs.

            The transform function can fully edit the passed in dataframe or even create a new one, but must return a
                dataframe. Any libraries that the Python script imports must exist in the environment where the dataflow
                is run.

            .. code-block:: python

                # the script passed in should have this function defined
                def transform(df, index):
                    # perform any partition level transforms here and return resulting `df`
                    return df

        :param script: A script that will be used to transform each partition.
        :return: The modified Dataflow.
        """
        import warnings
        warnings.warn('transform_partition has been deprecated. Please use map_partition instead', DeprecationWarning)

        return self.add_step(step_type='Microsoft.DPrep.MapPartitionsAsDataFrameBlock', arguments={
            "codeBlockType": CodeBlockType.MODULE,
            "MapPartitionsAsDataFrameBlock": script
        })

    def transform_partition_with_file(self,
                                      script_path: str) -> 'Dataflow':
        """
        Transforms an entire partition using the Python script in the passed in file.

        .. remarks::

            The Python script must define a function called transform that takes two arguments, typically called `df` and
                `index`. The first argument `df` will be a Pandas Dataframe that contains the data for the partition and the
                second argument `index` will be a unique identifier for the partition.

            .. note::

                `df` does not usually contain all of the data in the dataflow, but a partition of the data as it is being processed in the runtime.\
                The number and contents of each partition is not guaranteed across runs.

            The transform function can fully edit the passed in dataframe or even create a new one, but must return a
                dataframe. Any libraries that the Python script imports must exist in the environment where the dataflow is run.

            .. code-block:: python

                # the script file passed in should have this function defined
                def transform(df, index):
                    # perform any partition level transforms here and return resulting `df`
                    return df

        :param script_path: Relative path to script that will be used to transform the partition.
        :return: The modified Dataflow.
        """
        return self.add_step(step_type='Microsoft.DPrep.MapPartitionsAsDataFrameBlock', arguments={
            "codeBlockType": CodeBlockType.FILE,
            "MapPartitionsAsDataFrameBlock": script_path
        })

    def split_column_by_delimiters(self,
                                   source_column: str,
                                   delimiters: Delimiters,
                                   keep_delimiters: False) -> 'Dataflow':
        """
        Splits the provided column and adds the resulting columns to the dataflow.

        .. remarks::

            This will pull small sample of the data, determine number of columns it should expect as a result of the
                split and generate a split program that would ensure that the expected number of columns will be produced,
                so that there is a deterministic schema after this operation.

        :param source_column: Column to split.
        :param delimiters: String or list of strings to be deemed as column delimiters.
        :param keep_delimiters: Controls whether to keep or drop column with delimiters.
        :return: The modified Dataflow.
        """
        builder = self.builders.split_column_by_example(source_column=source_column,
                                                        delimiters=delimiters,
                                                        keep_delimiters=keep_delimiters)
        return builder.to_dataflow()

    def split_column_by_example(self, source_column: str, example: SplitExample = None) -> 'Dataflow':
        """
        Splits the provided column and adds the resulting columns to the dataflow based on the provided example.

        .. remarks::

            This will pull small sample of the data, determine the best program to satisfy provided example
                and generate a split program that would ensure that the expected number of columns will be produced, so that
                there is a deterministic schema after this operation.

            .. note::

                If example is not provided, this will generate a split program based on common split patterns, like splitting by space, punctuation, date parts and etc.

        :param source_column: Column to split.
        :param example: Example to use for program generation.
        :return: The modified Dataflow.
        """
        builder = self.builders.split_column_by_example(source_column=source_column)
        if example is not None:
            builder.add_example(example)
        return builder.to_dataflow()

    def replace(self,
                columns: MultiColumnSelection,
                find: Any,
                replace_with: Any) -> 'Dataflow':
        """
        Replaces values in a column that match the specified search value.

        .. remarks::

            The following types are supported for both the find or replace arguments: str, int, float,
                datetime.datetime, and bool.

        :param columns: The source columns.
        :param find: The value to find, or None.
        :param replace_with: The replacement value, or None.
        :return: The modified Dataflow.
        """
        replace_dict = self._make_replace_dict(find, replace_with)

        if replace_dict['valueToFindType'] == FieldType.UNKNOWN:
            raise ValueError('The type of the find argument is not supported')
        if replace_dict['replaceWithType'] == FieldType.UNKNOWN:
            raise ValueError('The type of the replace_with argument is not supported')

        return self._add_replace_step(columns, replace_dict)

    def error(self,
              columns: MultiColumnSelection,
              find: Any,
              error_code: str) -> 'Dataflow':
        """
        Creates errors in a column for values that match the specified search value.

        .. remarks::

            The following types are supported for the find argument: str, int, float,
                datetime.datetime, and bool.

        :param columns: The source columns.
        :param find: The value to find, or None.
        :param error_code: The error code to use in new errors, or None.
        :return: The modified Dataflow.
        """
        replace_dict = self._make_replace_dict(find, None)

        if replace_dict['valueToFindType'] == FieldType.UNKNOWN:
            raise ValueError('The type of the find argument is not supported')
        replace_dict['replaceWithType'] = FieldType.ERROR

        return self._add_replace_step(columns, replace_dict, error_code)

    def fill_nulls(self,
                   columns: MultiColumnSelection,
                   fill_with: Any) -> 'Dataflow':
        """
        Fills all nulls in a column with the specified value.

        .. remarks::

            The following types are supported for the fill_with argument: str, int, float,
                datetime.datetime, and bool.

        :param columns: The source columns.
        :param fill_with: The value to fill nulls with.
        :return: The modified Dataflow.
        """
        replace_dict = self._make_replace_dict(None, fill_with)

        if replace_dict['replaceWithType'] == FieldType.UNKNOWN or replace_dict['replaceWithType'] == FieldType.NULL:
            raise ValueError('The type of the fill_with argument is not supported')

        return self.add_step('Microsoft.DPrep.FillNullsBlock', {
            'columns': column_selection_to_selector_value(columns),
            'replaceWithType': replace_dict['replaceWithType'],
            'stringReplaceWith': replace_dict['stringReplaceWith'],
            'doubleReplaceWith': replace_dict['doubleReplaceWith'],
            'datetimeReplaceWith': replace_dict['datetimeReplaceWith'],
            'booleanReplaceWith': replace_dict['booleanReplaceWith']
        })

    def fill_errors(self,
                    columns: MultiColumnSelection,
                    fill_with: Any) -> 'Dataflow':
        """
        Fills all errors in a column with the specified value.

        .. remarks::

            The following types are supported for the fill_with argument: str, int, float,
                datetime.datetime, and bool.

        :param columns: The source columns.
        :param fill_with: The value to fill errors with, or None.
        :return: The modified Dataflow.
        """
        replace_dict = self._make_replace_dict(None, fill_with)

        replace_dict['valueToFindType'] = FieldType.ERROR
        if replace_dict['replaceWithType'] == FieldType.UNKNOWN:
            raise ValueError('The type of the fill_with argument is not supported')

        return self._add_replace_step(columns, replace_dict)

    def join(self,
             right_dataflow: DataflowReference,
             join_key_pairs: List[Tuple[str, str]] = None,
             join_type: JoinType = JoinType.MATCH,
             left_column_prefix: str = 'l_',
             right_column_prefix: str = 'r_',
             left_non_prefixed_columns: List[str] = None,
             right_non_prefixed_columns: List[str] = None) -> 'Dataflow':
        """
        Creates a new Dataflow that is a result of joining this Dataflow with the provided right_dataflow.

        :param right_dataflow: Right Dataflow or DataflowReference to join with.
        :param join_key_pairs: Key column pairs. List of tuples of columns names where each tuple forms a key pair to
            join on. For instance: [('column_from_left_dataflow', 'column_from_right_dataflow')]
        :param join_type: Type of join to perform. Match is default.
        :param left_column_prefix: Prefix to use in result Dataflow for columns originating from left_dataflow.
            Needed to avoid column name conflicts at runtime.
        :param right_column_prefix: Prefix to use in result Dataflow for columns originating from right_dataflow.
            Needed to avoid column name conflicts at runtime.
        :param left_non_prefixed_columns: List of column names from left_dataflow that should not be prefixed with
            left_column_prefix. Every other column appearing in the data at runtime will be prefixed.
        :param right_non_prefixed_columns: List of column names from right_dataflow that should not be prefixed with
            left_column_prefix. Every other column appearing in the data at runtime will be prefixed.
        :return: The new Dataflow.
        """

        return Dataflow.join(self,
                             right_dataflow,
                             join_key_pairs,
                             join_type,
                             left_column_prefix,
                             right_column_prefix,
                             left_non_prefixed_columns,
                             right_non_prefixed_columns)

    def write_to_csv(self,
                     file_path: Optional[DataDestination] = None,
                     directory_path: Optional[DataDestination] = None,
                     single_file: bool = False,
                     separator: str = ',',
                     na: str = 'NA',
                     error: str = 'ERROR',
                     if_destination_exists: str = IfDestinationExists.MERGEWITHOVERWRITE) -> 'Dataflow':
        """
        Write out the data in the Dataflow in a delimited text format. The output is specified as a directory
            which will contain multiple files, one per partition processed in the Dataflow.

        :param directory_path: The path to a directory in which to store the output files.
        :param separator: The separator to use.
        :param na: String to use for null values.
        :param error: String to use for error values.
        :param if_destination_exists: Behavior if destination exists.
        :return: The modified Dataflow. Every execution of the returned Dataflow will perform the write again.
        """
        if directory_path and isinstance(directory_path, str):
            directory_path = FileOutput.file_output_from_str(directory_path)
        if file_path and isinstance(file_path, str):
            file_path = FileOutput.file_output_from_str(file_path)

        if directory_path is None and file_path is None:
            raise ValueError("Please provide either `directory_path` or `file_path` to specify where to write the files.")

        if isinstance(directory_path, FileOutput) or isinstance(file_path, FileOutput):
            return self.add_step('Microsoft.DPrep.WriteToCsvBlock', {
                'filePath': file_path.underlying_value if file_path is not None else None,
                'directoryPath': directory_path.underlying_value if directory_path is not None else None,
                'separator': separator,
                'singleFile': single_file,
                'na': na,
                'error': error,
                'ifDestinationExists': if_destination_exists
            })

        return self.add_step('Microsoft.DPrep.WriteCsvToDatastoreBlock', {
            'datastore': get_datastore_value(directory_path)[1]._to_pod(),
            'separator': separator,
            'singleFile': False,
            'na': na,
            'error': error,
            'ifDestinationExists': if_destination_exists
        })

    def write_to_parquet(self,
                         file_path: Optional[DataDestination] = None,
                         directory_path: Optional[DataDestination] = None,
                         single_file: bool = False,
                         error: str = 'ERROR',
                         row_groups: int = 0,
                         if_destination_exists: str = IfDestinationExists.MERGEWITHOVERWRITE,
                         partition_keys: List[str] = None ) -> 'Dataflow':
        """
        Writes out the data in the Dataflow into Parquet files.

        :param file_path: The path in which to store the output file.
        :param directory_path: The path in which to store the output files.
        :param single_file: Whether to store the entire Dataflow in a single file.
        :param error: String to use for error values.
        :param row_groups: Number of rows to use per row group.
        :param if_destination_exists: Behavior if destination exists.
        :param partition_keys: Optional, list of column names used to write the data by.
            Defaults to be None.
        :return: The modified Dataflow.
        """
        if directory_path and isinstance(directory_path, str):
            directory_path = FileOutput.file_output_from_str(directory_path)
        if file_path and isinstance(file_path, str):
            file_path = FileOutput.file_output_from_str(file_path)

        if directory_path is None and file_path is None:
            raise ValueError("Please provide either `directory_path` or `file_path` to specify where to write the files.")

        if partition_keys is not None:
            for key in partition_keys:
                if not key:
                    raise ValueError("Please provide valid partition keys")

        if partition_keys is not None and isinstance(directory_path, FileOutput):
            return self.add_step(
                'Microsoft.DPrep.WriteToParquetByBucketsBlock', {
                    'directoryPath': directory_path.underlying_value if directory_path is not None else None,
                    'error': error,
                    'rowGroups': row_groups,
                    'ifDestinationExists': if_destination_exists,
                    'bucketKeys': partition_keys
                }
            )

        if isinstance(directory_path, FileOutput) or isinstance(file_path, FileOutput):
            return self.add_step('Microsoft.DPrep.WriteToParquetBlock', {
                'filePath': file_path.underlying_value if file_path is not None else None,
                'directoryPath': directory_path.underlying_value if directory_path is not None else None,
                'singleFile': single_file,
                'error': error,
                'rowGroups': row_groups,
                'ifDestinationExists': if_destination_exists
            })

        if partition_keys is not None:
            return self.add_step(
                'Microsoft.DPrep.WriteParquetToDatastoreByBucketsBlock', {
                    'datastore': get_datastore_value(directory_path if directory_path else file_path)[1]._to_pod(),
                    'error': error,
                    'rowGroups': row_groups,
                    'ifDestinationExists': if_destination_exists,
                    'bucketKeys': partition_keys
                }
            )

        return self.add_step(
            'Microsoft.DPrep.WriteParquetToDatastoreBlock', {
                'datastore': get_datastore_value(directory_path if directory_path else file_path)[1]._to_pod(),
                'singleFile': single_file,
                'error': error,
                'rowGroups': row_groups,
                'ifDestinationExists': if_destination_exists
            }
        )

    def write_to_preppy(self, directory_path: DataDestination,
                        if_destination_exists: str = IfDestinationExists.MERGEWITHOVERWRITE) -> 'Dataflow':
        """
        Writes out the data in the Dataflow into Preppy files, a DataPrep serialization format.

        :param directory_path: The path in which to store the output Preppy files.
        :param if_destination_exists: Behavior if destination exists.
        :return: The modified Dataflow.
        """
        if directory_path and isinstance(directory_path, str):
            directory_path = FileOutput.file_output_from_str(directory_path)

        if isinstance(directory_path, FileOutput):
            return self.add_step('Microsoft.DPrep.WritePreppyBlock', {
                'outputPath': directory_path.underlying_value if directory_path is not None else None,
                'profilingFields': ['Kinds', 'MissingAndEmpty'],
                'ifDestinationExists': if_destination_exists
            })

        return self.add_step(
            'Microsoft.DPrep.WritePreppyToDatastoreBlock', {
                'datastore': get_datastore_value(directory_path)[1]._to_pod(),
                'profilingFields': ['Kinds', 'MissingAndEmpty'],
                'ifDestinationExists': if_destination_exists
            }
        )

    SortColumns = TypeVar('SortColumns', str, List[str])

    def sort_asc(self, columns: SortColumns) -> 'Dataflow':
        """
        Sorts the dataset in ascending order by the specified columns.

        :param columns: The columns to sort in ascending order.
        :return: The modified Dataflow.
        """
        columns = [columns] if not isinstance(columns, List) else columns
        return self.add_step('Microsoft.DPrep.SortBlock', {
            'sortOrder': [{'column': single_column_to_selector_value(c), 'descending': False} for c in columns]
        })

    def sort_desc(self, columns: SortColumns) -> 'Dataflow':
        """
        Sorts the dataset in descending order by the specified columns.

        :param columns: The columns to sort in descending order.
        :return: The modified Dataflow.
        """
        columns = [columns] if not isinstance(columns, List) else columns
        return self.add_step('Microsoft.DPrep.SortBlock', {
            'sortOrder': [{'column': single_column_to_selector_value(c), 'descending': True} for c in columns]
        })

    def to_datetime(self,
                    columns: MultiColumnSelection,
                    date_time_formats: Optional[List[str]] = None,
                    date_constant: Optional[str] = None) -> 'Dataflow':
        """
        Converts the values in the specified columns to DateTimes.

        :param columns: The source columns.
        :param date_time_formats: The formats to use to parse the values. If none are provided, a partial scan of the
            data will be performed to derive one.
        :param date_constant: If the column contains only time values, a date to apply to the resulting DateTime.
        :return: The modified Dataflow.
        """
        arguments = {
            'columns': column_selection_to_selector_value(columns),
            'dateTimeFormats': [p for p in date_time_formats] if date_time_formats is not None else None,
            'dateConstant': date_constant
        }

        if date_time_formats is None or len(date_time_formats) == 0:
            blocks = steps_to_block_datas(self._get_steps())
            block_args = BlockArguments(
                PropertyValues.from_pod(arguments, _get_prop_descriptions('Microsoft.DPrep.ToDateTimeBlock')),
                'Microsoft.DPrep.ToDateTimeBlock')
            to_datetime_block = self._engine_api.add_block_to_list(
                AddBlockToListMessageArguments(blocks=blocks, new_block_arguments=block_args))
            learned_arguments = to_datetime_block.arguments.to_pod()
            if learned_arguments.get('dateTimeFormats') is None or len(learned_arguments['dateTimeFormats']) == 0:
                raise ValueError('Can\'t detect date_time_formats automatically, please provide desired formats.')

            arguments['dateTimeFormats'] = learned_arguments['dateTimeFormats']

        return self.add_step('Microsoft.DPrep.ToDateTimeBlock', arguments)

    def summarize(self,
                  summary_columns: Optional[List[SummaryColumnsValue]] = None,
                  group_by_columns: Optional[List[str]] = None,
                  join_back: bool = False,
                  join_back_columns_prefix: Optional[str] = None) -> 'Dataflow':
        """
        Summarizes data by running aggregate functions over specific columns.

        .. remarks::

            The aggregate functions are independent and it is possible to aggregate the same column multiple times.
                Unique names have to be provided for the resulting columns. The aggregations can be grouped, in which case
                one record is returned per group; or ungrouped, in which case one record is returned for the whole dataset.
                Additionally, the results of the aggregations can either replace the current dataset or augment it by
                appending the result columns.

            .. code-block:: python

                import azureml.dataprep as dprep

                dataflow = dprep.read_csv(path='./some/path')
                dataflow = dataflow.summarize(
                    summary_columns=[
                        dprep.SummaryColumnsValue(
                            column_id='Column To Summarize',
                            summary_column_name='New Column of Counts',
                            summary_function=dprep.SummaryFunction.COUNT)],
                    group_by_columns=['Column 1', 'Column 2'])

        :param summary_columns: List of SummaryColumnsValue, where each value defines the column to summarize,
            the summary function to use and the name of resulting column to add.
        :param group_by_columns: Columns to group by.
        :param join_back: Whether to append subtotals or replace current data with them.
        :param join_back_columns_prefix: Prefix to use for subtotal columns when appending them to current data.
        :return: The modified Dataflow.
        """
        # handle string as a single column
        group_by_columns = [] if group_by_columns is None else\
            [group_by_columns] if isinstance(group_by_columns, str) else group_by_columns
        if not summary_columns and len(group_by_columns) == 0:
            raise ValueError("Missing required argument. Please provide at least one of the following arguments: "
                             "'summary_columns', 'group_by_columns'.")
        return self._summarize(summary_columns, group_by_columns, join_back, join_back_columns_prefix)

    def assert_value(self,
                     columns: MultiColumnSelection,
                     expression: Expression,
                     policy: AssertPolicy = AssertPolicy.ERRORVALUE,
                     error_code: str = 'AssertionFailed') -> 'Dataflow':
        """
        Ensures that values in the specified columns satisfy the provided expression. This is useful to identify anomalies in the dataset
            and avoid broken pipelines by handling assertion errors in a clean way.

        :param columns: Columns to apply evaluation to.
        :param expression: Expression that has to be evaluated to be True for the value to be kept.
        :param policy: Action to take when expression is evaluated to False. Options are `FAILEXECUTION` and `ERRORVALUE`.
            `FAILEXECUTION` ensures that any data that violates the assertion expression during execution will immediately fail the job. This is useful to save computing resources and time.
            `ERRORVALUE` captures any data that violates the assertion expression by replacing it with error_code. This allows you to handle these error values by either filtering or replacing them.
        :param error_code: Error message to use to replace values failing the assertion or failing an execution.
        :return: The modified Dataflow.
        """
        return self.add_step('Microsoft.DPrep.ExpressionAssertValueBlock', {
            'columns': column_selection_to_selector_value(columns),
            'expression': expression.underlying_data,
            'assertPolicy': policy,
            'errorCode': error_code
        })

    def _assert_schema(self,
                       columns: MultiColumnSelection,
                       schema_assertion_policy: ComparePolicy = ComparePolicy.CONTAIN,
                       policy: AssertPolicy = AssertPolicy.FAILEXECUTION,
                       error_code: str = 'AssertionFailed') -> 'Dataflow':
        """
        Compare schema based on the schema_assertion_policy. Currently we only support ComparePolicy.CONTAIN.

        :param columns: Columns to apply evaluation to.
        :param schema_assertion_policy: Evaluation to apply for the columns argument and record schema.
        :param policy: Action to take when assertion is evaluated to False. Options are `FAILEXECUTION` and `ERRORVALUE`.
            `FAILEXECUTION` ensures that any data that violates the assertion expression during execution will immediately fail the job. This is useful to save computing resources and time.
            `ERRORVALUE` captures any data that violates the assertion expression by replacing it with error_code. This allows you to handle these error values by either filtering or replacing them.
        :param error_code: Error message to use to replace values failing the assertion or failing an execution.
        :return: The modified Dataflow.
        """
        return self.add_step('Microsoft.DPrep.AssertSchemaBlock', {
            'staticColumns': column_selection_to_selector_value(columns),
            'comparePolicy': schema_assertion_policy,
            'assertPolicy': policy,
            'errorCode': error_code
        })

    def get_missing_secrets(self) -> List[str]:
        """
        Get a list of missing secret IDs.

        :return: A list of missing secret IDs.
        """
        secrets = self._engine_api.get_secrets(GetSecretsMessageArguments(steps_to_block_datas(self._steps)))
        missing_secret_ids = map(
            lambda secret: secret.key,
            filter(lambda secret: not secret.is_available, secrets)
        )

        return list(missing_secret_ids)

    def use_secrets(self, secrets: Dict[str, str]):
        """
        Uses the passed in secrets for execution.

        :param secrets: A dictionary of secret ID to secret value. You can get the list of required secrets by calling
            the get_missing_secrets method on Dataflow.
        """
        self._engine_api.add_temporary_secrets(secrets)

    def _get_source_data_hash(self) -> (str, datetime):
        """
        Get the hash of the source data in the dataflow
        """
        result = self._engine_api.get_source_data_hash(GetSourceDataHashMessageArguments(
            blocks=steps_to_block_datas(self._steps)
        ))
        return (result.data_hash, value_from_field(result.data_changed_time))

    @staticmethod
    def get_files(path: FilePath) -> 'Dataflow':
        """
        Expands the path specified by reading globs and files in folders and outputs one record per file found.

        :param path: The path or paths to expand.
        :return: A new Dataflow.
        """
        return Dataflow._path_to_get_files_block(path)

    @staticmethod
    def open(file_path: str) -> 'Dataflow':
        """
        Opens a Dataflow with specified name from the package file.

        :param file_path: Path to the package containing the Dataflow.
        :return: The Dataflow.
        """
        activity_data = get_engine_api().get_activity(file_path)
        return Dataflow._dataflow_from_activity_data(activity_data, get_engine_api())

    @staticmethod
    def from_json(dataflow_json: str) -> 'Dataflow':
        """
        Load Dataflow from 'package_json'.

        :param dataflow_json: JSON string representation of the Package.
        :return: New Package object constructed from the JSON string.
        """
        engine_api = get_engine_api()
        activity_data = engine_api.load_activity_from_json(dataflow_json)
        return Dataflow._dataflow_from_activity_data(activity_data, get_engine_api())

    def save(self, file_path: str):
        """
        Saves the Dataflow to the specified file

        :param file_path: The path of the file.
        """
        activity_data = Dataflow._dataflow_to_anonymous_activity_data(self)
        self._engine_api.save_activity_from_data(SaveActivityFromDataMessageArguments(activity_data, file_path))

    def to_json(self) -> str:
        """
        Get the JSON string representation of the Dataflow.
        """
        return self._engine_api.save_activity_to_json(Dataflow._dataflow_to_anonymous_activity_data(self))

    @staticmethod
    def _dataflow_from_activity_data(activity_data: AnonymousActivityData, engine_api: EngineAPI) -> 'Dataflow':
        steps = [Dataflow._step_from_block_data(bd) for bd in activity_data.blocks]
        df = Dataflow(engine_api, steps, activity_data.meta)
        df._activity_data = activity_data
        return df

    @staticmethod
    def _dataflow_to_anonymous_activity_data(dataflow: 'Dataflow') -> AnonymousActivityData:
        return AnonymousActivityData(
            [AnonymousBlockData(step.arguments,
                                step.id,
                                step.local_data,
                                step.step_type) for step in dataflow._get_steps()],
            dataflow._meta
            )

    @staticmethod
    def _step_from_block_data(block_data: AnonymousBlockData):
        step = Step(block_data.type, block_data.arguments.to_pod(), block_data.local_data.to_pod())
        step.id = block_data.id
        step._block_data = block_data
        return step

    @staticmethod
    def _path_to_get_files_block(path: FilePath, archive_options: ArchiveOptions = None) -> 'Dataflow':
        try:
            if _is_datapath(path) or _is_datapaths(path):
                return datastore_to_dataflow(path)
        except ImportError:
            pass

        datasource = path if isinstance(path, FileDataSource) else FileDataSource.datasource_from_str(path)
        return Dataflow._get_files(datasource, archive_options)

    @staticmethod
    def _path_to_create_dataset_block(path: FilePath) -> 'Dataflow':
        try:
            if _is_datapath(path) or _is_datapaths(path):
                return datastore_to_dataflow(path, is_file=True)
        except ImportError:
            pass

        datasource = path if isinstance(path, FileDataSource) else FileDataSource.datasource_from_str(path)
        return Dataflow._create_dataset_files(datasource)

    @staticmethod
    def _get_files(path: FileDataSource, archive_options: ArchiveOptions = None) -> 'Dataflow':
        """
        Expands the path specified by reading globs and files in folders and outputs one record per file found.

        :param path: The path or paths to expand.
        :return: A new Dataflow.
        """
        df = Dataflow(get_engine_api())
        args = {
            'path': path.underlying_value
        }
        if archive_options is not None:
            args['isArchive'] = True
            args['archiveOptions'] = {
                'archiveType': archive_options.archive_type
            }
            if archive_options.entry_glob is not None:
                args['archiveOptions']['entryGlob'] = archive_options.entry_glob

        return df.add_step('Microsoft.DPrep.GetFilesBlock', args)

    @staticmethod
    def _create_dataset_files(path: FileDataSource) -> 'Dataflow':
        df = Dataflow(get_engine_api())
        args = {
            'path': path.underlying_value
        }
        return df.add_step('Microsoft.DPrep.CreateDatasetFilesBlock', args)

    @staticmethod
    def _datetime_for_message(dt: datetime):
        t = {'timestamp': int(dt.timestamp() * 1000)}
        return t

    @staticmethod
    def _ticks(dt: datetime):
        return int((dt - datetime.datetime(1, 1, 1)).total_seconds() * 10000000)

    @staticmethod
    def _get_field_type(data):
        if data is None:
            return ValueKind.NULL
        elif isinstance(data, str):
            return ValueKind.STRING
        elif isinstance(data, int):
            return ValueKind.LONG
        elif isinstance(data, float):
            return ValueKind.DOUBLE
        elif isinstance(data, datetime.datetime):
            return ValueKind.DATETIME
        elif isinstance(data, bool):
            return ValueKind.BOOLEAN

    def _set_values_to_find(self, replace_dict: Dict[str, Any], find: Any):
        if find is None:
            replace_dict['valueToFindType'] = FieldType.NULL
        elif isinstance(find, str):
            replace_dict['valueToFindType'] = FieldType.STRING
            replace_dict['stringValueToFind'] = find
        elif isinstance(find, int) or isinstance(find, float):
            replace_dict['valueToFindType'] = FieldType.DECIMAL
            replace_dict['doubleValueToFind'] = find
        elif isinstance(find, datetime.datetime):
            replace_dict['valueToFindType'] = FieldType.DATE
            replace_dict['datetimeValueToFind'] = self._datetime_for_message(find)
        elif isinstance(find, bool):
            replace_dict['valueToFindType'] = FieldType.BOOLEAN
            replace_dict['booleanValueToFind'] = find

    def _make_replace_dict(self, find: Any, replace_with: Any):
        replace_dict = {
            'valueToFindType': FieldType.UNKNOWN,
            'stringValueToFind': None,
            'doubleValueToFind': None,
            'datetimeValueToFind': None,
            'booleanValueToFind': None,
            'replaceWithType': FieldType.UNKNOWN,
            'stringReplaceWith': None,
            'doubleReplaceWith': None,
            'datetimeReplaceWith': None,
            'booleanReplaceWith': None
        }

        self._set_values_to_find(replace_dict, find)

        if replace_with is None:
            replace_dict['replaceWithType'] = FieldType.NULL
        elif isinstance(replace_with, str):
            replace_dict['replaceWithType'] = FieldType.STRING
            replace_dict['stringReplaceWith'] = replace_with
        elif isinstance(replace_with, int) or isinstance(replace_with, float):
            replace_dict['replaceWithType'] = FieldType.DECIMAL
            replace_dict['doubleReplaceWith'] = replace_with
        elif isinstance(replace_with, datetime.datetime):
            replace_dict['replaceWithType'] = FieldType.DATE
            replace_dict['datetimeReplaceWith'] = self._datetime_for_message(replace_with)
        elif isinstance(replace_with, bool):
            replace_dict['replaceWithType'] = FieldType.BOOLEAN
            replace_dict['booleanReplaceWith'] = replace_with

        return replace_dict

    def _add_replace_step(self, columns: MultiColumnSelection, replace_dict: Dict, error_replace_with: str = None):
        error_replace_with = str(error_replace_with) if error_replace_with is not None else None
        return self.add_step('Microsoft.DPrep.ReplaceBlock', {
                                 'columns': column_selection_to_selector_value(columns),
                                 'valueToFindType': replace_dict['valueToFindType'],
                                 'stringValueToFind': replace_dict['stringValueToFind'],
                                 'doubleValueToFind': replace_dict['doubleValueToFind'],
                                 'datetimeValueToFind': replace_dict['datetimeValueToFind'],
                                 'booleanValueToFind': replace_dict['booleanValueToFind'],
                                 'replaceWithType': replace_dict['replaceWithType'],
                                 'stringReplaceWith': replace_dict['stringReplaceWith'],
                                 'doubleReplaceWith': replace_dict['doubleReplaceWith'],
                                 'datetimeReplaceWith': replace_dict['datetimeReplaceWith'],
                                 'booleanReplaceWith': replace_dict['booleanReplaceWith'],
                                 'errorReplaceWith': error_replace_with
                             })

    def _partition_to_pandas_dataframe(self,
                                       i: int,
                                       extended_types: bool,
                                       nulls_as_nan: bool,
                                       on_error: str,
                                       out_of_range_datetime: str) -> 'pandas.DataFrame':
        return self.select_partitions([i]).to_pandas_dataframe(extended_types=extended_types,
                                                                nulls_as_nan=nulls_as_nan,
                                                                on_error=on_error,
                                                                out_of_range_datetime=out_of_range_datetime)

    def _raise_if_missing_secrets(self, secrets: Dict[str, str]=None):
        missing_secrets = set(self.get_missing_secrets())
        if len(missing_secrets) == 0:
            return

        new_secret_ids = set(secrets.keys()) if secrets else set()
        missing_secrets = missing_secrets.difference(new_secret_ids)

        if len(missing_secrets) == 0:
            return

        class MissingSecretsError(DataPrepException):
            def __init__(self, missing_secret_ids):
                generic_message = 'Required secrets are missing. Please call `use_secrets()` to register the missing secrets.\n'
                message = generic_message + 'Missing secrets:\n{}'.format('\n'.join(missing_secret_ids))

                super().__init__(message, 'MissingSecrets', generic_message)

                self.missing_secret_ids = missing_secret_ids

        raise MissingSecretsError(missing_secrets)

    # Steps are immutable so we don't need to create a full deepcopy of them when cloning Dataflows.
    def __deepcopy__(self, memodict=None):
        return copy(self)

    def __getitem__(self, key):
        if isinstance(key, slice):
            return Dataflow(self._engine_api, self._steps[key])
        elif isinstance(key, int):
            return Dataflow(self._engine_api, [self._steps[key]])
        elif isinstance(key, str):
            return col(key)
        elif isinstance(key, Iterable):
            return col(list(key))
        else:
            raise TypeError("Invalid argument type.")

    def __getstate__(self):
        dflow_bytes = self._engine_api.save_activity_to_package(Dataflow._dataflow_to_anonymous_activity_data(self))
        return {'dataflow_bytes': dflow_bytes}

    def __setstate__(self, newstate):
        engine_api = get_engine_api()
        dflow = Dataflow._dataflow_from_activity_data(engine_api.load_activity_from_package(newstate['dataflow_bytes']),
                                                      engine_api)
        self.__dict__ = dflow.__dict__

    # Will fold the right Dataflow into the left by appending the rights steps to the lefts.
    def __add__(self, other):
        if not isinstance(other, Dataflow):
            raise TypeError("Can only add two Dataflow objects together. Was given: " + str(type(other)))
        return Dataflow(self._engine_api, self._steps + other._steps)

    def __repr__(self):
        result = dedent("""\
        Dataflow
          steps: [\n""".format(**vars(self)))
        result += ''.join(
            indent(str(step), '  ' * 2) + ',\n'
            for step in self._steps)
        result += '  ]'
        return result

    @staticmethod
    def join(left_dataflow: DataflowReference,
             right_dataflow: DataflowReference,
             join_key_pairs: List[Tuple[str, str]] = None,
             join_type: JoinType = JoinType.MATCH,
             left_column_prefix: str = 'l_',
             right_column_prefix: str = 'r_',
             left_non_prefixed_columns: List[str] = None,
             right_non_prefixed_columns: List[str] = None) -> 'Dataflow':
        """
        Creates a new Dataflow that is a result of joining two provided Dataflows.

        :param left_dataflow: Left Dataflow or DataflowReference to join with.
        :param right_dataflow: Right Dataflow or DataflowReference to join with.
        :param join_key_pairs: Key column pairs. List of tuples of columns names where each tuple forms a key pair to
            join on. For instance: [('column_from_left_dataflow', 'column_from_right_dataflow')]
        :param join_type: Type of join to perform. Match is default.
        :param left_column_prefix: Prefix to use in result Dataflow for columns originating from left_dataflow.
            Needed to avoid column name conflicts at runtime.
        :param right_column_prefix: Prefix to use in result Dataflow for columns originating from right_dataflow.
            Needed to avoid column name conflicts at runtime.
        :param left_non_prefixed_columns: List of column names from left_dataflow that should not be prefixed with
            left_column_prefix. Every other column appearing in the data at runtime will be prefixed.
        :param right_non_prefixed_columns: List of column names from right_dataflow that should not be prefixed with
            left_column_prefix. Every other column appearing in the data at runtime will be prefixed.
        :return: The new Dataflow.
        """

        df = Dataflow(get_engine_api())
        return df.add_step('TwoWayJoin', {
            'leftActivityReference': left_dataflow if isinstance(left_dataflow, ActivityReference)
            else make_activity_reference(left_dataflow),
            'rightActivityReference': right_dataflow if isinstance(right_dataflow, ActivityReference)
            else make_activity_reference(right_dataflow),
            'joinKeyPairs': [{'leftKeyColumn': pair[0], 'rightKeyColumn': pair[1]} for pair in join_key_pairs],
            'joinType': join_type,
            'leftColumnPrefix': left_column_prefix,
            'rightColumnPrefix': right_column_prefix,
            'leftNonPrefixedColumns': left_non_prefixed_columns,
            'rightNonPrefixedColumns': right_non_prefixed_columns
        })

    def keep_columns(self,
                     columns: MultiColumnSelection,
                     validate_column_exists: bool = False) -> 'Dataflow':
        """
        Keeps the specified columns and drops all others.

        :param columns: The source columns.
        :param validate_column_exists: Whether to validate the columns selected.
        :return: The modified Dataflow.
        """
        df = self.add_step('Microsoft.DPrep.KeepColumnsBlock', {
                               'columns': column_selection_to_selector_value(columns)
                           })

        if validate_column_exists:
            df._verify_has_data_using_profile()

        return df

# >>> BEGIN GENERATED METHODS
    @staticmethod
    def reference(reference: 'DataflowReference') -> 'Dataflow':
        """
        Creates a reference to an existing activity object.

        :param reference: The reference activity.
        :return: A new Dataflow.
        """
        df = Dataflow(get_engine_api())
        return df.add_step('Microsoft.DPrep.ReferenceBlock', {
                               'reference': make_activity_reference(reference)
                           })

    @staticmethod
    def read_parquet_dataset(path: FileDataSource) -> 'Dataflow':
        """
        Creates a step to read parquet file.

        :param path: The path to the Parquet file.
        :return: A new Dataflow.
        """
        df = Dataflow(get_engine_api())
        return df.add_step('Microsoft.DPrep.ReadParquetDatasetBlock', {
                               'path': path.underlying_value
                           })

    def select_partitions(self,
                          partition_indices: List[int]) -> 'Dataflow':
        """
        Selects specific partitions from the data, dropping the rest.

        :return: The modified Dataflow.
        """
        return self.add_step('Microsoft.DPrep.SelectPartitionsBlock', {
                                 'partitionIndices': [p for p in partition_indices]
                             })

    def to_csv_streams(self,
                       separator: str = ',',
                       na: str = 'NA',
                       error: str = 'ERROR') -> 'Dataflow':
        """
        Creates streams with the data in delimited format.

        :return: The modified Dataflow.
        """
        return self.add_step('Microsoft.DPrep.ToCsvStreamsBlock', {
                                 'separator': separator,
                                 'na': na,
                                 'error': error
                             })

    def to_parquet_streams(self,
                           error: str = 'ERROR',
                           rows_per_group: int = 5000) -> 'Dataflow':
        """
        Creates streams with the data in parquet format.

        :param error: String to use for error values.
        :param rows_per_group: Number of rows to use per row group.
        :return: The modified Dataflow.
        """
        return self.add_step('Microsoft.DPrep.ToParquetStreamsBlock', {
                                 'error': error,
                                 'rowsPerGroup': rows_per_group
                             })

    def to_data_frame_directory(self,
                                error: str = 'ERROR',
                                rows_per_group: int = 5000) -> 'Dataflow':
        """
        Creates streams with the data in dataframe directory format.

        :param error: String to use for error values.
        :param rows_per_group: Number of rows to use per row group.
        :return: The modified Dataflow.
        """
        return self.add_step('Microsoft.DPrep.ToDataFrameDirectoryBlock', {
                                 'error': error,
                                 'rowsPerGroup': rows_per_group
                             })

    def null_coalesce(self,
                      columns: List[str],
                      new_column_id: str) -> 'Dataflow':
        """
        For each record, selects the first non-null value from the columns specified and uses it as the value of a new column.

        :param columns: The source columns.
        :param new_column_id: The name of the new column.
        :return: The modified Dataflow.
        """
        return self.add_step('Microsoft.DPrep.NullCoalesceBlock', {
                                 'columns': [single_column_to_selector_value(p) for p in columns],
                                 'newColumnId': new_column_id
                             })

    def extract_error_details(self,
                              column: str,
                              error_value_column: str,
                              extract_error_code: bool = False,
                              error_code_column: Optional[str] = None) -> 'Dataflow':
        """
        Extracts the error details from error values into a new column.

        :param column: The source column.
        :param error_value_column: Name of a column to hold the original value of the error.
        :param extract_error_code: Whether to also extract the error code.
        :param error_code_column: Name of a column to hold the error code.
        :return: The modified Dataflow.
        """
        return self.add_step('Microsoft.DPrep.ExtractErrorDetailsBlock', {
                                 'column': single_column_to_selector_value(column),
                                 'errorValueColumn': error_value_column,
                                 'extractErrorCode': extract_error_code,
                                 'errorCodeColumn': error_code_column
                             })

    def duplicate_column(self,
                         column_pairs: Dict[str, str]) -> 'Dataflow':
        """
        Creates new columns that are duplicates of the specified source columns.

        :param column_pairs: Mapping of the columns to duplicate to their new names.
        :return: The modified Dataflow.
        """
        return self.add_step('Microsoft.DPrep.DuplicateColumnBlock', {
                                 'columnPairs': [{'column': single_column_to_selector_value(k), 'newColumnId': v} for k, v in column_pairs.items()]
                             })

    def split_stype(self,
                    column: str,
                    stype: SType,
                    stype_fields: Optional[List[str]] = None,
                    new_column_names: Optional[List[str]] = None) -> 'Dataflow':
        """
        Creates new columns from an existing column, interpreting its values as a semantic type.

        :param column: The source column.
        :param stype: The semantic type used to interpret values in the column.
        :param stype_fields: Fields of the semantic type to use. If not provided, all fields will be used.
        :param new_column_names: Names of the new columns. If not provided new columns will be named with the source column name plus the semantic type field name.
        :return: The modified Dataflow.
        """
        return self.add_step('Microsoft.DPrep.SplitSTypeBlock', {
                                 'column': single_column_to_selector_value(column),
                                 'stype': stype.value,
                                 'stypeFields': [p for p in stype_fields] if stype_fields is not None else None,
                                 'newColumnNames': [p for p in new_column_names] if new_column_names is not None else None
                             })

    def replace_na(self,
                   columns: MultiColumnSelection,
                   use_default_na_list: bool = True,
                   use_empty_string_as_na: bool = True,
                   use_nan_as_na: bool = True,
                   custom_na_list: Optional[str] = None) -> 'Dataflow':
        """
        Replaces values in the specified columns with nulls. You can choose to use the default list, supply your own, or both.

        :param use_default_na_list: Use the default list and replace 'null', 'NaN', 'NA', and 'N/A' with null.
        :param use_empty_string_as_na: Replace empty strings with null.
        :param use_nan_as_na: Replace NaNs with Null.
        :param custom_na_list: Provide a comma separated list of values to replace with null.
        :param columns: The source columns.
        :return: The modified Dataflow.
        """
        return self.add_step('Microsoft.DPrep.ReplaceNaBlock', {
                                 'columns': column_selection_to_selector_value(columns),
                                 'useDefaultNaList': use_default_na_list,
                                 'useEmptyStringAsNa': use_empty_string_as_na,
                                 'useNanAsNa': use_nan_as_na,
                                 'customNaList': custom_na_list
                             })

    def trim_string(self,
                    columns: MultiColumnSelection,
                    trim_left: bool = True,
                    trim_right: bool = True,
                    trim_type: TrimType = TrimType.WHITESPACE,
                    custom_characters: str = '') -> 'Dataflow':
        """
        Trims string values in specific columns.

        :param columns: The source columns.
        :param trim_left: Whether to trim from the beginning.
        :param trim_right: Whether to trim from the end.
        :param trim_type: Whether to trim whitespace or custom characters.
        :param custom_characters: The characters to trim.
        :return: The modified Dataflow.
        """
        return self.add_step('Microsoft.DPrep.TrimStringBlock', {
                                 'columns': column_selection_to_selector_value(columns),
                                 'trimLeft': trim_left,
                                 'trimRight': trim_right,
                                 'trimType': trim_type.value,
                                 'customCharacters': custom_characters
                             })

    def round(self,
              column: str,
              decimal_places: int) -> 'Dataflow':
        """
        Rounds the values in the column specified to the desired number of decimal places.

        :param column: The source column.
        :param decimal_places: The number of decimal places.
        :return: The modified Dataflow.
        """
        return self.add_step('Microsoft.DPrep.RoundBlock', {
                                 'column': single_column_to_selector_value(column),
                                 'decimalPlaces': decimal_places
                             })

    def clip(self,
             columns: MultiColumnSelection,
             lower: Optional[float] = None,
             upper: Optional[float] = None,
             use_values: bool = True) -> 'Dataflow':
        """
        Clips values so that all values are between the lower and upper boundaries.

        :param columns: The source columns.
        :param lower: All values lower than this value will be set to this value.
        :param upper: All values higher than this value will be set to this value.
        :param use_values: If true, values outside boundaries will be set to the boundary values. If false, those values will be set to null.
        :return: The modified Dataflow.
        """
        return self.add_step('Microsoft.DPrep.ClipBlock', {
                                 'columns': column_selection_to_selector_value(columns),
                                 'lower': lower,
                                 'upper': upper,
                                 'useValues': use_values
                             })

    def str_replace(self,
                    columns: MultiColumnSelection,
                    value_to_find: Optional[str] = None,
                    replace_with: Optional[str] = None,
                    match_entire_cell_contents: bool = False,
                    use_special_characters: bool = False) -> 'Dataflow':
        """
        Replaces values in a string column that match a search string with the specified value.

        :param columns: The source columns.
        :param value_to_find: The value to find.
        :param replace_with: The replacement value.
        :param match_entire_cell_contents: Whether the value to find must match the entire value.
        :param use_special_characters: If checked, you can use '#(tab)', '#(cr)', or '#(lf)' to represent special characters in the find or replace arguments.
        :return: The modified Dataflow.
        """
        return self.add_step('Microsoft.DPrep.StrReplaceBlock', {
                                 'columns': column_selection_to_selector_value(columns),
                                 'valueToFind': value_to_find,
                                 'replaceWith': replace_with,
                                 'matchEntireCellContents': match_entire_cell_contents,
                                 'useSpecialCharacters': use_special_characters
                             })

    def distinct_rows(self) -> 'Dataflow':
        """
        Filters out records that contain duplicate values in all columns, leaving only a single instance.
        :return: The modified Dataflow.
        """
        return self.add_step('Microsoft.DPrep.DistinctRowsBlock', {
                             })

    def drop_nulls(self,
                   columns: MultiColumnSelection,
                   column_relationship: typedefinitions.ColumnRelationship = typedefinitions.ColumnRelationship.ALL) -> 'Dataflow':
        """
        Drops rows where all or any of the selected columns are null.

        :param columns: The source columns.
        :param column_relationship: Whether all or any of the selected columns must be null.
        :return: The modified Dataflow.
        """
        return self.add_step('Microsoft.DPrep.DropNullsBlock', {
                                 'columns': column_selection_to_selector_value(columns),
                                 'columnRelationship': column_relationship.value
                             })

    def drop_errors(self,
                    columns: MultiColumnSelection,
                    column_relationship: typedefinitions.ColumnRelationship = typedefinitions.ColumnRelationship.ALL) -> 'Dataflow':
        """
        Drops rows where all or any of the selected columns are an Error.

        :param columns: The source columns.
        :param column_relationship: Whether all or any of the selected columns must be an Error.
        :return: The modified Dataflow.
        """
        return self.add_step('Microsoft.DPrep.DropErrorsBlock', {
                                 'columns': column_selection_to_selector_value(columns),
                                 'columnRelationship': column_relationship.value
                             })

    def distinct(self,
                 columns: MultiColumnSelection) -> 'Dataflow':
        """
        Filters out records that contain duplicate values in the specified columns, leaving only a single instance.

        :param columns: The source columns.
        :return: The modified Dataflow.
        """
        return self.add_step('Microsoft.DPrep.DistinctBlock', {
                                 'columns': column_selection_to_selector_value(columns)
                             })

    def skip(self,
             count: int) -> 'Dataflow':
        """
        Skips the specified number of records.

        :param count: The number of records to skip.
        :return: The modified Dataflow.
        """
        return self.add_step('Microsoft.DPrep.SkipBlock', {
                                 'count': count
                             })

    def take(self,
             count: int) -> 'Dataflow':
        """
        Takes the specified count of records.

        :param count: The number of records to take.
        :return: The modified Dataflow.
        """
        return self.add_step('Microsoft.DPrep.TakeBlock', {
                                 'count': count
                             })

    def rename_columns(self,
                       column_pairs: Dict[str, str]) -> 'Dataflow':
        """
        Renames the specified columns.

        :param column_pairs: The columns to rename and the desired new names.
        :return: The modified Dataflow.
        """
        return self.add_step('Microsoft.DPrep.RenameColumnsBlock', {
                                 'columnPairs': [{'column': single_column_to_selector_value(k), 'newColumnId': v} for k, v in column_pairs.items()]
                             })

    def drop_columns(self,
                     columns: MultiColumnSelection) -> 'Dataflow':
        """
        Drops the specified columns.

        :param columns: The source columns.
        :return: The modified Dataflow.
        """
        return self.add_step('Microsoft.DPrep.DropColumnsBlock', {
                                 'columns': column_selection_to_selector_value(columns)
                             })

    def promote_headers(self) -> 'Dataflow':
        """
        Sets the first record in the dataset as headers, replacing any existing ones.
        :return: The modified Dataflow.
        """
        return self.add_step('Microsoft.DPrep.PromoteHeadersBlock', {
                             })

    def to_number(self,
                  columns: MultiColumnSelection,
                  decimal_point: DecimalMark = DecimalMark.DOT) -> 'Dataflow':
        """
        Converts the values in the specified columns to floating point numbers.

        :param columns: The source columns.
        :param decimal_point: The symbol to use as the decimal mark.
        :return: The modified Dataflow.
        """
        return self.add_step('Microsoft.DPrep.ToNumberBlock', {
                                 'columns': column_selection_to_selector_value(columns),
                                 'decimalPoint': decimal_point.value
                             })

    def to_bool(self,
                columns: MultiColumnSelection,
                true_values: List[str],
                false_values: List[str],
                mismatch_as: MismatchAsOption = MismatchAsOption.ASERROR) -> 'Dataflow':
        """
        Converts the values in the specified columns to booleans.

        :param columns: The source columns.
        :param true_values: The values to treat as true.
        :param false_values: The values to treat as false.
        :param mismatch_as: How to treat values that don't match the values in the true or false values lists.
        :return: The modified Dataflow.
        """
        return self.add_step('Microsoft.DPrep.ToBoolBlock', {
                                 'columns': column_selection_to_selector_value(columns),
                                 'trueValues': [p for p in true_values],
                                 'falseValues': [p for p in false_values],
                                 'mismatchAs': mismatch_as.value
                             })

    def to_string(self,
                  columns: MultiColumnSelection) -> 'Dataflow':
        """
        Converts the values in the specified columns to strings.

        :param columns: The source columns.
        :return: The modified Dataflow.
        """
        return self.add_step('Microsoft.DPrep.ToStringBlock', {
                                 'columns': column_selection_to_selector_value(columns)
                             })

    def to_long(self,
                columns: MultiColumnSelection) -> 'Dataflow':
        """
        Converts the values in the specified columns to 64 bit integers.

        :param columns: The source columns.
        :return: The modified Dataflow.
        """
        return self.add_step('Microsoft.DPrep.ToLongBlock', {
                                 'columns': column_selection_to_selector_value(columns)
                             })

    def convert_unix_timestamp_to_datetime(self,
                                           columns: MultiColumnSelection,
                                           use_seconds: bool = False) -> 'Dataflow':
        """
        Converts the specified column to DateTime values by treating the existing value as a Unix timestamp.

        :param columns: The source columns.
        :param use_seconds: Whether to use seconds as the resolution. Milliseconds are used if false.
        :return: The modified Dataflow.
        """
        return self.add_step('Microsoft.DPrep.ConvertUnixTimestampToDateTime', {
                                 'columns': column_selection_to_selector_value(columns),
                                 'useSeconds': use_seconds
                             })

    def _summarize(self,
                   summary_columns: Optional[List[_SummaryColumnsValue]] = None,
                   group_by_columns: Optional[List[str]] = None,
                   join_back: bool = False,
                   join_back_columns_prefix: Optional[str] = None) -> 'Dataflow':
        """
        Summarizes data by running aggregate functions over specific columns. The aggregate functions are independent and it is possible to aggregate
            the same column multiple times. Unique names have to be provided for the resulting columns. The aggregations can be grouped, in which
            case one record is returned per group; or ungrouped, in which case one record is returned for the whole dataset. Additionally, the
            results of the aggregations can either replace the current dataset or augment it by appending the result columns.

        :param summary_columns: Column summarization definition.
        :param group_by_columns: Columns to group by.
        :param join_back: Whether to append subtotals or replace current data with them.
        :param join_back_columns_prefix: Prefix to use for subtotal columns when appending them to current data.
        :return: The modified Dataflow.
        """
        return self.add_step('Microsoft.DPrep.SummarizeBlock', {
                                 'summaryColumns': [p._to_pod() for p in summary_columns] if summary_columns is not None else None,
                                 'groupByColumns': [p for p in group_by_columns] if group_by_columns is not None else None,
                                 'joinBack': join_back,
                                 'joinBackColumnsPrefix': join_back_columns_prefix
                             })

    def _summarize_each(self,
                        summary_functions: Optional[List[_SummaryFunctionsValue]] = None,
                        group_by_columns: Optional[List[str]] = None) -> 'Dataflow':
        return self.add_step('Microsoft.DPrep.SummarizeEachBlock', {
                                 'summaryFunctions': [p._to_pod() for p in summary_functions] if summary_functions is not None else None,
                                 'groupByColumns': [p for p in group_by_columns] if group_by_columns is not None else None
                             })

    def append_columns(self,
                       dataflows: List['DataflowReference'],
                       parallelize: bool = True) -> 'Dataflow':
        """
        Appends the columns from the referenced dataflows to the current one. Duplicate columns will result in failure.

        :param dataflows: The dataflows to append.
        :param parallelize: Whether to parallelize the operation. If true, the data for all inputs will be loaded into memory.
        :return: The modified Dataflow.
        """
        return self.add_step('Microsoft.DPrep.AppendColumnsBlock', {
                                 'dataflows': [make_activity_reference(p) for p in dataflows],
                                 'parallelize': parallelize
                             })

    def append_rows(self,
                    dataflows: List['DataflowReference']) -> 'Dataflow':
        """
        Appends the records in the specified dataflows to the current one. If the schemas of the dataflows are distinct, this will result in records
            with different schemas.

        :param dataflows: The dataflows to append.
        :return: The modified Dataflow.
        """
        return self.add_step('Microsoft.DPrep.AppendRowsBlock', {
                                 'dataflows': [make_activity_reference(p) for p in dataflows]
                             })

    def sort(self,
             sort_order: List[Tuple[str, bool]]) -> 'Dataflow':
        """
        Sorts the dataset by the specified columns.

        :param sort_order: The columns to sort by and whether to sort ascending or descending. True is treated as descending, false as ascending.
        :return: The modified Dataflow.
        """
        return self.add_step('Microsoft.DPrep.SortBlock', {
                                 'sortOrder': [{'column': single_column_to_selector_value(t[0]), 'descending': t[1]} for t in sort_order]
                             })

    def zip_partitions(self,
                       dataflows: List['DataflowReference']) -> 'Dataflow':
        """
        Appends the columns from the referenced dataflows to the current one. This is different from AppendColumns in that it assumes all dataflows
            being appended have the same number of partitions and same number of Records within each corresponding partition. If these two
            conditions are not true the operation will fail.

        :param dataflows: The dataflows to append.
        :return: The modified Dataflow.
        """
        return self.add_step('Microsoft.DPrep.ZipPartitionsBlock', {
                                 'dataflows': [make_activity_reference(p) for p in dataflows]
                             })

    def write_streams(self,
                      streams_column: str,
                      base_path: FileOutput,
                      file_names_column: Optional[str] = None,
                      prefix_path: Optional[str] = None) -> 'Dataflow':
        """
        Writes the streams in the specified column to the destination path. By default, the name of the files written will be the resource identifier
            of the streams. This behavior can be overriden by specifying a column which contains the names to use.

        :param streams_column: The column containing the streams to write.
        :param file_names_column: A column containing the file names to use.
        :param base_path: The path under which the files should be written.
        :param prefix_path: The prefix path that needs to be removed from the target paths.
        :return: The modified Dataflow.
        """
        return self.add_step('Microsoft.DPrep.WriteStreamsBlock', {
                                 'streamsColumn': single_column_to_selector_value(streams_column),
                                 'basePath': base_path.underlying_value,
                                 'fileNamesColumn': single_column_to_selector_value(file_names_column) if file_names_column is not None else None,
                                 'prefixPath': prefix_path
                             })

    def parse_json_column(self,
                          column: str) -> 'Dataflow':
        """
        Parses the values in the specified column as JSON objects and expands them into multiple columns.

        :param column: The source column.
        :return: The modified Dataflow.
        """
        args = {
            'column': single_column_to_selector_value(column)
        }
        blocks = steps_to_block_datas(self._get_steps())
        new_block = self._engine_api.add_block_to_list(
            AddBlockToListMessageArguments(blocks=blocks,
                                           new_block_arguments=BlockArguments(PropertyValues.from_pod(args, _get_prop_descriptions('Microsoft.DPrep.ParseJsonColumnBlock')), 'Microsoft.DPrep.ParseJsonColumnBlock')))
        return self.add_step('Microsoft.DPrep.ParseJsonColumnBlock', new_block.arguments.to_pod(), new_block.local_data.to_pod())
# <<< END GENERATED METHODS
