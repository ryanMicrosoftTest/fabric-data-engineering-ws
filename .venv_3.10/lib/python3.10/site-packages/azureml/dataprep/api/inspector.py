# Copyright (c) Microsoft Corporation. All rights reserved.
""" Contains data preparation inspectors that can be used in Azure Machine Learning.

An inspector collects specific statistics on specified columns of a :class:`azureml.dataprep.Dataflow`, which
can be used to understand the input data. Create an inspector and execute it with the
Dataflow class ``execute_inspector`` method. You can execute multiple Inspector objects with the
``execute_inspectors`` method.
"""

from .engineapi.api import EngineAPI
from .engineapi.typedefinitions import (
    ActivityReference,
    ExecuteInspectorCommonArguments,
    ExecuteInspectorsMessageArguments,
    InspectorArguments,
    ExecuteInspectorCommonResponse,
    RowsData,
    ColumnDefinition,
    FieldType,
    DataField)
from ._dataframereader import _execute
from typing import List, Union
from uuid import uuid4
import json

from .value import to_dprep_value_and_type

_MAX_ROW_COUNT = 2**31 - 1


class BaseInspector:
    """Represents the base inspector.
    """

    def __init__(self):
        pass

    def _to_inspector_arguments(self):
        pass


class TableInspector(BaseInspector):
    """Defines an inspector that returns summary statistics on all the data represented by a Dataflow.
    """

    def __init__(
            self,
            includeSTypeCounts: bool,
            includeAverageSpacesCount: bool,
            includeStringLengths: bool,
            numberOfHistogramBins: int = 10):
        super().__init__()
        self.includeSTypeCounts = includeSTypeCounts
        self.includeAverageSpacesCount = includeAverageSpacesCount
        self.includeStringLengths = includeStringLengths
        self.numberOfHistogramBins = numberOfHistogramBins

    def _to_inspector_arguments(self):
        return InspectorArguments(
            inspector_type='Microsoft.DPrep.TableInspector',
            arguments={
                "includeSTypeCounts": self.includeSTypeCounts,
                "includeAverageSpacesCount": self.includeAverageSpacesCount,
                "includeStringLengths": self.includeStringLengths,
                "numberOfHistogramBins": self.numberOfHistogramBins if self.numberOfHistogramBins is not None else None})


value_kind_to_field_type = {
    0: FieldType.NULL,
    1: FieldType.BOOLEAN,
    2: FieldType.INTEGER,
    3: FieldType.DECIMAL,
    4: FieldType.STRING,
    5: FieldType.DATE,
    6: FieldType.STRING,
    7: FieldType.LIST,
    8: FieldType.DATAROW,
    9: FieldType.ERROR,
    10: FieldType.STREAM
}


def to_data_field(value):
    (value, field_type) = to_dprep_value_and_type(value)
    return DataField(field_type, value)


def get_column_type(batch, column):
    value_kinds = batch['valueKinds'][0][column]
    # 0 is Null and 9 is Error
    filtered_kinds = [value_kind_to_field_type[k[0]]
                      for k in value_kinds if k[0] != 0 and k[0] != 9]
    if len(filtered_kinds) == 0:
        return DataField(FieldType.INTEGER, FieldType.NULL.value)
    elif len(filtered_kinds) == 1:
        return DataField(FieldType.INTEGER, filtered_kinds[0])
    else:
        return DataField(FieldType.INTEGER, FieldType.STRING.value)


def get_simple_aggregate(batch, column, aggregate):
    aggregate_values_by_column = batch.get(aggregate)
    if aggregate_values_by_column is None:
        return DataField(FieldType.NULL)
    else:
        value_for_column = aggregate_values_by_column[0][column]
        return to_data_field(value_for_column)


def get_statistical_moment_aggregate(batch, column, aggregate):
    aggregate_values_by_column = batch.get('statisticalMoments')
    if aggregate_values_by_column is None:
        return DataField(FieldType.NULL)
    else:
        value_for_column = aggregate_values_by_column[0][column][aggregate]
        return to_data_field(value_for_column)


def get_quantile(batch, column, idx):
    aggregate_values_by_column = batch.get('quantiles')
    if aggregate_values_by_column is None:
        return DataField(FieldType.NULL)
    else:
        value_for_column = aggregate_values_by_column[0][column][idx]
        return to_data_field(value_for_column)


def get_total_count(batch):
    kinds = next(iter(batch['valueKinds'][0].values()))
    if kinds is None:
        return 0

    return sum((k[1] for k in kinds))


def get_data_quality_aggregate(batch, column, aggregate, total_count):
    missing_and_empty = batch['missingAndEmpty'][0][column]
    if aggregate == 0:  # not missing
        value = total_count - missing_and_empty[0]
    elif aggregate == 1:  # missing
        value = missing_and_empty[0]
    elif aggregate == 2:  # errors
        try:
            value = next(
                (k[1] for k in batch['valueKinds'][0][column] if k[0] == 9))
        except StopIteration:
            value = 0
    elif aggregate == 3:  # empty
        value = missing_and_empty[1]
    elif aggregate == 4:  # % missing
        if total_count > 0:
            value = float(missing_and_empty[0]) / float(total_count)
        else:
            value = 0.0
    else:
        raise RuntimeError('Unexpected missing and empty aggregate type.')

    return to_data_field(value)


def get_value_count_aggregate(batch, column, agg):
    value_counts = batch['valueCountsLimited'][0][column]
    if value_counts is None:
        return DataField(FieldType.NULL)

    if agg == 0:  # unique values
        return DataField(FieldType.INTEGER, len(value_counts))
    elif agg == 1:  # value counts
        values = [{
            'value': to_data_field(value_count['value']),
            'count': value_count['count']
        } for value_count in value_counts]
        return DataField(FieldType.LIST, values)
    else:
        raise RuntimeError('Unexpected value count aggregate.')


def get_whiskers_value(batch, column, top):
    whiskers = batch.get('whiskers')
    if whiskers is None:
        return DataField(FieldType.NULL)

    value = whiskers[0][column][0 if top else 1]
    return to_data_field(value)


def get_type_count(batch, column):
    value_kinds = batch['valueKinds'][0][column]
    return DataField(FieldType.LIST, [
        {
            'type': value_kind_to_field_type[kind_and_count[0]],
            'count': kind_and_count[1]
        } for kind_and_count in value_kinds])


def get_histogram(batch, column):
    histogram_values = batch.get('histogram')
    if histogram_values is None:
        return DataField(FieldType.NULL)

    histogram_values = histogram_values[0][column]
    return DataField(
        FieldType.LIST, [
            to_data_field(v) for v in histogram_values])


# This list is mirrored in TableInspectorDefinition.cs and must be kept in
# sync.
profile_columns = {
    'Column': lambda batch, column, _: DataField(FieldType.STRING, column),
    'type': lambda batch, column, _: get_column_type(batch, column),
    'min': lambda batch, column, _: get_simple_aggregate(batch, column, 'min'),
    'max': lambda batch, column, _: get_simple_aggregate(batch, column, 'max'),
    'mean': lambda batch, column, _: get_statistical_moment_aggregate(batch, column, 'Mean'),
    'standard_deviation': lambda batch, column, _: get_statistical_moment_aggregate(batch, column, 'StandardDeviation'),
    'variance': lambda batch, column, _: get_statistical_moment_aggregate(batch, column, 'Variance'),
    'skewness': lambda batch, column, _: get_statistical_moment_aggregate(batch, column, 'Skewness'),
    'kurtosis': lambda batch, column, _: get_statistical_moment_aggregate(batch, column, 'Kurtosis'),
    '0.1%': lambda batch, column, _: get_quantile(batch, column, 1),
    '1%': lambda batch, column, _: get_quantile(batch, column, 2),
    '5%': lambda batch, column, _: get_quantile(batch, column, 3),
    '25%': lambda batch, column, _: get_quantile(batch, column, 4),
    '50%': lambda batch, column, _: get_quantile(batch, column, 5),
    '75%': lambda batch, column, _: get_quantile(batch, column, 6),
    '95%': lambda batch, column, _: get_quantile(batch, column, 7),
    '99%': lambda batch, column, _: get_quantile(batch, column, 8),
    '99.9%': lambda batch, column, _: get_quantile(batch, column, 9),
    'num_not_missing': lambda batch, column, total_count: get_data_quality_aggregate(batch, column, 0, total_count),
    'num_missing': lambda batch, column, total_count: get_data_quality_aggregate(batch, column, 1, total_count),
    'num_errors': lambda batch, column, total_count: get_data_quality_aggregate(batch, column, 2, total_count),
    'num_empty': lambda batch, column, total_count: get_data_quality_aggregate(batch, column, 3, total_count),
    'count': lambda batch, column, total_count: DataField(FieldType.INTEGER, total_count),
    '%missing': lambda batch, column, total_count: get_data_quality_aggregate(batch, column, 4, total_count),
    'unique_values': lambda batch, column, _: get_value_count_aggregate(batch, column, 0),
    'value_count': lambda batch, column, _: get_value_count_aggregate(batch, column, 1),
    'histogram': lambda batch, column, _: get_histogram(batch, column),
    'type_count': lambda batch, column, _: get_type_count(batch, column),
    'whisker_top': lambda batch, column, _: get_whiskers_value(batch, column, True),
    'whisker_bottom': lambda batch, column, _: get_whiskers_value(batch, column, False),
    'stype_count': lambda batch, column, _: DataField(FieldType.NULL),
    'average_spaces_count': lambda batch, column, _: DataField(FieldType.NULL),
    'string_lengths': lambda batch, column, _: DataField(FieldType.NULL),
}


def get_inspector_response_from_batches(batches):
    if len(batches) == 0:
        return ExecuteInspectorCommonResponse(
            column_definitions=[], rows_data=RowsData([]))
    elif len(batches) > 1:
        raise RuntimeError('Expected a single batch from profile execution.')

    aggregates = batches[0].to_pydict()
    total_count = get_total_count(aggregates)
    if len(aggregates) == 0:
        return ExecuteInspectorCommonResponse(
            column_definitions=[], rows_data=RowsData([]))

    columns_aggregated = list(next(iter(aggregates.values()))[0].keys())

    def get_column_aggregates_row(column):
        return [get_fn(aggregates, column, total_count)
                for _, get_fn in profile_columns.items()]

    inspector_column_definitions = [ColumnDefinition(
        c, FieldType.STRING) for c in profile_columns.keys()]
    inspector_rows = [get_column_aggregates_row(c) for c in columns_aggregated]
    return ExecuteInspectorCommonResponse(
        column_definitions=inspector_column_definitions,
        rows_data=RowsData(inspector_rows))


class _Inspector:
    @classmethod
    def _from_execution(cls,
                        engine_api: EngineAPI,
                        context: ActivityReference,
                        inspector: Union[str, BaseInspector],
                        span_context: 'DPrepSpanContext' = None):

        if isinstance(inspector, BaseInspector):
            inspector_arguments = inspector._to_inspector_arguments()
        elif isinstance(inspector, str):
            inspector_arguments = json.loads(inspector)
        else:
            inspector_arguments = inspector

        try:
            script = engine_api.get_inspector_lariat(
                ExecuteInspectorCommonArguments(
                    context=context,
                    inspector_arguments=inspector_arguments,
                    offset=0,
                    row_count=_MAX_ROW_COUNT))
            (record_batches,
             _,
             _) = _execute('_Inspector._from_execution',
                           script,
                           collect_results=True,
                           allow_fallback_to_clex=False,
                           fail_on_out_of_range_datetime=True,
                           traceparent=span_context.span_id if span_context is not None else '',
                           span_context=span_context)
            return get_inspector_response_from_batches(record_batches)
        except Exception as e:
            return engine_api.execute_inspector(
                ExecuteInspectorCommonArguments(
                    context=context,
                    inspector_arguments=inspector_arguments,
                    offset=0,
                    row_count=_MAX_ROW_COUNT))


class _InspectorBatch:
    # inspector_id corresponds to a GUID that is used to match the requested
    # InspectorArguments as a key and the content in the Inspector as a value
    # due to objects being unable to be stored as keys for a dictionary in JSON
    @classmethod
    def _from_execution(
            cls,
            engine_api: EngineAPI,
            context: ActivityReference,
            inspectors: Union[str, List[BaseInspector]]):
        if isinstance(inspectors, str):
            inspectors = json.loads(inspectors)
            request = [ExecuteInspectorsMessageArguments(
                inspector_arguments=ExecuteInspectorCommonArguments(
                    context=context,
                    inspector_arguments=inspector,
                    offset=0,
                    row_count=_MAX_ROW_COUNT),
                inspector_id=uuid4()
            ) for inspector in inspectors]

            response = engine_api.execute_inspectors(request)

            result = {}
            for inspector_response in request:
                result[inspector_response.inspector_arguments.inspector_arguments] = response[inspector_response.inspector_id]
            return result

        elif isinstance(inspectors, List):
            request = []
            inspector_dict = {}
            for inspector in inspectors:
                guid = uuid4()
                inspector_dict[str(guid)] = inspector
                request.append(
                    ExecuteInspectorsMessageArguments(
                        inspector_arguments=ExecuteInspectorCommonArguments(
                            context=context,
                            inspector_arguments=inspector._to_inspector_arguments(),
                            offset=0,
                            row_count=_MAX_ROW_COUNT),
                        inspector_id=guid))

            response = engine_api.execute_inspectors(request)

            result = {}
            for inspector_response in request:
                result[inspector_dict[inspector_response.inspector_id]
                       ] = response[inspector_response.inspector_id]
            return result
