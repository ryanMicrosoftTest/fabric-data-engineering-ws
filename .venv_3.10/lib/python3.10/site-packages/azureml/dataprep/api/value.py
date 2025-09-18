# Copyright (c) Microsoft Corporation. All rights reserved.
import math
from datetime import datetime, timedelta, timezone
from typing import Any
from .engineapi.typedefinitions import DataField, FieldType
from ._pandas_helper import have_numpy, have_pandas


def field_type_to_string(field_type: FieldType) -> str:
    if field_type == FieldType.BOOLEAN:
        return 'bool'
    elif field_type == FieldType.DATAROW:
        return 'dict'
    elif field_type == FieldType.DATE:
        return 'datetime'
    elif field_type == FieldType.DECIMAL:
        return 'float'
    elif field_type == FieldType.ERROR:
        return 'DataPrepError'
    elif field_type == FieldType.INTEGER:
        return 'int'
    elif field_type == FieldType.LIST:
        return 'list'
    elif field_type == FieldType.NULL:
        return 'None'
    elif field_type == FieldType.STREAM:
        return 'StreamInfo'
    elif field_type == FieldType.STRING:
        return 'str'
    elif field_type == FieldType.UNKNOWN:
        return 'Unknown'
    else:
        raise ValueError("Unexpected FieldType")


def to_dprep_value(value: Any) -> Any:
    return to_dprep_value_and_type(value)[0]


def to_dprep_value_and_type(value: Any) -> (Any, FieldType):
    if isinstance(value, list):
        data = [to_dprep_value(v) for v in value]
        return data, FieldType.LIST

    if isinstance(value, str):
        return value, FieldType.STRING
    if value is None:
        return value, FieldType.NULL
    if isinstance(value, bool):
        return value, FieldType.BOOLEAN
    if isinstance(value, int):
        return value, FieldType.INTEGER
    if hasattr(value, 'dtype') and value.dtype.kind == 'i':  # numpy.int*
        return int(value), FieldType.INTEGER
    if hasattr(value, 'dtype') and value.dtype.kind == 'b':  # numpy.bool*
        return bool(value), FieldType.BOOLEAN

    if value == float('inf'):
        return {'n': 1}, FieldType.DECIMAL
    if value == -float('inf'):
        return {'n': -1}, FieldType.DECIMAL

    if isinstance(value, float):
        if have_numpy():
            import numpy
            try:
                if numpy.isnan(value):
                    return {'n': 0}, FieldType.DECIMAL
            except TypeError:
                pass
        return value, FieldType.DECIMAL

    if hasattr(value, 'dtype') and value.dtype.kind == 'f':  # numpy.float*
        return float(value), FieldType.DECIMAL

    if isinstance(value, datetime):
        if have_numpy() and have_pandas():
            import numpy
            import pandas
            try:
                if isinstance(value, type(pandas.NaT)):
                    return None, FieldType.DATE
                if isinstance(value, pandas.Timestamp) or isinstance(value, numpy.datetime64):
                    value = pandas.Timestamp(value).to_pydatetime()
            except TypeError:
                pass
        diff = value - datetime(1, 1, 1)
        ticks = diff.days * 864000000000 + diff.seconds * 10000000 + diff.microseconds * 10
        return {'d': ticks}, FieldType.DATE

    if isinstance(value, dict):
        return {'r': [item for sublist in [[k, v] for (k, v) in value.items()] for item in sublist]}, FieldType.DATAROW

    raise ValueError('The value ' + str(value) + ' cannot be used in an expression.')


_TIMESTAMP_KEY = 'timestamp'
_TICKS_KEY = 'd'
_NAN_STRING = 'NaN'
_FLOAT_KEY = 'n'


def value_from_field(field: DataField) -> Any:
    if field.type == FieldType.DECIMAL and field.value == _NAN_STRING:
        return math.nan
    elif field.type == FieldType.DECIMAL and hasattr(field.value, '__contains__') and _FLOAT_KEY in field.value:
        special_value = field.value[_FLOAT_KEY]
        if special_value == 1:
            return float('inf')
        elif special_value == -1:
            return float('-inf')
        else:
            return float(special_value)
    elif field.type == FieldType.DATE and _TIMESTAMP_KEY in field.value:
        return datetime.fromtimestamp(0, timezone.utc) + timedelta(milliseconds=field.value[_TIMESTAMP_KEY])
    elif field.type == FieldType.DATE and _TICKS_KEY in field.value:
        return datetime(1, 1, 1, tzinfo=timezone.utc) + timedelta(milliseconds=field.value[_TICKS_KEY] / 10000)
    else:
        return field.value
