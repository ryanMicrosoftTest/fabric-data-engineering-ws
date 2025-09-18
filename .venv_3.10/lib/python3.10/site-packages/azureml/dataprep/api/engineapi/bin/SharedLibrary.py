import pandas, numpy, json
from collections import namedtuple
from azureml.dataprep.native import DataPrepError

def to_string(value):
    if isinstance(value, numpy.int64):
        return int(value)
    if isinstance(value, pandas.Timestamp):
        result = str(value)
        if result[-6:] == '+00:00': # Drop UTC offset as the D3 inspectors don't expect it.
            result = result[:-6]
        return result
    else:
        return value

def replace_errors_as_nan(df: pandas.DataFrame, cols: list) -> pandas.DataFrame:
    try:
        return df[cols].applymap(lambda x: numpy.nan if type(x) == DataPrepError else x)
    except pandas.tslib.OutOfBoundsDatetime:
        # when df has datetime value beyond pandas.Timestamp range, calling DataFrame.applymap raises exception
        for col in cols:
            df[col] = df[col].apply(lambda x: numpy.nan if type(x) == DataPrepError else x)
        return df