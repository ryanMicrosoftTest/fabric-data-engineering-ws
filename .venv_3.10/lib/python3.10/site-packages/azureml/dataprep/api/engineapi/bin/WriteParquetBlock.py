from collections import namedtuple
import pyarrow as pa
import pyarrow.parquet as pq
import Sanitizer
import ParquetFunctions
from azureml.dataprep.native import DataPrepError
import pandas

def writeParquet(df, args):
    emptydf = pandas.DataFrame()

    # If path is None then write() will fail and except, this is expected.
    path = args.path if hasattr(args, 'path') else None
    # If rows per row-group is 0 then we will use dastparquet default, which is 50000000 rows per rowgroup.
    rowsPerRowGroup = args.rowsPerRowGroup if hasattr(args, 'rowsPerRowGroup') and args.rowsPerRowGroup != None and args.rowsPerRowGroup > 0 else None
    error = args.errorReplacement if hasattr(args, 'errorReplacement') and args.errorReplacement != None else None

    # Spark currently asserts Parquet Schema fields (includes column names) cannot contain
    # Any special characters from this list " ,;{}()\\n\\t=". So when writing to parquet
    # column names are 'normalized' to place any of these chars with _, uniqueness is maintained.
    sanitizedNames = ParquetFunctions.sanitizeParquetNames(df.columns)
    df.columns = Sanitizer.makeNamesUnique(sanitizedNames)

    # This was pulled from the value processing code in python exports WriteFiles.
    def process(value):
        if type(value) == DataPrepError:
            return error
        return value

    # Iterate over columns and check if there are any columns containing more than one type.
    # If there is that column must be converted to a fallback type (str) since parquet doesn't support multi-type columns.
    for col in df:
        # When a numpy dtype or python base type doesn't match a column it falls back to object. So mixed type columns should only be objects.
        if (df[col].dtype != 'object'):
            continue

        types = df[col].apply(lambda x: type(x)).drop_duplicates()
        if any(t is DataPrepError for t in types):
            df[col] = df[col].apply(process)
        if (types.count() > 1):
            df[col] = df[col].astype(str)

    table = pa.Table.from_pandas(df, preserve_index=False)

    # If a pandas object column is all None we end up with a null column and
    # pyarrow.parquet cannot determine the type for Parquet column and will fail.
    # So we convert any such columns to nullable string fo all nulls before we write.
    table = nullColumnsToString(table)

    # The parquet format does not officially support Timestamps being written out as 96bit ints. How ever Spark and various other
    # implementations support this because of legacy (impala) reasons. Spark <2.2.0 doesn't support any Timestamp type except INT96.
    # PyArrow >=0.5.0 added support for INT64 backed timestamps and infact started defaulting to them if the supplied Arrow datatype
    # for the datetime is any precision but 'ns'. Pandas Timestamp type converts to Arrow type timestamp(ns) which will be writen to INT96
    # as long as the use_deprecated option is set below. Until we support Spark >=2.2.0 we need to write out Parquet files with INT96
    # datetimes for them to be readable by Spark <2.2.0. We can also no longer coerce timestamps to ms when coming from pandas as this
    # will result in Arrow attempting to write them out as INT64.
    pq.write_table(table, path, use_deprecated_int96_timestamps=True, row_group_size=rowsPerRowGroup)

    return emptydf

def nullColumnsToString(table):
    for i, field in enumerate(table.schema):
        if field.type == pa.null():
            newField = pa.field(field.name, pa.string(), field.nullable, field.metadata)
            newArray = pa.array([None]*len(table[i].data), pa.string())
            newColumn = pa.Column.from_array(newField, newArray)
            table = table.remove_column(i)
            table = table.add_column(i, newColumn)
    return table
