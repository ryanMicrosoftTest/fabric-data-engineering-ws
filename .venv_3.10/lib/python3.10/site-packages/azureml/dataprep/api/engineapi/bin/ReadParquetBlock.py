import ParquetFunctions
from collections import namedtuple
import pyarrow.parquet as pq
import pandas

def readParquet(df, args):

    max_rows = args.maxRows if hasattr(args, 'maxRows') and args.maxRows is not None and args.maxRows >= 0 else None
    # Try to extract path from reader arguments, CLex passes paths this way until a CLex <=> Python stream exists.
    path = args.path if hasattr(args, 'path') and args.path is not None else None
    # If no path argument then this is being used for python export and path(s) should be in the passed dataframe.
    if (path == None):
        paths = list(df['Path'])
        if len(paths) > 1:
            raise NotImplementedError("Multiple Parquet files not yet supported.")
        path = paths[0]

    # FUTURE: When maxRows is passed use pyarrow.parquet.ParquetFile.read_row_group to only read the groups
    # needed to satifiy the maxRows. This would need to handle pyarrow.parquet.ParquetDataset also
    # (which read_table does for us now).
    ### Since sampling is done in CLex at the moment maxRows is always None and the whole file is read in.

    # Spark's parquet implementation does not support FIXED_LENGTH_BYTE_ARRAY. This means some files
    # can be successfully opened and processed by CLex or Python Pandas but not by a Spark job.
    # TODO: explcitly not support parquet files with FIXED_LENGTH_BYTE_ARRAY types and fail this read.
    # See: https://issues.apache.org/jira/browse/SPARK-2489

    result =  pq.read_table(path).to_pandas()
    result = ParquetFunctions.makePathColumnUnique(result, 'Path')
    result.insert(loc=0, column='Path', value=path)
    return result
