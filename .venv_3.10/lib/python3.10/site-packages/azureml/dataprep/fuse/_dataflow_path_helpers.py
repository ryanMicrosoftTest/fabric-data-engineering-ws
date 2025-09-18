from azureml.dataprep import col

from ._dataflowconstants import PORTABLE_PATH, STREAM_SIZE, LAST_MODIFIED, CAN_SEEK, STREAM_PROPERTIES
from ._streaminfo import StreamDetails


def get_stream_details(dataflow, path: str, files_column: str) -> StreamDetails:
    matching_rows = dataflow.filter(col(PORTABLE_PATH) == path) \
        .add_column(col(STREAM_SIZE, col(STREAM_PROPERTIES)), STREAM_SIZE, STREAM_PROPERTIES) \
        .add_column(col(LAST_MODIFIED, col(STREAM_PROPERTIES)), LAST_MODIFIED, STREAM_SIZE) \
        .add_column(col(CAN_SEEK, col(STREAM_PROPERTIES)), CAN_SEEK, LAST_MODIFIED) \
        .take(1) \
        ._to_pyrecords()
    if len(matching_rows) == 0:
        return None

    row = matching_rows[0]
    return StreamDetails(row[files_column],
                         row[PORTABLE_PATH],
                         row[STREAM_SIZE],
                         row[LAST_MODIFIED],
                         row[CAN_SEEK])
