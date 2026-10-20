# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

import notebookutils
from notebookutils.mssparkutils.handlers.fsHandler import FileInfo
from datetime import datetime, timezone
from pyspark.sql import functions as F

import io
from openpyxl import load_workbook
from delta.tables import DeltaTable
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Parameters

# CELL ********************

MERGE_KEYS = ['first_name', 'last_name', 'dob', 'arrival_date']

TABLE_PATH = 'abfss://a8cbda3d-903e-4154-97d9-9a91c95abb42@onelake.dfs.fabric.microsoft.com/aa9f7a03-994d-4738-b7da-8e1e9a06cf89/Tables/dbo/patient_visits'

SCHEMA = StructType([
    StructField('first_name', StringType()),
    StructField('last_name', StringType()),
    StructField('dob', TimestampType()),
    StructField('arrival_date', TimestampType()),
    StructField('departure_date', TimestampType()),
])

# define log path for last modified info
log_path = f'abfss://a8cbda3d-903e-4154-97d9-9a91c95abb42@onelake.dfs.fabric.microsoft.com/aa9f7a03-994d-4738-b7da-8e1e9a06cf89/Files/patient_ingestion_time_log.txt'

# lakehouse file path where the excel files exist via shortcut
lakehouse_files_path = 'abfss://a8cbda3d-903e-4154-97d9-9a91c95abb42@onelake.dfs.fabric.microsoft.com/aa9f7a03-994d-4738-b7da-8e1e9a06cf89/Files/patient_data'

# name of the worksheet within the excel workbook to load
ws_name = 'Patients'

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Functions

# CELL ********************

def get_lakehouse_files(lakehouse_abfss_path:str) -> list[FileInfo]:
    """
    Given a path to a directory, return a list of FileInfo objects

    params:
        lakehouse_abfss_path: str:  The abfss path of the lakehouse where the excel files are

    returns:
        list[FileInfo]: A list of the FileInfo objects which make up all the excel files in the provided lakehouse path

    Examples:
    [FileInfo(path=abfss://a8cbda3d-903e-4154-97d9-9a91c95abb42@onelake.dfs.fabric.microsoft.com/aa9f7a03-994d-4738-b7da-8e1e9a06cf89/Files/patient_data/patients_august.xlsx, name=patients_august.xlsx, size=7439),
    FileInfo(path=abfss://a8cbda3d-903e-4154-97d9-9a91c95abb42@onelake.dfs.fabric.microsoft.com/aa9f7a03-994d-4738-b7da-8e1e9a06cf89/Files/patient_data/patients_july.xlsx, name=patients_july.xlsx, size=7437),
    FileInfo(path=abfss://a8cbda3d-903e-4154-97d9-9a91c95abb42@onelake.dfs.fabric.microsoft.com/aa9f7a03-994d-4738-b7da-8e1e9a06cf89/Files/patient_data/patients_september.xlsx, name=patients_september.xlsx, size=7444)]
    """
    items = notebookutils.fs.ls(lakehouse_abfss_path)

    return items

def get_last_updated_dttm(file_info:list[FileInfo]) ->list[str]:
    """
    This function takes on a list of FileInfo objects and looks for the last updated time for each of them and returns a list of strings of those last updated timestamps

    params:
        file_info: list[FileInfo]:  A list of FileInfo objects representing the excel documents to check the metadata of to see the last modified time

    returns:
        last_modified_list: list[str]:  A list of the last updated timestamp for each file
    """
    last_modified_list = []

    for item in items:
        if item.isFile and item.name.lower().endswith('.xlsx'):
            modified_utc = datetime.fromtimestamp(
                item.modifyTime / 1000,
                tz = timezone.utc
            )
            print(
                f'{item.name} |'
                f'Last Modified: {modified_utc.isoformat()} |'
                f'Size: {item.size} bytes'
            )

            # store in a list
            last_modified_list.append(f'{item.name}, {modified_utc.isoformat()}, {item.size}\n')

    return last_modified_list


def write_log_to_file(last_modified_list:list[str], file_path:str) ->None:
    """
    This takes the last_modified_list, which is the list of strings of the last modified times for each of the excel files in the directory and writes this information to a log file

    params:
        last_modified_list: list[str]:  The list of strings of the last modified datetimes for each of the excel files in the directory
        file_path: str:  The file path where the log should be written to in append only format

    returns:
        None
    """
    for _ in last_modified_list:
        notebookutils.fs.append(file_path,_,True)
    
    print('Logs written successfully')


def get_watermark_dttm(log_path:str)-> str|None:
    """
    Read the log and return the maximum value of it's datetime column

    params:
        log_path: str: Path to the csv log file.  The csv must contain a header with a datetime column

    returns:
        str | None: The max datetime value if exists or None if it does not
    """
    df = spark.read.option('sep', ',').option('header','true').csv(log_path)

    return df.agg(F.max('datetime')).collect()[0][0]


def _read_individual_files_to_delta_table(file_info: FileInfo, table_path: str) -> int:
    """
    This creates or overwrites a delta table at a provided location, merging changes in the underlying files

    params:
        file_info: FileInfo:  The excel files that the table will be built from
        table_path:  str:  The path of the delta table to be created or overwritten if exists
    
    returns:
        count: int: The total amount of rows in the created dataframe
    """
    # load excel file content
    content = spark.read.format('binaryFile').load(file_info.path).collect()[0].content

    # load workbook
    wb = load_workbook(io.BytesIO(content), read_only=True, data_only=True)

    # load the worksheet

    ws = wb[ws_name]
    rows = list(ws.iter_rows(values_only=True))
    wb.close()

    data = [row[:len(SCHEMA)] for row in rows[1:] if any(cell is not None for cell in row)]

    # wrap data into spark dataframe
    df = (spark.createDataFrame(data, SCHEMA)
            .dropDuplicates(MERGE_KEYS)
            .withColumn('source_file', F.lit(file_info.name))
            .withColumn('source_modified',
                        F.lit(datetime.fromtimestamp(file_info.modifyTime / 1000, tz=timezone.utc)))
            .withColumn('load_ts', F.current_timestamp()))

    # create a delta table if one doesn't exist, otherwise overwrite the existing one and return the count of total rows
    if not DeltaTable.isDeltaTable(spark, table_path):
        df.write.format('delta').save(table_path)
    else:
        safe_name = file_info.name.replace("'", "''")
        (df.write.format('delta')
           .mode('overwrite')
           .option('replaceWhere', f"source_file = '{safe_name}'")
           .save(table_path))
    return df.count()

def load_excel_files_to_delta(file_infos: list[FileInfo], table_path: str) -> list[dict]:
    """
    Take a list of file info objects and process them into one delta table, handling for merges
    """
    results = []
    for file in file_infos:
        try:
            total_rows = _read_individual_files_to_delta_table(file, table_path)
            results.append({'file': file.name, 'status': 'success', 'rows': total_rows, 'error': None})
        except Exception as e:
            results.append({'file': file.name, 'status': 'failed', 'rows': 0, 'error': str(e)})
    return results

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Main

# CELL ********************

### get the lakehouse file structure
items = get_lakehouse_files(lakehouse_files_path)

### check for updates to files or any new files
last_modified_list = get_last_updated_dttm(items)
write_log_to_file(last_modified_list, log_path)

last_watermark_time = get_watermark_dttm(log_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

results = load_excel_files_to_delta(items, TABLE_PATH)
for r in results:
    print(r)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

"""

# load excel file content
content = spark.read.format('binaryFile').load(items[0].path).collect()[0].content

# load workbook
wb = load_workbook(io.BytesIO(content), read_only=True, data_only=True)

# load the worksheet

ws = wb[ws_name]
rows = list(ws.iter_rows(values_only=True))
wb.close()

data = [row[:len(SCHEMA)] for row in rows[1:] if any(cell is not None for cell in row)]

# wrap data into spark dataframe
df = (spark.createDataFrame(data, SCHEMA)
        .dropDuplicates(MERGE_KEYS)
        .withColumn('source_file', F.lit(items[0].name))
        .withColumn('source_modified',
                    F.lit(datetime.fromtimestamp(items[0].modifyTime / 1000, tz=timezone.utc)))
        .withColumn('load_ts', F.current_timestamp()))

# create a delta table if one doesn't exist, otherwise overwrite the existing one
if not DeltaTable.isDeltaTable(spark, lakehouse_files_path):
    df.write.format('delta').save(lakehouse_files_path)
else:
    safe_name = file_info.name.replace("'", "''")
    (df.write.format('delta')
        .mode('overwrite')
        .option('replaceWhere', f"source_file = '{safe_name}'")
        .save(lakehouse_files_path))
"""

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
