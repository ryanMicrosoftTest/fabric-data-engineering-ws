# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

name = 'healthdata'
workspace_id = 'a8cbda3d-903e-4154-97d9-9a91c95abb42'
lakehouse_id = '4bd2cd19-0177-4fa9-9ab7-a67a69233f72'
shortcut_location = 'Files'
target_type = 'AdlsGen2'
target_connection_id = '1f126619-984e-4ab2-b5fa-42d5680242f7'
target_location = 'https://healthsaadlsrheus.dfs.core.windows.net'
target_subpath = '/healthdata'
data_type = 'Folder'


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# this is left outside of parameters because it is expected to be defined only once
metadata_lh_abfs_table_path = 'abfss://metadata_ws@onelake.dfs.fabric.microsoft.com/metadata_lh.Lakehouse/Tables'

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.types import StructType, StructField, StringType
from delta.tables import DeltaTable


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# build csv data parameter
shortcut_table_csv_data = f'{name},{workspace_id},{lakehouse_id},{shortcut_location},{target_type},{target_connection_id},{target_location},{target_subpath},{data_type}'

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

shortcut_table_schema = StructType([
    StructField("name", StringType(), True),
    StructField("workspace_id", StringType(), True),
    StructField("lakehouse_id", StringType(), True),
    StructField("shortcut_location", StringType(), True),
    StructField("target_type", StringType(), True),
    StructField("target_connection_id", StringType(), True),
    StructField("target_location", StringType(), True),
    StructField("target_subpath", StringType(), True),
    StructField("data_type", StringType(), True)
])


data_rows = [row.split(",") for row in shortcut_table_csv_data.strip().split("\n")]


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# grab existing table
existing_df = spark.read.format('delta').load(f'{metadata_lh_abfs_table_path}/shortcut_table')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

shortcut_df = spark.createDataFrame(data_rows, schema=shortcut_table_schema)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(shortcut_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# perform the merge
(
    existing_delta_table.alias('target')
        .merge(
            shortcut_df.alias('source'),
            """
            target.name = source.name 
            and target.workspace_id = source.workspace_id 
            and target.lakehouse_id = source.lakehouse_id
            """   
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()

)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

delta_table_post_merge = DeltaTable.forPath(spark, f'{metadata_lh_abfs_table_path}/shortcut_table')

delta_table_df = delta_table_post_merge.toDF()

display(delta_table_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
