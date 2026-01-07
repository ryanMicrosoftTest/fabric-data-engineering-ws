# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# ##### Create Control Table


# CELL ********************

from pyspark.sql.types import StringType, StructField, StructType, StringType, ArrayType

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

control_table_schema = StructType([
    StructField("source_lh_name",            StringType(), False),
    StructField("source_lh_workspace_id",    StringType(), False),
    StructField("target_lh_workspace_id",    StringType(), False),
    StructField("source_lh_id",              StringType(), False),
    StructField("target_lh_id",              StringType(), False),
    StructField("source_abfss_path",         StringType(), False),
    StructField("target_abfss_path",         StringType(), False),
    StructField("table_name",                StringType(), False),
])


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ['airline', 'airline_delta']

bronze_lh_map = [{
    'source_lh_name': 'bronze_lh',
    'source_lh_workspace_id': 'a8cbda3d-903e-4154-97d9-9a91c95abb42',
    'target_lh_workspace_id': 'c6477a7a-4704-4c20-a04d-7d81f3413ab0',
    'source_lh_id': '07a81854-b712-44b4-8a59-5a021c2f4d32',
    'target_lh_id': '8a9e7e6d-9f0f-41dd-9c29-713be27eb5e4',
    'source_abfss_path':'abfss://a8cbda3d-903e-4154-97d9-9a91c95abb42@onelake.dfs.fabric.microsoft.com/07a81854-b712-44b4-8a59-5a021c2f4d32/Tables',
    'target_abfss_path':'abfss://c6477a7a-4704-4c20-a04d-7d81f3413ab0@onelake.dfs.fabric.microsoft.com/8a9e7e6d-9f0f-41dd-9c29-713be27eb5e4/Tables',
    'table_name': 'airline'
},
{
    'source_lh_name': 'bronze_lh',
    'source_lh_workspace_id': 'a8cbda3d-903e-4154-97d9-9a91c95abb42',
    'target_lh_workspace_id': 'c6477a7a-4704-4c20-a04d-7d81f3413ab0',
    'source_lh_id': '07a81854-b712-44b4-8a59-5a021c2f4d32',
    'target_lh_id': '8a9e7e6d-9f0f-41dd-9c29-713be27eb5e4',
    'source_abfss_path':'abfss://a8cbda3d-903e-4154-97d9-9a91c95abb42@onelake.dfs.fabric.microsoft.com/07a81854-b712-44b4-8a59-5a021c2f4d32/Tables',
    'target_abfss_path':'abfss://c6477a7a-4704-4c20-a04d-7d81f3413ab0@onelake.dfs.fabric.microsoft.com/8a9e7e6d-9f0f-41dd-9c29-713be27eb5e4/Tables',
    'table_name': 'airline_delta'
}

]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

control_table_df = spark.createDataFrame(bronze_lh_map, schema=control_table_schema)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(control_table_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# write to table
control_table_df.write.format('delta').option("overwriteSchema", "true").mode('overwrite').save('abfss://a8cbda3d-903e-4154-97d9-9a91c95abb42@onelake.dfs.fabric.microsoft.com/94229b35-ada2-44c7-9a33-e6d2092aa482/Tables/backup_control_tbl')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
