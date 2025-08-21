# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "cb17676c-cefe-4bf3-b9a8-cf5cef0725a1",
# META       "default_lakehouse_name": "shortcut_lh",
# META       "default_lakehouse_workspace_id": "a8cbda3d-903e-4154-97d9-9a91c95abb42",
# META       "known_lakehouses": [
# META         {
# META           "id": "cb17676c-cefe-4bf3-b9a8-cf5cef0725a1"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

table_df = spark.sql("SELECT * FROM shortcut_lh.airline_parquet LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

files_df = spark.read.parquet("Files/airline/part-00000-tid-7551988471792275449-78d7b4a1-0dcf-425c-b3bf-d4b5ec98976f-128-1.c000.snappy.parquet")
display(df)

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
