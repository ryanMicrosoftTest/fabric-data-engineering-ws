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

df = spark.read.parquet("Files/airline/part-00000-tid-8446832410521168300-525ac47f-a4af-4c7f-bcad-8bfcfde751b6-129-1.c000.snappy.parquet")
# df now is a Spark DataFrame containing parquet data from "Files/airline/part-00000-tid-8446832410521168300-525ac47f-a4af-4c7f-bcad-8bfcfde751b6-129-1.c000.snappy.parquet".
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
