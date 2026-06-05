# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "5090acaa-9246-4eb2-b9fc-93b8b6e7e60c",
# META       "default_lakehouse_name": "deid_lh",
# META       "default_lakehouse_workspace_id": "a8cbda3d-903e-4154-97d9-9a91c95abb42",
# META       "known_lakehouses": [
# META         {
# META           "id": "5090acaa-9246-4eb2-b9fc-93b8b6e7e60c"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

df = spark.read.format('delta').load('abfss://a8cbda3d-903e-4154-97d9-9a91c95abb42@onelake.dfs.fabric.microsoft.com/5090acaa-9246-4eb2-b9fc-93b8b6e7e60c/Tables/doctor_memos_fact_obt')

display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM deid_lh.doctor_memos_fact_obt LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
