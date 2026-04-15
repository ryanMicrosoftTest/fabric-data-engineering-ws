# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "environment": {
# META       "environmentId": "4d3272ae-0804-81fa-43d0-eb1439b80f8c",
# META       "workspaceId": "00000000-0000-0000-0000-000000000000"
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Spark Diagnostic Emitters Notebook

# CELL ********************

log4jLogger = spark._jvm.org.apache.log4j
logger = log4jLogger.LogManager.getLogger("PySparkLogger")
logger.setLevel(log4jLogger.Level.INFO)
logger.info("Application started.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
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

logger.info(f"Successfully loaded table doctor_memos_fact_obt")

# this is a bad example of a log write because it happens to create a spark job since it's doing a count
logger.info(f"Total Records in table: {df.count()}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

cardiology_df = df.where('department == "Cardiology"')

logger.info(f"Selecting Cardiology Department")
display(cardiology_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

department_df = spark.read.format('delta').load('abfss://a8cbda3d-903e-4154-97d9-9a91c95abb42@onelake.dfs.fabric.microsoft.com/5090acaa-9246-4eb2-b9fc-93b8b6e7e60c/Tables/dim_department')

logger.info(f"Loading table dim_department")
display(department_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
