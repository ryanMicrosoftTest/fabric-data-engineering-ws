# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "402811bc-5048-4f85-b426-a30bf2a6b0a8",
# META       "default_lakehouse_name": "bronze_lakehouse",
# META       "default_lakehouse_workspace_id": "a8cbda3d-903e-4154-97d9-9a91c95abb42"
# META     }
# META   }
# META }

# CELL ********************

# Set arguments
cw_schema = "SalesLT"
cw_table = "Address"
cw_primaryKey = "AddressID"
cw_location = "20240426"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.conf.set("sprk.sql.parquet.vorder.enabled", "true")
spark.conf.set("spark.microsoft.delta.optimizeWrite.enabled", "true")
spark.conf.set("spark.microsoft.delta.optimizeWrite.binSize", "1073741824")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark import *
from pyspark.sql.window import Window
from pyspark.sql.functions import *
from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType, BooleanType, DateType
from typing import List
from datetime import datetime

# Read Parquet data from landing zone location
dataChanged = spark.read.format("parquet").load('abfss://aom_arrivals_team1@onelake.dfs.fabric.microsoft.com/Bronze.Lakehouse/Files/' + cw_location + '/' + cw_schema + '.' + cw_table + '.parquet')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("""DROP TABLE IF EXISTS """ + cw_table)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Add metadata loading_date column using current date
from pyspark.sql.functions import current_date
dataChanged2 = dataChanged.withColumn("loading_date", current_date().cast("string"))
# Overwrite table
dataChanged2.write.format("delta").mode("overwrite").save("abfss://aom_arrivals_team1@onelake.dfs.fabric.microsoft.com/Bronze.Lakehouse/Tables/" + cw_table)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
