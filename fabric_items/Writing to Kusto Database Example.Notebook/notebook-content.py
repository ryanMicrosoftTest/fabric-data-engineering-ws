# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# PARAMETERS CELL ********************

# Parameters - set by pipeline at runtime
parameter = "ImputVar_Two"
kustoUri = ""
database = ""
kustoTable = ""
debug = False

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Writing to Kusto Databases Example
# 
# This notebook writes a pipeline parameter value to a Kusto (Eventhouse) table.
# All connection parameters are externalized for environment promotion (dev/test/prod).

# CELL ********************

import pandas as pd
from pyspark.sql.types import StructType, StructField, StringType

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Validate required parameters
assert kustoUri, "kustoUri parameter is required - set via pipeline"
assert database, "database parameter is required - set via pipeline"
assert kustoTable, "kustoTable parameter is required - set via pipeline"
assert parameter, "parameter value is required"

print(f"Target: {kustoUri} / {database} / {kustoTable}")
print(f"Parameter value: {parameter}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Authenticate using managed identity (no hardcoded secrets)
accessToken = mssparkutils.credentials.getToken(kustoUri)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Create Spark DataFrame with the parameter value
schema = StructType([StructField('parameter', StringType(), True)])
spark_df = spark.createDataFrame([(parameter,)], schema)

if debug:
    display(spark_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Write to Kusto (append mode is the only mode supported)
try:
    spark_df.write \
        .format("com.microsoft.kusto.spark.synapse.datasource") \
        .option("accessToken", accessToken) \
        .option("kustoCluster", kustoUri) \
        .option("kustoDatabase", database) \
        .option("kustoTable", kustoTable) \
        .mode('append') \
        .save()

    print(f"Wrote {spark_df.count()} row(s) to {database}.{kustoTable}")
except Exception as e:
    print(f"Failed to write to Kusto: {e}")
    raise

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
