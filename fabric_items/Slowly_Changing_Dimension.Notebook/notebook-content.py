# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "environment": {}
# META   }
# META }

# CELL ********************

cw_table = 'Address'
primaryKey = ''
metadata = '[{\"Id\":1,\"TableName\":\"Address\",\"ColumnName\":\"AddressID\",\"DataType\":\"int\",\"CharacterMaximumLength\":null,\"NumericPrecision\":10,\"NumericScale\":0,\"IsNullable\":\"NO\",\"DateTimePrecision\":null,\"IsPrimaryKey\":true},{\"Id\":2,\"TableName\":\"Address\",\"ColumnName\":\"AddressLine1\",\"DataType\":\"nvarchar\",\"CharacterMaximumLength\":60,\"NumericPrecision\":null,\"NumericScale\":null,\"IsNullable\":\"NO\",\"DateTimePrecision\":null,\"IsPrimaryKey\":false},{\"Id\":3,\"TableName\":\"Address\",\"ColumnName\":\"AddressLine2\",\"DataType\":\"nvarchar\",\"CharacterMaximumLength\":60,\"NumericPrecision\":null,\"NumericScale\":null,\"IsNullable\":\"YES\",\"DateTimePrecision\":null,\"IsPrimaryKey\":false},{\"Id\":4,\"TableName\":\"Address\",\"ColumnName\":\"City\",\"DataType\":\"nvarchar\",\"CharacterMaximumLength\":30,\"NumericPrecision\":null,\"NumericScale\":null,\"IsNullable\":\"NO\",\"DateTimePrecision\":null,\"IsPrimaryKey\":false},{\"Id\":5,\"TableName\":\"Address\",\"ColumnName\":\"StateProvince\",\"DataType\":\"int\",\"CharacterMaximumLength\":null,\"NumericPrecision\":10,\"NumericScale\":0,\"IsNullable\":\"NO\",\"DateTimePrecision\":null,\"IsPrimaryKey\":false},{\"Id\":6,\"TableName\":\"Address\",\"ColumnName\":\"PostalCode\",\"DataType\":\"nvarchar\",\"CharacterMaximumLength\":15,\"NumericPrecision\":null,\"NumericScale\":null,\"IsNullable\":\"NO\",\"DateTimePrecision\":null,\"IsPrimaryKey\":false},{\"Id\":7,\"TableName\":\"Address\",\"ColumnName\":\"CountryRegion\",\"DataType\":\"geography\",\"CharacterMaximumLength\":null,\"NumericPrecision\":null,\"NumericScale\":null,\"IsNullable\":\"YES\",\"DateTimePrecision\":null,\"IsPrimaryKey\":false},{\"Id\":8,\"TableName\":\"Address\",\"ColumnName\":\"rowguid\",\"DataType\":\"uniqueidentifier\",\"CharacterMaximumLength\":null,\"NumericPrecision\":null,\"NumericScale\":null,\"IsNullable\":\"NO\",\"DateTimePrecision\":null,\"IsPrimaryKey\":false},{\"Id\":9,\"TableName\":\"Address\",\"ColumnName\":\"ModifiedDate\",\"DataType\":\"datetime\",\"CharacterMaximumLength\":null,\"NumericPrecision\":null,\"NumericScale\":null,\"IsNullable\":\"NO\",\"DateTimePrecision\":3,\"IsPrimaryKey\":false}]'

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import json

# Validate metadata object
try:
    json.loads(metadata)
except ValueError as e:
    mssparkutils.notebook.exit("Metadata parameter is not a valid JSON object.")

json_metadata = json.loads(metadata)

# Set primary key if found in metadata
for item in json_metadata:
    if item["IsPrimaryKey"]:
        primaryKey = item["ColumnName"]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark import *
from pyspark.sql.functions import *

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Import functions
from pyspark.sql.functions import sha2, concat_ws

# Fetch data from Bronze
dataChanged = spark.sql("SELECT * FROM Bronze." + cw_table)

# Remove loading_date column from dataset
dataChanged = dataChanged.drop('loading_date')

# Create var with all column names
columnNames = dataChanged.schema.names

# Generate hash key if primary is missing
if not primaryKey or primaryKey == "":
    dataChanged = dataChanged.withColumn("hash", sha2(concat_ws("||", *dataChanged.columns), 256))
    primaryKey = 'hash'

# Create list with all columns
columnNames = dataChanged.schema.names

dataChanged.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import datetime
current_date = datetime.date.today()

try:
    # Read original data - this is your scd type 2 table holding all data
    dataOriginal = spark.sql("SELECT * FROM Silver." + cw_table)
except:
    # Use first load when no data exists yet
    newOriginalData = dataChanged.withColumn('current', lit(True)).withColumn('effectiveDate', lit(current_date)).withColumn('endDate', lit(datetime.date(9999, 12, 31)))
    newOriginalData.write.format("delta").mode("overwrite").saveAsTable("Silver." + cw_table)
    newOriginalData.show()
    newOriginalData.printSchema()
    mssparkutils.notebook.exit("Done loading data! Newly loaded data will be used to generate original data.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Prepare for merge, rename columns of newly loaded data, append 'src_'
from pyspark.sql import functions as F
from pyspark.sql.functions import lit

# Rename all columns in dataChanged, prepend src_, and add additional columns
df_new = dataChanged.select([F.col(c).alias("src_"+c) for c in dataChanged.columns])
src_columnNames = df_new.schema.names
df_new2 = df_new.withColumn('src_current', lit(True)).withColumn('src_effectiveDate', lit(current_date)).withColumn('src_endDate', lit(datetime.date(9999, 12, 31)))

import hashlib

# Create dynamic columns
src_primaryKey = 'src_' + primaryKey

# FULL Merge, join on key column and also high date column to make only join to the latest records
df_merge = dataOriginal.join(df_new2, (df_new2[src_primaryKey] == dataOriginal[primaryKey]), how='fullouter')

# Derive new column to indicate the action
df_merge = df_merge.withColumn('action',
    when(md5(concat_ws('+', *columnNames)) == md5(concat_ws('+', *src_columnNames)), 'NOACTION')
    .when(df_merge.current == False, 'NOACTION')
    .when(df_merge[src_primaryKey].isNull() & df_merge.current, 'DELETE')
    .when(df_merge[src_primaryKey].isNull(), 'INSERT')
    .otherwise('UPDATE')
)

# Generate target selections based on action codes
column_names = columnNames + ['current', 'effectiveDate', 'endDate']
src_column_names = src_columnNames + ['src_current', 'src_effectiveDate', 'src_endDate']

# Generate target selections based on action codes
column_names = columnNames + ['current', 'effectiveDate', 'endDate']
src_column_names = src_columnNames + ['src_current', 'src_effectiveDate', 'src_endDate']

# For records that needs no action
df_merge_p1 = df_merge.filter(df_merge.action == 'NOACTION').select(column_names)

# For records that needs insert only
df_merge_p2 = df_merge.filter(df_merge.action == 'INSERT').select(src_column_names)
df_merge_p2_1 = df_merge_p2.select([F.col(c).alias(c.replace(c[0:4], "")) for c in df_merge_p2.columns])

# For records that needs to be deleted
df_merge_p3 = df_merge.filter(df_merge.action == 'DELETE').select(column_names).withColumn('current', lit(False)).withColumn('endDate', lit(current_date))

# For records that needs to be expired and then inserted
df_merge_p4_1 = df_merge.filter(df_merge.action == 'UPDATE').select(src_column_names)
df_merge_p4_2 = df_merge_p4_1.select([F.col(c).alias(c.replace(c[0:4], "")) for c in df_merge_p2.columns])

# Replace src_ alias in all columns
df_merge_p4_3 = df_merge.filter(df_merge.action == 'UPDATE').withColumn('endDate', date_sub(df_merge.src_effectiveDate, 1)).withColumn('current', lit(False)).select(column_names)

# Union all records together
df_merge_final = df_merge_p1.unionAll(df_merge_p2).unionAll(df_merge_p3).unionAll(df_merge_p4_2).unionAll(df_merge_p4_3)

# At last, you can overwrite existing data using this new data frame
df_merge_final.write.format("delta").mode("overwrite").saveAsTable("Silver." + cw_table)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
