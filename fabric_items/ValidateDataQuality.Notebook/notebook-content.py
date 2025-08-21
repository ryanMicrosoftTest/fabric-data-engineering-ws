# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
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
try:
    json.loads(metadata)
except ValueError as e:
    mssparkutils.notebook.exit("Metadata parameter is not a valid JSON object.")

json_metadata = json.loads(metadata)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# install libraries
#%pip install great-expectations

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Import great expectations
import great_expectations as ge

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Load data and convert data frame to great expectations data frame
dataChanged = spark.sql("SELECT * FROM Bronze." + cw_table)
columnNames = dataChanged.schema.names
print(columnNames)
df_ge = ge.dataset.SparkDFDataset(dataChanged)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

for item in json_metadata:
    df_ge.expect_column_to_exist(item["ColumnName"])

if df_ge.validate()["success"]:
    print("Validation passed")
else:
    print("Validation failed")
    mssparkutils.notebook.exit("Data quality vaidations have failed.")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
