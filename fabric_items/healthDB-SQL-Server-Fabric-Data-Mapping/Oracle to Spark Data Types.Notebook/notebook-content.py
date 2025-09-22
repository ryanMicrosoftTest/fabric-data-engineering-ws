# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# # Oracle to Spark Data Types

# CELL ********************

from pyspark.sql.types import BooleanType, DecimalType, FloatType, DoubleType, BinaryType, TimestampType, TimestampNTZType, YearMonthIntervalType, DayTimeIntervalType, CharType, StringType, VarcharType

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

oracle_spark_mapping = {
    'BOOLEAN', BooleanType(),
    'NUMBER', DecimalType(),
    
}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
