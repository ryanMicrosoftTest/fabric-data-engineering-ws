# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "3eb953e1-7588-43dc-8b3e-22e68004db6d",
# META       "default_lakehouse_name": "bronze_health_lh",
# META       "default_lakehouse_workspace_id": "a8cbda3d-903e-4154-97d9-9a91c95abb42",
# META       "known_lakehouses": [
# META         {
# META           "id": "3eb953e1-7588-43dc-8b3e-22e68004db6d"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # MSFT SQL Server to Spark Data Types

# CELL ********************

from pyspark.sql.types import BooleanType, DecimalType, DoubleType, BinaryType, TimestampType, TimestampNTZType, YearMonthIntervalType, DayTimeIntervalType, CharType, StringType, VarcharType, ShortType, IntegerType, LongType, FloatType, DoubleType, StructField, StructType
import yaml

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# PARAMETERS CELL ********************

# parameters
yaml_metadata_file_loc = 'abfss://metadata_ws@onelake.dfs.fabric.microsoft.com/metadata_lh.Lakehouse/Files/healthDB_dim_product.yaml'


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

### Functions

def generate_pyspark_schema(column_defs, type_mapping):
    fields = []
    for col in column_defs:
        col_name = col['name']
        col_type = col['type']
        spark_type = type_mapping.get(col_type)
        if spark_type is None:
            if _check_handling(col['type']) is not None:
                spark_type = _check_handling(col['type'])
            else:
                raise ValueError(f"Unsupported SQL Server type: {col_type}")
        fields.append(StructField(col_name, spark_type, True))
    return StructType(fields)

def _check_handling(handling_str:str):
    """
    Generic handling method to pass any exceptions found to attempt resolution
    """
    return _handle_nvarchar(handling_str)

def _handle_nvarchar(handling_str:str):
    """
    This will attempt to handle nvarchar exceptions
    """
    if handling_str.strip().lower().startswith('nvarchar'):
        return StringType()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

msft_sql_server_spark_mapping = {
    'bit': BooleanType(),
    'tinyint': ShortType(),
    'smallint': ShortType(),
    'int': IntegerType(),
    'integer': IntegerType(),
    'bigint': LongType(),
    'float(p)': FloatType(),
    'float[p]': FloatType(),
    'double precision': DoubleType(),
    'money': DecimalType(19, 4),
    'smallmoney': DecimalType(10, 4),
    'smalldatetime': TimestampType(),
    'binary': BinaryType(),
    'varbinary': BinaryType(),
    'nchar': StringType(),
    'nvarchar': StringType()
}


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# read yaml file
yaml_content = mssparkutils.fs.head("abfss://metadata_ws@onelake.dfs.fabric.microsoft.com/metadata_lh.Lakehouse/Files/healthDB_dim_product.yaml")

metadata = yaml.safe_load(yaml_content)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

metadata

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

source_path = metadata['source']['path']
source_type = metadata['source']['type']
source_format = metadata['source']['format']
schema_evolution_enabled = metadata['target']['schema']['schema_evolution']['enabled']
source_schema = metadata['source']['schema']


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

schema_evolution_enabled == True

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

source_schema

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# start with base reader
reader = spark.read.format(source_format)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# check if defined schema
if 'schema' in metadata['source']:
    # generate schema
    pyspark_schema = generate_pyspark_schema(metadata['source']['schema']['fields'], msft_sql_server_spark_mapping)

    # add to reader
    reader = reader.schema(pyspark_schema)

# check merge schema

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Generate schema
pyspark_schema = generate_pyspark_schema(source_schema['fields'], msft_sql_server_spark_mapping)
print(pyspark_schema)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if schema_evolution_enabled == True:
    print('Will use schema evolution')
    source_df = spark.read.format(source_format).option('mergeSchema', 'true').load(source_path)
else:
    source_df = spark.read.format(source_format).schema(pyspark_schema).load(source_path)

display(source_df)

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
