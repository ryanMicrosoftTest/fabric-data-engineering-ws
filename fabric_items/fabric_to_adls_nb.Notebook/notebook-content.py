# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# # Fabric To ADLS

# CELL ********************

import pyspark.sql.dataframe

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def write_to_adls(df:pyspark.sql.dataframe, container:str, storage_account:str, path:str)-> None:
    """
    Utility to write to an ADLS path via abfss

    df: DataFrame: The dataframe to write to ADLS
    container:str: The name of the container to write to
    storage_account:str The name of the storage account
    path:str:  The path to persist the data to

    returns: None
    """
    target = f'abfss://{container}@{storage_account}.dfs.core.windows.net/{path}/'
    print(target)
    df.write.format('parquet').mode('overwrite').save(target)




# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.read.load('abfss://a8cbda3d-903e-4154-97d9-9a91c95abb42@onelake.dfs.fabric.microsoft.com/70e18f53-f14f-41bd-b3d0-8060d42c4909/Tables/employee')

display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

write_to_adls(df, 'output', 'healthsaadlsrheus', 'employee_parquet')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Alternatively, write to a shortcut

df = df.filter(df.last_name == 'Jackson')

display(df)

df.write.format('delta').mode('overwrite').save('abfss://a8cbda3d-903e-4154-97d9-9a91c95abb42@onelake.dfs.fabric.microsoft.com/70e18f53-f14f-41bd-b3d0-8060d42c4909/Tables/employee_1')

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
