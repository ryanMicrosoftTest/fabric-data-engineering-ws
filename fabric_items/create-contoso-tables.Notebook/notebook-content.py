# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "295a9fb8-19b9-4b14-b2ad-013b5ecf236f",
# META       "default_lakehouse_name": "Contoso",
# META       "default_lakehouse_workspace_id": "a8cbda3d-903e-4154-97d9-9a91c95abb42",
# META       "known_lakehouses": [
# META         {
# META           "id": "295a9fb8-19b9-4b14-b2ad-013b5ecf236f"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

import notebookutils.mssparkutils
abfss://a8cbda3d-903e-4154-97d9-9a91c95abb42@onelake.dfs.fabric.microsoft.com/295a9fb8-19b9-4b14-b2ad-013b5ecf236f/Files/Calendar.csv

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

files_list = notebookutils.mssparkutils.fs.ls('abfss://a8cbda3d-903e-4154-97d9-9a91c95abb42@onelake.dfs.fabric.microsoft.com/295a9fb8-19b9-4b14-b2ad-013b5ecf236f/Files')


for file in files_list:
    print(file.path.split('.')[-2].split('/')[-1])
    df = spark.read.format("csv").option("header","true").load(file.path)
    df.write.format('delta').saveAsTable(file.path.split('.')[-2].split('/')[-1])


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
