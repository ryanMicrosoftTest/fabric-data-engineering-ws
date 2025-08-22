# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "248bd845-017d-4063-8247-d90e8cff1402",
# META       "default_lakehouse_name": "Contoso",
# META       "default_lakehouse_workspace_id": "e4af6b98-5abd-4a30-9f0f-b897edbb9929",
# META       "known_lakehouses": [
# META         {
# META           "id": "248bd845-017d-4063-8247-d90e8cff1402"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# ### Spark session configuration
# This cell sets Spark session settings to enable _Verti-Parquet_ and _Optimize on Write_. More details about _Verti-Parquet_ and _Optimize on Write_ in tutorial document.

# CELL ********************

spark.conf.set("sprk.sql.parquet.vorder.enabled", "true")
spark.conf.set("spark.microsoft.delta.optimizeWrite.enabled", "true")
spark.conf.set("spark.microsoft.delta.optimizeWrite.binSize", "1073741824")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Create and Load tables from CSV files
# This cell creates a function to read raw data from the _Files_ section of the lakehouse for the table name passed as a parameter. Next, it creates a list of tables. Finally, it has a _for loop_ to loop through the list of tables and call above function with each table name as parameter to read data for that specific table and create delta table.

# CELL ********************

from pyspark.sql.types import *

def loadFullDataFromSource(table_name):
    df = spark.read.option("inferSchema","true").option("header","true").format("csv").load("Files/raw/" + table_name +".csv")


    df.write.mode("overwrite").format("delta").save("Tables/" + table_name)

full_tables = [
    "Calendar",
    "CustomerGroupMapping",
    "CustomerGroups",
    "Customers",
    "Geography",
    "ProductCategories",
    "ProductSubCategories",
    "Products",
    "Promotions",
    "Stores",
    "StrategicCustomers"
    ]

for table in full_tables:
    loadFullDataFromSource(table)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
