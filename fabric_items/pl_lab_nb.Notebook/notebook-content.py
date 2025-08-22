# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "d338a208-e2db-47b9-9701-e5716c2e94de",
# META       "default_lakehouse_name": "lab_lh",
# META       "default_lakehouse_workspace_id": "a8cbda3d-903e-4154-97d9-9a91c95abb42",
# META       "known_lakehouses": [
# META         {
# META           "id": "d338a208-e2db-47b9-9701-e5716c2e94de"
# META         }
# META       ]
# META     }
# META   }
# META }

# PARAMETERS CELL ********************

table_name = ''
file_location = 'Files/new_data/sales.csv'
cols_list = ['year', 'month', 'firstName', 'lastName']

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import *

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.read.format("csv").option("header","true").load(file_location)
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = (df.withColumn('year', year(col('OrderDate')))
        .withColumn('month', month(col('OrderDate')))
        .withColumn('firstName', split(col('CustomerName'), ' ').getItem(0))
        .withColumn('lastName', split(col('CustomerName'), ' ').getItem(1))
)

df = df['SalesOrderNumber',
 'SalesOrderLineNumber',
 'OrderDate',
 'year',
 'month',
 'firstName',
 'lastName',
 'EmailAddress',
 'Item',
 'Quantity',
 'UnitPrice',
 'TaxAmount'
 ]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df)

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
