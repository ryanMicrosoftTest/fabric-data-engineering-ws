# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "57dc47b1-26b1-4097-8e76-363c9af12c84",
# META       "default_lakehouse_name": "SalesLakehouse",
# META       "default_lakehouse_workspace_id": "a8cbda3d-903e-4154-97d9-9a91c95abb42",
# META       "known_lakehouses": [
# META         {
# META           "id": "57dc47b1-26b1-4097-8e76-363c9af12c84"
# META         },
# META         {
# META           "id": "c1624db2-ba23-4c47-94b5-9b8dad08b915"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# Create a Materialized Views Notebook that goes across multiple lakehouses in the same workspace

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE SCHEMA IF NOT EXISTS `Ryan Development Workspace`.SalesLakehouseSilver.silver

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT *
# MAGIC FROM SalesLakehouse.bronze.agent

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT *
# MAGIC FROM SalesLakehouse.bronze.agent_commissions

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT *
# MAGIC FROM SalesLakehouse.bronze.orders

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT *
# MAGIC FROM SalesLakehouse.bronze.sales

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as f
from datetime import datetime

spark.sql(
    """
    CREATE SCHEMA IF NOT EXISTS `Ryan Development Workspace`.SalesLakehouseSilver.silver_stage
    """
)
bronze_sales_df = spark.read.format('delta').table('SalesLakehouse.bronze.sales')

bronze_sales_df = bronze_sales_df.withColumn('process_time', f.lit(datetime.now()))

bronze_sales_df.write.format('delta').save('abfss://a8cbda3d-903e-4154-97d9-9a91c95abb42@onelake.dfs.fabric.microsoft.com/c1624db2-ba23-4c47-94b5-9b8dad08b915/Tables/silver_stage/sales_stage')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(
    spark.sql(
    """
    SELECT *
    FROM SalesLakehouseSilver.silver_stage.sales_stage
    """
)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# note that MLV Constraints are per row boolean checks so they can't enforce uniqueness semantics
spark.sql(
    """
DROP MATERIALIZED LAKE VIEW IF EXISTS SalesLakehouseSilver.silver.sales_clean
    """
)
spark.sql(
    """

CREATE MATERIALIZED LAKE VIEW IF NOT EXISTS SalesLakehouseSilver.silver.sales_clean
(
  CONSTRAINT sales_quantity_chk CHECK (quantity != 0) ON MISMATCH DROP,
  CONSTRAINT quantity_check     CHECK (quantity >  0) ON MISMATCH DROP,
  CONSTRAINT discount_check     CHECK (discount >= 0 AND discount <= 7) ON MISMATCH DROP
)
AS
SELECT s.*
FROM SalesLakehouseSilver.silver_stage.sales_stage AS s

WHERE 
    s.order_id IN (SELECT order_id FROM SalesLakehouse.bronze.orders)

"""
)

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
