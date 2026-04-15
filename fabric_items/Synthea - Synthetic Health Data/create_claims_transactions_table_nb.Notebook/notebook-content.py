# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "0cf2430d-4fd3-45d0-92e0-5a16e9332ccb",
# META       "default_lakehouse_name": "sql_health_db_lh",
# META       "default_lakehouse_workspace_id": "a8cbda3d-903e-4154-97d9-9a91c95abb42",
# META       "known_lakehouses": [
# META         {
# META           "id": "0cf2430d-4fd3-45d0-92e0-5a16e9332ccb"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

spark.sql("""
CREATE TABLE claims_transactions (
    id STRING,
    claim_id STRING,
    charge_id STRING,
    patient_id STRING,
    type STRING,
    amount DECIMAL(10,2),
    method STRING,
    from_date DATE,
    to_date DATE,
    last_updated_dttm TIMESTAMP
)

CLUSTER BY (id)
""")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("OPTIMIZE claims_transactions")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("""
ALTER TABLE claims_transactions
SET TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
)
""")


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
