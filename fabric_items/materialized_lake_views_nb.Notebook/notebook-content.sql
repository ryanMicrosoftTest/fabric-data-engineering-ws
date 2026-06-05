-- Fabric notebook source

-- METADATA ********************

-- META {
-- META   "kernel_info": {
-- META     "name": "synapse_pyspark"
-- META   },
-- META   "dependencies": {
-- META     "lakehouse": {
-- META       "default_lakehouse": "07a81854-b712-44b4-8a59-5a021c2f4d32",
-- META       "default_lakehouse_name": "bronze_lh",
-- META       "default_lakehouse_workspace_id": "a8cbda3d-903e-4154-97d9-9a91c95abb42",
-- META       "known_lakehouses": [
-- META         {
-- META           "id": "07a81854-b712-44b4-8a59-5a021c2f4d32"
-- META         }
-- META       ]
-- META     }
-- META   }
-- META }

-- MARKDOWN ********************

-- 


-- MARKDOWN ********************

-- # Materialized Lake Views

-- CELL ********************

-- MAGIC %%sql
-- MAGIC SELECT id, name, code, country
-- MAGIC FROM bronze_lh.airline

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

-- MAGIC %%sql
-- MAGIC 
-- MAGIC CREATE MATERIALIZED LAKE VIEW IF NOT EXISTS health_silver_lh.clean_airline_mv
-- MAGIC (
-- MAGIC     -- keep constraints row-level; e.g., basic data-quality checks
-- MAGIC     CONSTRAINT not_null_id CHECK (id IS NOT NULL) ON MISMATCH DROP
-- MAGIC )
-- MAGIC COMMENT 'silver table'
-- MAGIC TBLPROPERTIES ('layer' = 'silver')
-- MAGIC AS
-- MAGIC SELECT
-- MAGIC     id, name, code, country
-- MAGIC FROM (
-- MAGIC     SELECT
-- MAGIC         id,
-- MAGIC         name,
-- MAGIC         code,
-- MAGIC         country,
-- MAGIC         ROW_NUMBER() OVER (PARTITION BY name ORDER BY _commit_timestamp DESC, _commit_version DESC) AS rn
-- MAGIC     FROM bronze_lh.airline
-- MAGIC ) d
-- MAGIC WHERE rn = 1;


-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

-- MAGIC %%sql
-- MAGIC CREATE MATERIALIZED LAKE VIEW IF NOT EXISTS health_silver_lh.test_mv AS 
-- MAGIC SELECT id, name, code, country
-- MAGIC FROM bronze_lh.airline

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************


-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }
