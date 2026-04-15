# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "699d2e49-ec6f-42ed-b0be-4ea3e2ed641b",
# META       "default_lakehouse_name": "synthea_lh",
# META       "default_lakehouse_workspace_id": "a8cbda3d-903e-4154-97d9-9a91c95abb42",
# META       "known_lakehouses": [
# META         {
# META           "id": "699d2e49-ec6f-42ed-b0be-4ea3e2ed641b"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

claims_transactions_df = spark.read.csv('abfss://a8cbda3d-903e-4154-97d9-9a91c95abb42@onelake.dfs.fabric.microsoft.com/699d2e49-ec6f-42ed-b0be-4ea3e2ed641b/Files/synthea/output/csv/claims_transactions.csv', header='true')

display(claims_transactions_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

claims_transactions_df.count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

claims_transactions_df.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

claims_transactions_df.write.save('abfss://a8cbda3d-903e-4154-97d9-9a91c95abb42@onelake.dfs.fabric.microsoft.com/699d2e49-ec6f-42ed-b0be-4ea3e2ed641b/Tables/claims_transactions_tbl')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

"""
Example claims transaction table to use in SQL 

id
claim_id
charge_id
patient_id
type
amount
method
from_date
to_date
last_updated_dttm
"""

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# explore the observations data
observations_df = spark.read.csv('abfss://a8cbda3d-903e-4154-97d9-9a91c95abb42@onelake.dfs.fabric.microsoft.com/699d2e49-ec6f-42ed-b0be-4ea3e2ed641b/Files/synthea/output/csv/observations.csv', header='true')

print(f'Total count of records is: {claims_transactions_df.count()}')


observations_df.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# look at data:  This looks like it would be an append only table
display(observations_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# look at providers data
claims_transactions_df = spark.read.csv('abfss://a8cbda3d-903e-4154-97d9-9a91c95abb42@onelake.dfs.fabric.microsoft.com/699d2e49-ec6f-42ed-b0be-4ea3e2ed641b/Files/synthea/output/csv/claims_transactions.csv', header='true')

print(f'Total count of records is: {claims_transactions_df.count()}')


claims_transactions_df.printSchema()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(providers_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# save providers table to delta
providers_df.write.save('abfss://a8cbda3d-903e-4154-97d9-9a91c95abb42@onelake.dfs.fabric.microsoft.com/699d2e49-ec6f-42ed-b0be-4ea3e2ed641b/Tables/providers_tbl')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Now lets see the write time with liquid clustering

providers_df.write.save('abfss://a8cbda3d-903e-4154-97d9-9a91c95abb42@onelake.dfs.fabric.microsoft.com/699d2e49-ec6f-42ed-b0be-4ea3e2ed641b/Tables/providers_tbl_liquid_id')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Different Liquid Clustering Experiments

# CELL ********************

# write to table
claims_transactions_df.write.save('abfss://a8cbda3d-903e-4154-97d9-9a91c95abb42@onelake.dfs.fabric.microsoft.com/699d2e49-ec6f-42ed-b0be-4ea3e2ed641b/Tables/claims_transactions_tbl_liquid_id')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Execute first statement
spark.sql("""
CREATE OR REPLACE TABLE claims_transactions_tbl_liquid_id
CLUSTER BY (ID)
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
)
AS SELECT * FROM claims_transactions_tbl_liquid_id
""")

# Execute second statement
spark.sql("""
ALTER TABLE claims_transactions_tbl_liquid_id
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

# bronze doesn't care about skipping by ID because it will only ever process by ingest_date
# so instead, let's partition by ingest_date instead in bronze
# this way, we can very quickly find the new records needed because we can skip all folders other than the date we're interested in ingesting

from pyspark.sql.functions import row_number, when, col, date_format, date_sub, current_date, lit
from pyspark.sql.window import Window

# Define records per day
records_per_day = 35000

# Add row number to determine which day each record belongs to
window_spec = Window.orderBy(lit(1))  # Arbitrary ordering
claims_transactions_df = claims_transactions_df.withColumn("row_num", row_number().over(window_spec))

# Calculate how many days ago based on row number
# Row 1-35000 = day 0 (17 days ago), 35001-70000 = day 1 (16 days ago), etc.
claims_transactions_df = claims_transactions_df.withColumn(
    "days_ago",
    ((col("row_num") - 1) / records_per_day).cast("int")
)

# Calculate the actual ingest date by subtracting days from current date
claims_transactions_df = claims_transactions_df.withColumn(
    "ingest_date",
    date_format(
        date_sub(current_date(), (17 - col("days_ago"))),  # 17 is max days ago
        "MM-dd-yyyy"
    )
)

# Drop helper columns
claims_transactions_df = claims_transactions_df.drop("row_num", "days_ago")

# Show distribution
print("Ingest date distribution:")
claims_transactions_df.groupBy("ingest_date").count().orderBy("ingest_date").show(20)

# write as new dataframe clustered by ingest date
claims_transactions_df.write.partitionBy('ingest_date').save('abfss://a8cbda3d-903e-4154-97d9-9a91c95abb42@onelake.dfs.fabric.microsoft.com/699d2e49-ec6f-42ed-b0be-4ea3e2ed641b/Tables/claims_transactions_tbl_partition_ingest_date')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Create a Silver Table for claims_transactions liquid clustered by ID

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
