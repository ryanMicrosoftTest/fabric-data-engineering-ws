# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "e8e0dcae-da48-48b3-81f4-e2665a0c1988",
# META       "default_lakehouse_name": "nyc_taxi_bronze",
# META       "default_lakehouse_workspace_id": "a8cbda3d-903e-4154-97d9-9a91c95abb42",
# META       "known_lakehouses": [
# META         {
# META           "id": "e8e0dcae-da48-48b3-81f4-e2665a0c1988"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

%pip install azure.opendatasets

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


from azureml.opendatasets import NycTlcYellow

data = NycTlcYellow().to_spark_dataframe()



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


import json
from datetime import date

BASE = "https://d37ci6vzurychx.cloudfront.net/trip-data"

# TLC data goes back to 2009. Use current year/month as upper bound.
today = date.today()
end_year, end_month = today.year, today.month

datasets = [
    ("yellow", 2009, 1),
    ("green", 2013, 8),     # green begins later historically; safe default
    ("fhv",   2015, 1),     # fhv begins later; safe default
    ("fhvhv", 2019, 2),     # HVFHV split noted by TLC starting 2019
]

rows = []
for ds, start_y, start_m in datasets:
    y, m = start_y, start_m
    while (y < end_year) or (y == end_year and m <= end_month):
        ym = f"{y}-{m:02d}"
        filename = f"{ds}_tripdata_{ym}.parquet"
        url = f"{BASE}/{filename}"

        # OneLake landing path (partition-style folder structure)
        sink_path = f"Files/nyc_tlc_raw/{ds}/year={y}/month={m:02d}/{filename}"

        rows.append({"dataset": ds, "year": y, "month": m, "url": url, "sink_path": sink_path})

        m += 1
        if m == 13:
            m = 1
            y += 1

manifest_path = "/lakehouse/default/Files/nyc_tlc_raw/_manifest/nyc_tlc_manifest.jsonl"
# write JSON Lines
import os
os.makedirs(os.path.dirname(manifest_path), exist_ok=True)
with open(manifest_path, "w") as f:
    for r in rows:
        f.write(json.dumps(r) + "\n")

print("Wrote manifest rows:", len(rows))
print("Manifest:", manifest_path)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# Create AWS-sourced manifest file in Lakehouse Files
import json
from datetime import date
import os

today = date.today()
END_YEAR = today.year
END_MONTH = today.month

datasets = [
    {
        "dataset": "yellow",
        "start_year": 2009,
        "start_month": 1,
        "prefix": "trip data/yellow_tripdata/yellow_tripdata_"
    },
    {
        "dataset": "green",
        "start_year": 2013,
        "start_month": 8,
        "prefix": "trip data/green_tripdata/green_tripdata_"
    },
    {
        "dataset": "fhv",
        "start_year": 2015,
        "start_month": 1,
        "prefix": "trip data/fhv_tripdata/fhv_tripdata_"
    },
    {
        "dataset": "fhfhv",
        "start_year": 2019,
        "start_month": 2,
        "prefix": "trip data/fhvhv_tripdata/fhvhv_tripdata_"
    }
]

manifest = []

for ds in datasets:
    y, m = ds["start_year"], ds["start_month"]

    while (y < END_YEAR) or (y == END_YEAR and m <= END_MONTH):
        ym = f"{y}-{m:02d}"
        key = f"{ds['prefix']}{ym}.parquet"

        manifest.append({
            "dataset": ds["dataset"],
            "year": y,
            "month": m,
            "bucket": "nyc-tlc",
            "key": key,
            "sink_path": (
                f"Files/nyc_tlc_raw/"
                f"{ds['dataset']}/"
                f"year={y}/"
                f"month={m:02d}/"
                f"{key.split('/')[-1]}"
            )
        })

        m += 1
        if m == 13:
            m = 1
            y += 1

manifest_path = "/lakehouse/default/Files/manifests/aws_nyc_tlc_manifest.jsonl"

# Ensure directory exists
os.makedirs(os.path.dirname(manifest_path), exist_ok=True)

# Write JSONL
with open(manifest_path, "w") as f:
    for row in manifest:
        f.write(json.dumps(row) + "\n")

print(f"Manifest written to {manifest_path}")
print(f"Entries: {len(manifest)}")



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import requests
import pandas as pd

# NYC Open Data API endpoint (example for yellow taxi)
url = "https://data.cityofnewyork.us/resource/gkne-dk5s.json?$limit=100000"

# Get data
response = requests.get(url)
pdf = pd.DataFrame(response.json())

# Convert to Spark DataFrame
df = spark.createDataFrame(pdf)
df.write.format("delta").mode("overwrite").saveAsTable("nyc_taxi_data")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Yellow taxi data from 2009-2024 (billions of records)
from datetime import datetime

def load_nyc_taxi_bulk(years, months=range(1, 13), taxi_type="yellow"):
    """
    Load NYC taxi data for specified years and months
    taxi_type: 'yellow', 'green', or 'fhv' (for-hire-vehicle)
    """
    base_url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{taxi_type}_tripdata_{{year}}-{{month:02d}}.parquet"
    
    dfs = []
    for year in years:
        for month in months:
            url = base_url.format(year=year, month=month)
            try:
                print(f"Loading {url}...")
                df_month = spark.read.parquet(url)
                print(f"  ✓ Loaded {df_month.count():,} records from {year}-{month:02d}")
                dfs.append(df_month)
            except Exception as e:
                print(f"  ✗ Failed {year}-{month:02d}: {e}")
    
    # Union all DataFrames
    from functools import reduce
    df_all = reduce(lambda df1, df2: df1.union(df2), dfs)
    return df_all

# Load last 3 years of data (adjust as needed)
df = load_nyc_taxi_bulk(years=[2022, 2023, 2024])

print(f"Total records: {df.count():,}")

# Save to lakehouse
df.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("nyc_taxi_full")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import requests
import pandas as pd

def fetch_and_write_incrementally(base_url, table_name, limit=50000):
    """Fetch data in chunks and write directly to Delta"""
    offset = 0
    total_records = 0
    chunk_num = 0
    
    while True:
        url = f"{base_url}?$limit={limit}&$offset={offset}"
        print(f"Fetching chunk {chunk_num} (offset {offset})...")
        
        try:
            response = requests.get(url, timeout=120)
            response.raise_for_status()
            data = response.json()
            
            if not data:  # No more data
                print("No more data to fetch.")
                break
            
            # Convert to Spark DataFrame
            pdf = pd.DataFrame(data)
            spark_df = spark.createDataFrame(pdf)
            
            # Write this chunk directly to Delta (append mode)
            if chunk_num == 0:
                # First chunk: overwrite
                spark_df.write.format("delta") \
                    .mode("overwrite") \
                    .option("overwriteSchema", "true") \
                    .saveAsTable(table_name)
                print(f"  ✓ Wrote {len(data):,} records (initial)")
            else:
                # Subsequent chunks: append
                spark_df.write.format("delta") \
                    .mode("append") \
                    .saveAsTable(table_name)
                print(f"  ✓ Appended {len(data):,} records")
            
            total_records += len(data)
            offset += limit
            chunk_num += 1
            
            # Break if we got fewer records than requested (last page)
            if len(data) < limit:
                print("Reached last page.")
                break
                
        except Exception as e:
            print(f"  ✗ Error at offset {offset}: {str(e)}")
            break
    
    print(f"\nTotal records written: {total_records:,}")
    return total_records

# Fetch all data
base_url = "https://data.cityofnewyork.us/resource/gkne-dk5s.json"
fetch_and_write_incrementally(base_url, "nyc_taxi_data")

# Verify
df_final = spark.table("nyc_taxi_data")
print(f"Final table count: {df_final.count():,}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df_final)

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
