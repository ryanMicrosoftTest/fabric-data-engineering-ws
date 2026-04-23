# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "d96b3ef2-264e-41b2-8c19-cc74be38b423",
# META       "default_lakehouse_name": "weather_lh",
# META       "default_lakehouse_workspace_id": "a8cbda3d-903e-4154-97d9-9a91c95abb42",
# META       "known_lakehouses": [
# META         {
# META           "id": "d96b3ef2-264e-41b2-8c19-cc74be38b423"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Weather API Ingestion
# https://www.ncei.noaa.gov/access/services/data/v1

# CELL ********************


CONFIG = {
    "base_url": "https://www.ncei.noaa.gov/access/services/data/v1",
    "request": {
        "dataset": "daily-summaries",
        "stations": "USW00013874",
        "startDate": "2024-01-01",
        "endDate": "2024-01-31",
        "includeStationName": "true",
        "units": "standard",
        "format": "json",
        "timeout": 30,
    },
    "schema": {
        "numeric_columns": [
            "WSF2", "WSF5", "SNOW", "RHMX", "ASLP", "PRCP", "RHAV", "SNWD",
            "ASTP", "RHMN", "WDF2", "AWND", "WDF5", "AWBT", "TMAX", "ADPT",
            "TMIN", "WT01", "WT02", "WT03", "WT08",
        ]
    },
    "fabric": {
        "workspace": "Ryan Development Workspace",
        "lakehouse": "weather_lh",
        "table": "weather_daily_summaries",
    },
}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import requests
import pandas as pd

def extract_records(config):
    request_cfg = dict(config.get("request", {}))
    timeout = request_cfg.pop("timeout", 30)
    response = requests.get(config["base_url"], params=request_cfg, timeout=timeout)
    response.raise_for_status()
    payload = response.json()
    if not isinstance(payload, list):
        raise TypeError("Expected a list payload from NCEI API")
    return payload

records = extract_records(CONFIG)
print(f"Fetched {len(records)} records")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def normalize_records(records, numeric_columns):
    df = pd.DataFrame(records)
    if "DATE" in df.columns:
        df["DATE"] = pd.to_datetime(df["DATE"], format="%Y-%m-%d", errors="raise")
    for column in ["STATION", "NAME"]:
        if column in df.columns:
            df[column] = df[column].astype("string")
    present_numeric = [c for c in numeric_columns if c in df.columns]
    if present_numeric:
        df[present_numeric] = (
            df[present_numeric]
            .apply(lambda s: pd.to_numeric(s.astype("string").str.strip(), errors="coerce"))
            .astype("Float64")
        )
    return df

df = normalize_records(records, CONFIG["schema"]["numeric_columns"])
print(df.dtypes)
df.head()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Write to lakehouse Delta table.
df_for_spark = df.copy()
for col in df_for_spark.select_dtypes(include=["Float64"]).columns:
    df_for_spark[col] = df_for_spark[col].astype("float64")
for col in df_for_spark.select_dtypes(include=["string"]).columns:
    df_for_spark[col] = df_for_spark[col].astype("object")

spark_df = spark.createDataFrame(df_for_spark)
table_name = CONFIG["fabric"]["table"]
spark_df.write.mode("overwrite").format("delta").saveAsTable(table_name)
print(f"Wrote {spark_df.count()} rows to table: {table_name}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Validation
result = spark.sql(f"SELECT COUNT(*) AS row_count FROM {CONFIG['fabric']['table']}")
result.show()
spark.sql(f"DESCRIBE TABLE {CONFIG['fabric']['table']}").show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
