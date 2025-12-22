# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "environment": {
# META       "environmentId": "8875d66e-01fc-8fe2-4e57-4eb832814c00",
# META       "workspaceId": "00000000-0000-0000-0000-000000000000"
# META     }
# META   }
# META }

# CELL ********************


sfOptions = {
    "sfURL":       "<account>.snowflakecomputing.com",
    "sfUser":      "<username>",
    "sfPassword":  "<password>",
    "sfDatabase":  "SNOWFLAKE_SAMPLE_DATA",
    "sfSchema":    "TPCH_SF1",
    "sfWarehouse": "COMPUTE_WH",
    "sfRole":      "SYSADMIN"
}




# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

user     = 'user'
password = 'password'

jdbc_url = (
    "jdbc:snowflake://<your_account>.snowflakecomputing.com/"
    "?db=SNOWFLAKE_SAMPLE_DATA&schema=TPCH_SF1&warehouse=COMPUTE_WH"
)

props = {
    "user": user,
    "password": password,
    "driver": "net.snowflake.client.jdbc.SnowflakeDriver"
}

df_jdbc = (spark.read
                 .format("jdbc")
                 .option("url", jdbc_url)
                 .option("dbtable", "CUSTOMER")
                 .options(**props)
                 .load()
                 .limit(10))
display(df_jdbc)

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
