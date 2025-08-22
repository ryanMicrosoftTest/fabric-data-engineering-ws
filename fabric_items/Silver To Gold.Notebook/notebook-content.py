# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "214ef0d3-997b-4576-acd0-f7fa366e4794",
# META       "default_lakehouse_name": "gold",
# META       "default_lakehouse_workspace_id": "c34e3014-ba4e-45ed-84a9-201d5959fd01",
# META       "known_lakehouses": [
# META         {
# META           "id": "214ef0d3-997b-4576-acd0-f7fa366e4794"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!
spark.conf.set("spark.sql.parquet.vorder.enabled", "true")
spark.conf.set("spark.microsoft.delta.optimizeWrite.enabled","true")
spark.conf.set("spark.microsoft.delta.optimizeWrite.binSize","1073741824")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import monotonically_increasing_id

#Create DimCustomer
df_dimCustomer = spark.sql("select sc.* , sca.AddressID, sca.AddressType from silver.salesCustomer sc join silver.salesCustomerAddress sca on sc.customerid = sca.customerid")

#Add surrogate key as the first column
df_dimCustomer_with_surrogate_key = df_dimCustomer.withColumn("CustomerIDKey", monotonically_increasing_id())\
    .select(
        "CustomerIDKey",  # Select the surrogate key column first
        *[column for column in df_dimCustomer.columns if column != "CustomerIDKey"]  # Select the remaining columns in their original order
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df_dimCustomer_with_surrogate_key)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import monotonically_increasing_id

#Create dimProduct
df_dimProduct = <ENTER HERE>



#Add surrogate key as the first column
df_dimProduct_with_surrogate_key = df_dimProduct.withColumn("ProductIDKey", monotonically_increasing_id())\
    .select(
        "ProductIDKey",  # Select the surrogate key column first
        *[column for column in df_dimProduct.columns if column != "ProductIDKey" and column!="spc.Name"]  # Select the remaining columns in their original order
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df_dimProduct_with_surrogate_key)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import expr
 
# Define the start and end dates for your DimDate table
start_date = "2000-01-01"
end_date = "2024-12-31"
 
# Create a DataFrame with a range of dates
df_dimDate = spark.range(0, (spark.sql("SELECT datediff('{0}', '{1}')".format(end_date, start_date)).collect()[0][0])+1) \
    .selectExpr("CAST(id AS INT) AS id") \
    .selectExpr("date_add('{0}', id) AS Date".format(start_date))
 
# Extract different date components
df_dimDate = df_dimDate \
    .withColumn("Year", expr("year(Date)")) \
    .withColumn("Month", expr("month(Date)")) \
    .withColumn("DayOfMonth", expr("dayofmonth(Date)")) \
    .withColumn("DayOfYear", expr("dayofyear(Date)")) \
    .withColumn("WeekOfYear", expr("weekofyear(Date)")) \
    .withColumn("DayOfWeek", expr("dayofweek(Date)")) \
    .withColumn("Quarter", expr("quarter(Date)"))

display(df_dimDate)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Write dataframes to Gold Layer as External Tables

# CELL ********************

# Write dimCustomer table into Gold Lakehouse as Delta Table
basePathGoldLakeHouse = "<ENTER HERE>"
tableName="dimCustomer"
df_dimCustomer_with_surrogate_key.write.mode("overwrite").format("delta").save(basePathGoldLakeHouse + '//' + tableName)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Write dimProduct table into Gold Lakehouse as Delta Table
tableName="dimProduct"
df_dimProduct_with_surrogate_key.write.mode("overwrite").format("delta").save(basePathGoldLakeHouse + '//' +tableName)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Create factSales
df_factSales = <ENTER HERE>
display(df_factSales)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Write dimDate table into Gold Lakehouse as Delta Table

tableName="dimDate"
df_dimDate.write.mode("overwrite").format("delta").save(basePathGoldLakeHouse + '//' + tableName)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Write factSales table into Gold Lakehouse as Delta Table

tableName="factSales"
df_factSales.write.mode("overwrite").format("delta").save(basePathGoldLakeHouse + '//' + tableName)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
