# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "07a81854-b712-44b4-8a59-5a021c2f4d32",
# META       "default_lakehouse_name": "bronze_lh",
# META       "default_lakehouse_workspace_id": "a8cbda3d-903e-4154-97d9-9a91c95abb42",
# META       "known_lakehouses": [
# META         {
# META           "id": "07a81854-b712-44b4-8a59-5a021c2f4d32"
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

class Silver_Utils():
    """
    Utilities class for writing tables to silver in the Medallion Layer

    Silver is a layer aimed at Filtered, Cleaned, Augmented Data
    It requires a balance between reads and writes for data transformations and cleansing based on the dependency of data serving layers
    """

    def write_table_to_silver():
        """
        Write a table to the silver layer 
        """
        spark.conf.set("spark.sql.parquet.vorder.enabled", "true")
        spark.conf.set("spark.databricks.delta.optimizeWrite.enabled","true")
        



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from notebookutils import mssparkutils
adventureWorksPath = "abfss://a8cbda3d-903e-4154-97d9-9a91c95abb42@onelake.dfs.fabric.microsoft.com/07a81854-b712-44b4-8a59-5a021c2f4d32/Files"

# list of file names to ignore in processing
ignore_list = ['dbo_ErrorLog', 'airline_1', 'airline', 'dbo_BuildVersion']

file_list = mssparkutils.fs.ls(adventureWorksPath)

# Read each file and create a DataFrame
for file_path in file_list:
    print(file_path)
    if file_path.name in ignore_list:
        continue
    
    df = spark.read.format("csv").options(inferSchema="true", header="true").load(path=f"{file_path.path}*")
    # You can process the DataFrame or register it as a table here
    # For example, to create a temporary table:
    df.createOrReplaceTempView(file_path.name.removesuffix('.csv'))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC <ENTER HERE>

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC <ENTER HERE>

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

views =  <ENTER HERE>
display(views)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Read Temp Views, clean the data, load into Spark Dataframes
# - Filter Rows
# - Rename Columns
# - Drop Columns

# CELL ********************

df_salesltsalesorderdetail = spark.sql("SELECT SalesOrderID, OrderQty, ProductID,UnitPrice, UnitPriceDIscount, LineTotal FROM salesltsalesorderdetail")
display(df_salesltsalesorderdetail)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_salesltsalesorderheader = spark.sql("SELECT SalesOrderID, RevisionNumber, OrderDate, DueDate, ShipDate, Status, OnlineOrderFlag, SalesOrderNumber, PurchaseOrderNumber, AccountNumber, CustomerID, ShipToAddressID, BillToAddressID, ShipMethod, SubTotal, TaxAmt, Freight, TotalDue FROM salesltsalesorderheader")
display(df_salesltsalesorderheader)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Randomizing the dates in the OrderDate column since our toy AdventureWorks LT dataset only has one distinct order date.
from pyspark.sql.functions import rand, col, expr
df_salesltsalesorderheader = df_salesltsalesorderheader.drop("OrderDate").withColumn("OrderDate", expr("date_add(current_date()-1000, CAST(rand() * 365 AS INT))"))
display(df_salesltsalesorderheader)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_salesltcustomer = spark.sql("SELECT CustomerID, Title, FirstName, MiddleName, LastName, Suffix, CompanyName, EmailAddress, Phone  FROM salesltcustomer")
display(df_salesltcustomer)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_salesltcustomeraddress = spark.sql("SELECT CustomerID, AddressID, AddressType FROM salesltcustomeraddress")
display(df_salesltcustomeraddress)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_salesltproduct = spark.sql("SELECT ProductID, Name, ProductNumber, Color, StandardCost, ListPrice, Size, Weight, ProductCategoryID FROM salesltproduct")
display(df_salesltproduct)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_salesltproductcategory = spark.sql("SELECT ProductCategoryID, ParentProductCategoryID, Name FROM salesltproductcategory")
display(df_salesltproductcategory)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Write Data Frames into Silver Lakehouse as Delta Tables

# CELL ********************

basePathSilverLakeHouse = "<ENTER HERE>"
tableName="salesOrderHeader"
df_salesltsalesorderheader.write.mode("overwrite").format("delta").save(basePathSilverLakeHouse + '//' + tableName)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

tableName="salesOrderDetail"
df_salesltsalesorderdetail.write.mode("overwrite").format("delta").save(basePathSilverLakeHouse + '//' + tableName)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

tableName="salesCustomer"
df_salesltcustomer.write.mode("overwrite").format("delta").save(basePathSilverLakeHouse + '//' + tableName)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

tableName="salesCustomerAddress"
df_salesltcustomeraddress.write.mode("overwrite").format("delta").save(basePathSilverLakeHouse + '//' + tableName)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

tableName="salesProduct"
df_salesltproduct.write.mode("overwrite").format("delta").save(basePathSilverLakeHouse + '//' + tableName)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

tableName="salesProductCategory"
df_salesltproductcategory.write.mode("overwrite").format("delta").save(basePathSilverLakeHouse + '//' + tableName)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
