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

# MARKDOWN ********************

# ## Data Ingestion Sales  Data Set
# We are going to load the Sales Data Set from a private Azure Storage Account.
# 
# <font size="2" color="red" face="sans-serif" bold> 
# 
# <b> <i> <u>No changes are required to this cell, This cell have all the necessary credentials to Ingest data from storage account
# </font>

# CELL ********************

from azure.identity import DefaultAzureCredential, ClientSecretCredential
from azure.keyvault.secrets import SecretClient
import notebookutils

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def get_api_token_via_akv(kv_uri:str, client_id_secret:str, tenant_id_secret:str, client_secret_name:str)->str:
    """
    Function to retrieve an api token used to authenticate with Microsoft Fabric APIs

    kv_uri:str: The uri of the azure key vault
    client_id_secret:str: The name of the key used to store the value for the client id in the akv
    tenant_id_secret:str: The name of the key used to store the value for the tenant id in the akv
    client_secret_name:str: The name of the key used to store the value for the client secret in the akv

    """
    client_id = notebookutils.credentials.getSecret(kv_uri, client_id_secret)
    tenant_id = notebookutils.credentials.getSecret(kv_uri, tenant_id_secret)
    client_secret = notebookutils.credentials.getSecret(kv_uri, client_secret_name)

    credential = ClientSecretCredential(tenant_id, client_id, client_secret)
    scope = 'https://analysis.windows.net/powerbi/api/.default'
    token = credential.get_token(scope).token

    return token

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

### get sas token from key vault
kv_uri = 'https://kvfabricprodeus2rh.vault.azure.net/'
client_id_secret = 'fuam-spn-client-id'
tenant_id_secret = 'fuam-spn-tenant-id'
client_secret_name = 'fuam-spn-secret'


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# get sas token
kv_uri = 'https://kvfabricprodeus2rh.vault.azure.net/'
secret_name = 'public-sa-sas'

sas_value = notebookutils.credentials.getSecret(kv_uri, secret_name)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# pull sales container
# Providing the details for the Azure Storage account
# Mention about the SAS key

storage_account = "adlswestusfab"
container = "sales"


sas_token = "sv=2024-11-04&ss=bfqt&srt=sco&sp=rwdlacupyx&se=2025-08-08T21:19:40Z&st=2025-08-03T13:04:40Z&spr=https&sig=48ArQ1zZyabF8pdf%2Fc1LHE%2B63dj5MSN%2F4OAFcZsfkng%3D" 

# Set Spark config to access  dfs storage
spark.conf.set("fs.azure.sas.%s.%s.dfs.core.windows.net" % (container, storage_account),sas_token)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# pull wwi-sample data
# Providing the details for the Azure Storage account
# Mention about the SAS key

storage_account = "adlswestusfab"
container = "wwi-sample"


sas_token = "sv=2024-11-04&ss=bfqt&srt=sco&sp=rwdlacupyx&se=2025-08-08T21:19:40Z&st=2025-08-03T13:04:40Z&spr=https&sig=48ArQ1zZyabF8pdf%2Fc1LHE%2B63dj5MSN%2F4OAFcZsfkng%3D" 

# Set Spark config to access  dfs storage
spark.conf.set("fs.azure.sas.%s.%s.dfs.core.windows.net" % (container, storage_account),sas_token)

abfss_path1 = f"abfss://{container}@{storage_account}.dfs.core.windows.net/"


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Providing the details for the Azure Storage account
# Mention about the SAS key
# sv=2024-11-04&ss=bf&srt=c&sp=rl&se=2025-09-04T20:57:43Z&st=2025-08-01T12:42:43Z&spr=https&sig=Cfe8rDObBLLws3fItrJZ4BlYdYhrSI5M5sT5Z8TKkK0%3D
storage_account = "publicdatasetssa"
container = "fabricdatafactorylab"


# sas_token = "sv=2024-11-04&ss=bf&srt=c&sp=rl&se=2025-09-04T20:57:43Z&st=2025-08-01T12:42:43Z&spr=https&sig=Cfe8rDObBLLws3fItrJZ4BlYdYhrSI5M5sT5Z8TKkK0%3D" 

# Set Spark config to access  dfs storage
spark.conf.set("fs.azure.sas.%s.%s.dfs.core.windows.net" % (container, storage_account),sas_value)

abfss_path1 = f"abfss://{container}@{storage_account}.dfs.core.windows.net/"

df = spark.read


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# We specify the path for the csv files we need to read. By reading the *.csv, we are reading all the files from the folder Facts
abfss_path1 = f"abfss://{container}@{storage_account}.dfs.core.windows.net/csv/Facts/Sales_File1.csv"
abfss_path2 = f"abfss://{container}@{storage_account}.dfs.core.windows.net/csv/Facts/Sales_File2.csv"

# We are specifying the file type, csv and displaying the headers of the sales files
sales_df1 = spark.read.format("csv").option("delimiter", ";").option("header","true").option("inferSchema", "true").load(abfss_path1)
sales_df2 = spark.read.format("csv").option("delimiter", ";").option("header","true").option("inferSchema", "true").load(abfss_path2)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# We are currently showcasing both data frames
display(sales_df2)
display(sales_df1)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# We print only the schema to better understand the dataframes data types structure

sales_df1.printSchema()
sales_df1.count()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# We print only the schema to better understand the dataframes data types structure. 
# As you can see these two dataframes have different columns. Hence a merge join is required.

sales_df2.printSchema()
sales_df2.count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# By ordering the data frames based on the columns that will be used for the merge join, we can significantly improve the efficiency of the join operation.
# In this case the column SalesOrderNumber
from pyspark.sql.functions import col

sorted_sales_df1 = sales_df1.orderBy(col("SalesOrderNumber"),col("SalesOrderLineNumber"))

display(sorted_sales_df1)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# By ordering the data frames based on the columns that will be used for the merge join, we can significantly improve the efficiency of the join operation.
# In this case the column SalesOrderNumber
from pyspark.sql.functions import col

sorted_sales_df2 = sales_df2.orderBy(col("SalesOrderNumber"),col("SalesOrderLineNumber"))
display(sorted_sales_df2)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# This cell contains an inner join operation on the 'SalesOrderNumber' and 'SalesOrderLineNumber' columns.

merged_df = sorted_sales_df1.join(
    sorted_sales_df2,
    on=["SalesOrderNumber", "SalesOrderLineNumber"],
    how="inner"
)

# Show the result
merged_df.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### Prior to providing a table name, we verify that no other table shares the same name.

# CELL ********************

# MAGIC %%sql
# MAGIC DROP TABLE IF EXISTS Sales;


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##  Sales DataSet ingestion onto a Managed Lakehouse Table
# 
# 
# <font size="2" color="red" face="sans-serif" bold> 
# 
# <b> <i> <u>
# </font>

# CELL ********************

# Writing the Data Frame directly into the Delta Table from Managed Zone
table_name = 'Sales'

merged_df \
    .write \
    .mode("overwrite") \
    .format("delta") \
    .save("Tables/" + table_name)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Now that the table has been created, we can utilize Pyspark SQL to generate a new data frame and load the table
table_name = "Sales"

# Read the table into a DataFrame
df = spark.read.table(table_name)

# Calculate the number of rows
row_count = df.count()

# Print or use the row count as needed
print("Number of rows:", row_count)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# To display the content of the table as dataframe we run the display command:
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
