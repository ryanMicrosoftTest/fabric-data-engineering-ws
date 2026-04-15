# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "jupyter",
# META     "jupyter_kernel_name": "python3.11"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

%pip install deltalake

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

import pandas as pd
from deltalake import DeltaTable
import requests
from azure.identity import DefaultAzureCredential, ClientSecretCredential
from azure.keyvault.secrets import SecretClient


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

df = pd.read_parquet('abfss://a8cbda3d-903e-4154-97d9-9a91c95abb42@onelake.dfs.fabric.microsoft.com/a1e15297-7fc9-41fa-89b4-b72ba14ab3c4/Tables/insider_transactions')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

from deltalake import DeltaTable

dt = DeltaTable(
    "abfss://a8cbda3d-903e-4154-97d9-9a91c95abb42@onelake.dfs.fabric.microsoft.com/a1e15297-7fc9-41fa-89b4-b72ba14ab3c4/Tables/insider_transactions",
    storage_options={
        "bearer_token": access_token,      
        "use_fabric_endpoint": "true"
    }
)

df = dt.to_pandas()
df.head()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

df = pd.read_parquet('https://onelake.dfs.fabric.microsoft.com/a8cbda3d-903e-4154-97d9-9a91c95abb42/a1e15297-7fc9-41fa-89b4-b72ba14ab3c4/Tables/insider_transactions')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

kv_uri = 'https://kvfabricprodeus2rh.vault.azure.net/'
client_id_secret = 'fuam-spn-client-id'
tenant_id_secret = 'fuam-spn-tenant-id'
client_secret_name = 'fuam-spn-secret'


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
    scope = 'https://onelake.fabric.microsoft.com/.default'
    token = credential.get_token(scope).token

    return token

### get token
token = get_api_token_via_akv(kv_uri, client_id_secret, tenant_id_secret, client_secret_name)

import pandas as pd

# GUID-based ABFS path (keep the .lakehouse segment)
path = 'https://onelake.dfs.fabric.microsoft.com/a8cbda3d-903e-4154-97d9-9a91c95abb42/a1e15297-7fc9-41fa-89b4-b72ba14ab3c4/Tables/insider_transactions'

dt = DeltaTable(
    path,
    storage_options={
        "bearer_token": token,
        "use_fabric_endpoint": "false"
    }
)
df = dt.to_pandas()

df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }
