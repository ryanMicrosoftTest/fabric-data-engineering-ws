# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "jupyter",
# META     "jupyter_kernel_name": "python3.11"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# # How to get the Event History File
#   Download Event Log via REST API
# 
#    GET https://api.fabric.microsoft.com/v1/workspaces/{workspaceId}/notebooks/{itemId}/livySessions/{livyId}/applications/{appId}/{attemptId}/logs
# 
#   Step-by-step:
# 
#    1. Find your Spark application — use the workspace-level API to list Livy sessions: GET https://api.fabric.microsoft.com/v1/workspaces/{workspaceId}/notebooks/{itemId}/livySessions
# 
#    This gives you the livyId and appId (e.g., application_1742369571479_0001).
#    2. Download the event log: GET .../livySessions/{livyId}/applications/{appId}/1/logs
# 
#    This returns the Spark event history file that you can feed directly into Sparklens.
#    3. Run Sparklens against it: spark-submit --packages qubole:sparklens:0.3.2-s_2.11 \
#       --class com.qubole.sparklens.app.ReporterApp \
#       qubole-dummy-arg <downloaded-event-log-file> source=history
# 
# 
# The SparkLens utilization will be handled in a different notebook

# CELL ********************

kv_uri = 'https://kvfabricprodeus2rh.vault.azure.net/'
client_id_secret = 'fuam-spn-client-id'
tenant_id_secret = 'fuam-spn-tenant-id'
client_secret_name = 'fuam-spn-secret'

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

import requests
from azure.identity import DefaultAzureCredential, ClientSecretCredential
from azure.keyvault.secrets import SecretClient
import os
import notebookutils
import pandas as pd
import zipfile
import io


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
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



def get_spark_notebook_event_log(workspace_id:str, item_id:str, livy_id:str, app_id:str, api_token:str):
    """
    https://api.fabric.microsoft.com/v1/workspaces/{workspaceId}/notebooks/{itemId}/livySessions/{livyId}/applications/{appId}/1/logs
    """
    url = f'https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/notebooks/{item_id}/livySessions/{livy_id}/applications/{app_id}/1/logs'

    headers = {
    "Authorization": f"Bearer {api_token}",
    "Content-Type": "application/json"
    }    

    response = requests.get(url, headers=headers)

    return response

def _unzip_spark_event_log(spark_event_content:bytes):
    """
    This is used to unzip the content received from the get_spark_event_log API and save to a location
    """
    with zipfile.ZipFile(io.BytesIO(spark_event_content)) as z:
        for name in z.namelist():
            with z.open(name) as f:
                content = f.read()

                # Write to lakehouse Files
                output_path = f"/lakehouse/default/Files/spark_logs/{name}"
                with open(output_path, "wb") as out:
                    out.write(content)

                print(f"Written to {output_path} ({len(content)} bytes)")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

type(resp.content)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

"""
WORKSPACE_ID a8cbda3d-903e-4154-97d9-9a91c95abb42
ITEM_ID 08eb76df-a61b-4f66-9e5a-5eaf1046cfc9
LIVY ID 48f25100-1a57-44ea-9344-ee3c769f06ea
APP ID application_1771612052121_0001
"""
# get token
token = get_api_token_via_akv(kv_uri, client_id_secret, tenant_id_secret, client_secret_name)


# get log
workspace_id = 'a8cbda3d-903e-4154-97d9-9a91c95abb42'
item_id = '08eb76df-a61b-4f66-9e5a-5eaf1046cfc9'
livy_id = '48f25100-1a57-44ea-9344-ee3c769f06ea'
app_id = 'application_1771612052121_0001'

resp = get_spark_notebook_event_log(workspace_id, item_id, livy_id, app_id, token)

unzipped_contents = _unzip_spark_event_log(resp.content)

unzipped_contents

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
