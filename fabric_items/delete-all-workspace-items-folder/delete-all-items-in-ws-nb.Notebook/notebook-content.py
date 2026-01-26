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

# This note book aims to delete all items from a workspace.  It may get rejections from dependencies (current logic can't figure out what the dependencies are because it isn't provided in the rejection response from the delete items API)

# CELL ********************

import requests
from azure.identity import DefaultAzureCredential, ClientSecretCredential
from azure.keyvault.secrets import SecretClient
import os
import notebookutils
import pandas as pd

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


def get_all_items_in_workspace(workspace_id:str, api_token:str):
    """
    Get all items in workspace

    GET https://api.fabric.microsoft.com/v1/workspaces/{workspaceId}/items
    """
    url = f'https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/items'

    headers = {
    "Authorization": f"Bearer {api_token}",
    "Content-Type": "application/json"
    }    

    response = requests.get(url, headers=headers)

    return response



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

def delete_all_items_in_workspace(workspace_id:str, item_id:str, api_token:str):
    """
    Delete all items in workspace
    https://learn.microsoft.com/en-us/rest/api/fabric/core/items/delete-item?tabs=HTTP

    DELETE https://api.fabric.microsoft.com/v1/workspaces/{workspaceId}/items/{itemId}
    """
    url = f'https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/items/{item_id}'

    headers = {
    "Authorization": f"Bearer {api_token}",
    "Content-Type": "application/json"
    }    

    print(f'INFO: Deleting item_id: {item_id} from workspace: {workspace_id}')
    response = requests.delete(url, headers=headers)

    print(f'Result from delete attempt: {response.status_code}')
    if response.status_code == 401:
        print(response.json())

    return response

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# client cxn

# get token
token = get_api_token_via_akv(kv_uri, client_id_secret, tenant_id_secret, client_secret_name)


# get all items in workspace
workspace_id = '7afc490e-115f-472c-a205-17dc6a5bee52'
resp = get_all_items_in_workspace(workspace_id, token)

resp.content

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

workspace_items_df = pd.DataFrame(resp.json()['value'])

workspace_items_df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# delete all items in workspace
ws_items_list = workspace_items_df['id'].tolist()

for _ in ws_items_list:
    # delete ws item
    del_resp = delete_all_items_in_workspace(workspace_id, _, token)


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
