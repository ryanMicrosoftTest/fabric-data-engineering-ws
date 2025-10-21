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

# ![image-alt-text](image-URL)# Restore Points Notebook
# The objective of this notebook is to setup restore points for a Fabric Data Warehouse via API <br>
# 
# Objectives
# - Parameter to choose Data Warehouse to create a restore point in
# - Create Restore Point (this can be called from a pipeline to schedule restore point creations)
# - List Restore Points
# - Restore to Restore Point
# 
# Requirements
# - SPN or caller must have Warehouse.ReadWrite.All or Item.ReadWrite.All Scope
# - Must have Write permission on the target warehouse

# CELL ********************

kv_uri = 'https://kvfabricprodeus2rh.vault.azure.net/'
client_id_secret = 'fuam-spn-client-id'
tenant_id_secret = 'fuam-spn-tenant-id'
client_secret_name = 'fuam-spn-secret'

workspace_id = 'a8cbda3d-903e-4154-97d9-9a91c95abb42'
warehouse_id = 'faf57c49-8a74-49aa-8e3b-8ccb7390d403'

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
import json

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

def create_restore_point(api_token:str, workspace_id:str, warehouse_id:str, display_name:str, description='Description'):
    """
    https://learn.microsoft.com/en-us/rest/api/fabric/warehouse/restore-points/create-restore-point?tabs=HTTP

    api_token:str: OAuth token to authenticate with the API
    workspace_id:str: The guid of the workspace where the warehouse resides
    warehouse_id:str: The guid of the warehouse to create a restore point in
    display_name:str: The display name of the restore point created
    description:str: The description of the restore point

    returns:
        response
    """
    url = f'https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/warehouses/{warehouse_id}/restorePoints'

    headers = {
    "Authorization": f"Bearer {api_token}",
    "Content-Type": "application/json"
    }

    body = {
        "displayName": display_name,
        "description": description
    }

    response = requests.post(url=url, headers=headers, json=body)

    if response.status_code >= 200 and response.status_code < 300:
        print(f'Restore Point Created Successfully')
        return response
    else:
        print(f'ERROR: {response}')


def list_restore_points(api_token:str, workspace_id:str, warehouse_id:str)->dict:
    """
    https://learn.microsoft.com/en-us/rest/api/fabric/warehouse/restore-points/list-restore-points?tabs=HTTP

    api_token:str: OAuth token to authenticate with the API
    workspace_id:str: The guid of the workspace where the warehouse resides
    warehouse_id:str: The guid of the warehouse to create a restore point in

    returns:
        response
    """
    url = f'https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/warehouses/{warehouse_id}/restorePoints'

    headers = {
    "Authorization": f"Bearer {api_token}",
    "Content-Type": "application/json"
    }

    response = requests.get(url=url, headers=headers)

    if response.status_code >= 200 and response.status_code < 300:
        return response.json()
    else:
        print(f'ERROR: {response}')
        return response


def restore_to_restore_point(api_token:str, workspace_id:str, warehouse_id:str, restore_point_id:str):
    """
    https://learn.microsoft.com/en-us/rest/api/fabric/warehouse/restore-points/restore-to-restore-point?tabs=HTTP

    api_token:str: OAuth token to authenticate with the API
    workspace_id:str: The guid of the workspace where the warehouse resides
    warehouse_id:str: The guid of the warehouse to create a restore point in
    restore_point_id:str: The restore id to restore to

    returns
    
    """
    url = f'https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/warehouses/{warehouse_id}/restorePoints/{restore_point_id}/restore'

    headers = {
    "Authorization": f"Bearer {api_token}",
    "Content-Type": "application/json"
    }

    response = requests.post(url=url, headers=headers)

    if response.status_code >= 200 and response.status_code < 300:
        print(f'INFO: Restore Point restored successfully')
        return response.json()
    else:
        print(f'ERROR: {response}')
        return response



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# get oauth token
token = get_api_token_via_akv(kv_uri, client_id_secret, tenant_id_secret, client_secret_name)

# create restore point
create_rp = create_restore_point(token, workspace_id, warehouse_id, display_name='see create time', description='Test for restore point')

# get restore point
resp = list_restore_points(token, workspace_id, warehouse_id)

create_rp

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

df = pd.DataFrame(resp['value'])

# filter for just the UserDefined ones
df = df[df['creationMode']=='UserDefined']


df['creationDetails'] = df['creationDetails'].apply(
    lambda x: x if isinstance(x, dict) else json.loads(str(x))
)

creationDetails_col_map = {
    'creationDetails_eventDateTime': ['creationDetails','eventDateTime'],
    'eventInitiator_id': ['creationDetails', 'id'],
    'eventInitiator_type': ['creationDetails', 'type']

}

for new_col, item in creationDetails_col_map.items():
    if new_col == 'creationDetails_eventDateTime':
        df[new_col] = df[item[0]].apply(lambda x: x.get(item[1]))
    else:
        df[new_col] = df[item[0]].apply(lambda x: x.get('eventInitiator',{}).get(item[1]))

display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

restore_resp = restore_to_restore_point(token, workspace_id, warehouse_id, restore_point_id='1760729644000')

restore_resp

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }
