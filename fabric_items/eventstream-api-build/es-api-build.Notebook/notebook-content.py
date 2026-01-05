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

kv_uri = 'https://kvfabricprodeus2rh.vault.azure.net/'
client_id_secret = 'fuam-spn-client-id'
tenant_id_secret = 'fuam-spn-tenant-id'
client_secret_name = 'fuam-spn-secret'

workspace_id = 'a8cbda3d-903e-4154-97d9-9a91c95abb42'
es_id = '8b3a0aca-3545-4f66-bde2-1d019c997205'

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
import base64
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

def list_items_in_workspace(workspace_id:str, api_token:str):
    """
    Given a workspace, list all items in it
    """
    url = f'https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/items'

    headers = {
    "Authorization": f"Bearer {api_token}",
    "Content-Type": "application/json"
    }    

    response = requests.get(url, headers=headers)

    return response

def get_eventstream_definition(workspace_id:str, eventstream_id:str, api_token:str):
    """
    """
    url = f'https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/eventstreams/{eventstream_id}/getDefinition'

    headers = {
    "Authorization": f"Bearer {api_token}",
    "Content-Type": "application/json"
    }    

    response = requests.post(url, headers=headers)

    return response

def decode_eventstream_definition(encoded_data:dict)->dict:
    """
    Given an eventstream defintion (such as  the defintion.json from the return of the get_eventstream_defintion function), decode and 
    return the decoded json
    """
    payload = encoded_data['definition']['parts'][0]['payload']

    # decode the base64 string
    decoded = base64.b64decode(payload)

    # convery bytes to dict
    decoded_json = json.loads(decoded.decode('utf-8'))

    return decoded_json

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# client conn
# https://app.fabric.microsoft.com/groups/a8cbda3d-903e-4154-97d9-9a91c95abb42/eventstreams/8f9baedb-6735-47b2-a0e7-400256b284a3?experience=fabric-developer

token = get_api_token_via_akv(kv_uri, client_id_secret, tenant_id_secret, client_secret_name)

es_def = get_eventstream_definition(workspace_id, es_id, api_token=token)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

es_def.json()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

decoded_data = decode_eventstream_definition(es_def.json())

decoded_data

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# # Get All Items in Workspace 

# CELL ********************

ws_items_list = list_items_in_workspace(workspace_id, token)

ws_items_list.json()['value']

# get pipelines now (can add notebooks later by adding  or pl['type']=='Notebook')
pl_list = [pl for pl in ws_items_list.json()['value'] if pl['type']=='DataPipeline']


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

pl_list

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# # Compare the Items in the EventStream Defintion to the Items in the Workspace
# - If item in workspace and not in EventStream definition, add it to an add_to_eventstream_list list
# - This is because the workspace item list has already been filtered to include all the items that should be on the eventstream definition

# CELL ********************

decoded_data['sources']

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

es_item_list = decoded_data['sources']
es_item_list = [id['id'] for id in es_item_list]

es_item_list

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# for source in decoded_json['sources']:
#     print(source)
add_to_eventstream_list = []

for item in pl_list:
    check_item_id = item['id']

    # lookup check_item_id
    if check_item_id not in es_item_list:
        add_to_eventstream_list.append(check_item_id)
        print(f'Item Id: {check_item_id} is missing from the list and has been added to add_to_eventstream_list')


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

add_to_eventstream_list

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# # Update Eventstream Item Definition

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }
