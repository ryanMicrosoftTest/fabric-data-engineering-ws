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

def replace_snake_with_kabob(inp_str:str)->str:
    """
    This will replace snake case characters _ with kabob case ones - and return a new string

    inp_str:str:  The original string

    returns:
        str
    """
    out_str = inp_str.replace('_', '-')

    return out_str

def replace_space_with_kabob(new_str:str):
    """
    Replace any whitespace in a string with -
    """
    new_str = new_str.replace(' ','-')

    return new_str

def update_eventstream_definition(workspace_id:str, eventstream_id:str, eventstream_definition:dict, api_token:str):
    """
    NOTE: For now, assume other functions prepare this and this will be the function that sends the update

    Update a given eventstream definition.  This overrides the current definition.  This API supports LRO
    https://learn.microsoft.com/en-us/rest/api/fabric/eventstream/items/update-eventstream-definition?tabs=HTTP

    workspace_id:str: The guid of the workspace where the eventstream resides
    eventstream_id:str:  The guid of the eventstream to be updated
    eventstream_definition:json:  The full base64 definition to be updated
    api_token:str:  The token to authenticate with the API server

    Example:POST https://api.fabric.microsoft.com/v1/workspaces/cfafbeb1-8037-4d0c-896e-a46fb27ff229/eventstreams/5b218778-e7a5-4d73-8187-f10824047715/updateDefinition?updateMetadata=True

    {
    "definition": {
        "parts": [
        {
            "path": "eventstream.json",
            "payload": "SSdkIGxpa2UgdG8gdGVsbCBh..IGpva2UgZm9yIHlvdS4K",
            "payloadType": "InlineBase64"
        },
        {
            "path": "eventstreamProperties.json",
            "payload": "ewogICJyZXRlbnRpb25UaW1l..V2ZWwiOiAiTG93Igp9",
            "payloadType": "InlineBase64"
        },
        {
            "path": ".platform",
            "payload": "ZG90UGxhdGZvcm1CYXNlNjRTdHJpbmc=",
            "payloadType": "InlineBase64"
        }
        ]
    }
    }
    """
    url = f'https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/eventstreams/{eventstream_id}/updateDefinition'

    headers = {
    "Authorization": f"Bearer {api_token}",
    "Content-Type": "application/json"
    }    

    response = requests.post(url, headers=headers, json=eventstream_definition)

    return response

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

# capture decoded data sources as missing sources will be added to it
decoded_es_sources_list = decoded_data['sources']

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

es_item_list = decoded_data['sources']
es_item_list = [id['properties']['itemId'] for id in es_item_list]

es_item_list

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

es_item_list

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# for source in decoded_json['sources']:
#     print(source)
add_to_eventstream_id_list = []
add_to_eventstream_name_list = []

for item in pl_list:
    check_item_id = item['id']
    check_item_name = item['displayName']

    # lookup check_item_id
    if check_item_id not in es_item_list:
        add_to_eventstream_id_list.append(check_item_id)
        add_to_eventstream_name_list.append(check_item_name)
        print(f'Item Id: {check_item_id} is missing from the list and has been added to add_to_eventstream_list')


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# # Update Eventstream Item Definition

# CELL ********************

# preprocess add_to_eventstream_name_list to replace any _ with -
add_to_eventstream_name_cleaned_list = []

for _ in add_to_eventstream_name_list:
    new_str = replace_snake_with_kabob(_)
    new_str = replace_space_with_kabob(new_str)
    add_to_eventstream_name_cleaned_list.append(new_str)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# use add_to_eventstream_id_list and add_to_eventstream_name_cleaned_list to create a dict to append to sources list

for idx, _ in enumerate(add_to_eventstream_id_list):
    sources_add = {
    'id': _,  
    'name': f'{add_to_eventstream_name_cleaned_list[idx]}-job',
    'type': 'FabricJobEvents',
    'properties': {
        'eventScope': 'Item',
        'workspaceId': 'a8cbda3d-903e-4154-97d9-9a91c95abb42',
        'itemId': '723b3cff-ebcf-4861-8286-bf5a36e22f3c',
        'includedEventTypes': [
            'Microsoft.Fabric.JobEvents.ItemJobCreated',
            'Microsoft.Fabric.JobEvents.ItemJobStatusChanged',
            'Microsoft.Fabric.JobEvents.ItemJobSucceeded',
            'Microsoft.Fabric.JobEvents.ItemJobFailed',
        ],
        'filters': [],
    }
    }

    # append to decoded_es_sources_list
    decoded_es_sources_list.append(sources_add)

decoded_es_sources_list

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# # Encode the eventstream.json data to base64

# CELL ********************

# update to the new list of sources
decoded_data['sources'] = decoded_es_sources_list

decoded_data

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

decoded_data['sources']

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

new_encoded_json = json.dumps(decoded_data, separators=(",", ":"), sort_keys=True)
new_encoded_bytes = new_encoded_json.encode('utf-8')
b64_new_sources = base64.b64encode(new_encoded_bytes).decode('utf-8')

print(b64_new_sources)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# # Consolidate the data EventStream Definition

# CELL ********************

es_def.json()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

new_definition = es_def.json()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# update the eventstream.json file which will be the first file
new_definition['definition']['parts'][0]['payload'] = b64_new_sources

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

new_definition

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# # Send the Update to the API

# CELL ********************

workspace_id = 'a8cbda3d-903e-4154-97d9-9a91c95abb42'
eventstream_id = '8b3a0aca-3545-4f66-bde2-1d019c997205'

update_resp = update_eventstream_definition(workspace_id, eventstream_id, new_definition, token)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

update_resp

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

update_resp.json()

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
