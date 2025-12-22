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

data = """{
  "properties": {
    "activities": [
      {
        "type": "InvokePipeline",
        "typeProperties": {
          "parameters": {},
          "waitOnCompletion": true,
          "workspaceId": "00000000-0000-0000-0000-000000000000",
          "pipelineId": "2c67a4d9-9049-bfdc-4483-620f9f6819a2",
          "operationType": "InvokeFabricPipeline"
        },
        "externalReferences": {
          "connection": "712d9a73-e5e3-4d3a-a58d-56bc433c1158"
        },
        "policy": {
          "timeout": "0.12:00:00",
          "retry": 0,
          "retryIntervalInSeconds": 30,
          "secureInput": false,
          "secureOutput": false
        },
        "name": "Invoke Metadata Table Pipeline",
        "dependsOn": []
      },
      {
        "type": "InvokePipeline",
        "typeProperties": {
          "parameters": {},
          "waitOnCompletion": true,
          "workspaceId": "00000000-0000-0000-0000-000000000000",
          "pipelineId": "d0e39cd5-21b6-936e-4018-4f3627c03e8e",
          "operationType": "InvokeFabricPipeline"
        },
        "externalReferences": {
          "connection": "712d9a73-e5e3-4d3a-a58d-56bc433c1158"
        },
        "policy": {
          "timeout": "0.12:00:00",
          "retry": 0,
          "retryIntervalInSeconds": 30,
          "secureInput": false,
          "secureOutput": false
        },
        "name": "Invoke Create Shortcut Pipeline",
        "dependsOn": [
          {
            "activity": "Invoke Metadata Table Pipeline",
            "dependencyConditions": [
              "Succeeded"
            ]
          }
        ]
      }
    ]
  }
}"""

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

import json

json_def = json.loads(data)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

json_def

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

json_def['properties']['activities']

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

type(json_def['properties']['activities'][0]['externalReferences'])

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

workspace_id = 'a046cf0f-8dca-4b61-b95e-7adf68fb4b0a'
dataset_id = '708da792-a344-4079-b205-61c587a51600'


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


def get_dataset_refresh_info(workspace_id:str, dataset_id:str, api_token:str)->pd.DataFrame:
    """
    https://learn.microsoft.com/en-us/rest/api/power-bi/datasets/get-refresh-history-in-group
    scopes required: Dataset.ReadWrite.All or Dataset.Read.All

    GET https://api.powerbi.com/v1.0/myorg/groups/{groupId}/datasets/{datasetId}/refreshes

    workspace_id:str: The Workspace ID where the semantic model/dataset resides
    dataset_id:str: The Dataset ID to get refresh info for
    api_token:str: The api token to authenticate with the API

    returns:
        refresh_history_pd_df:pd.DataFrame: DataFrame of the refresh history
    """
    url = f'https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/datasets/{dataset_id}/refreshes'

    headers = {
    "Authorization": f"Bearer {api_token}",
    "Content-Type": "application/json"
    }    

    response = requests.get(url, headers=headers)

    return pd.DataFrame(response.json()['value'])

def get_data_pipeline(workspace_id:str, data_pipeline_id:str, api_token:str):
    """
    Returns the specified data pipeline public definition.
    This API supports long running operations (LRO).

    When you get a DataPipeline's public definition, the sensitivity label is not a part of the definition.
    Note this only works if the pipeline does not have a protected label
    
    https://learn.microsoft.com/en-us/rest/api/fabric/datapipeline/items/get-data-pipeline-definition?tabs=HTTP
    POST https://api.fabric.microsoft.com/v1/workspaces/{workspaceId}/dataPipelines/{dataPipelineId}/getDefinition

    args:
        workspace_id:str: The guid of the workspace
        data_pipeline_id:str: The guid of the data pipeline
        api_token:str: The api token to authenticate with the API
    returns:
        response
    """
    url = f'https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/dataPipelines/{data_pipeline_id}/getDefinition'

    headers = {
    "Authorization": f"Bearer {api_token}",
    "Content-Type": "application/json"
    }    

    response = requests.post(url, headers=headers)

    return response

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# get oauth token
token = get_api_token_via_akv(kv_uri, client_id_secret, tenant_id_secret, client_secret_name)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# get pipeline definition
workspace_id = 'a8cbda3d-903e-4154-97d9-9a91c95abb42'
data_pipeline_id = '449fa92f-efbf-468e-a86d-c52c94682ea2'

resp = get_data_pipeline(workspace_id=workspace_id, data_pipeline_id=data_pipeline_id, api_token=token)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

resp.json()

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
