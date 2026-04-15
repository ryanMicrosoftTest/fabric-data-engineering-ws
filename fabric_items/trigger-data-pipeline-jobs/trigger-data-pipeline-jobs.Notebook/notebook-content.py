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

# # Trigger Pipelines via Notebook
# The objective of this notebook is to have a means to trigger pipelines so this can be migrated to an Azure DevOps Pipeline where these pipelines can be trigger as a task after deployment

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

def run_on_demand_pipeline_job(workspace_id:str, item_id:str, api_token:str):
    """
    https://learn.microsoft.com/en-us/fabric/data-factory/pipeline-rest-api-capabilities
    POST
    https://api.fabric.microsoft.com/v1/workspaces/{workspaceId}/items/{itemId}/jobs/instances?jobType=Pipeline
    body
    {
    "executionData": {
        "pipelineName": "pipeline",
        "OwnerUserPrincipalName": "<user@domain.com>",
        "OwnerUserObjectId": "<Your ObjectId>"
    }
    }
    """
    url = f'https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/items/{item_id}/jobs/instances?jobType=Pipeline'

    headers = {
    "Authorization": f"Bearer {api_token}",
    "Content-Type": "application/json"
    }

    body = {
    "executionData": {
        "pipelineName": "pipeline",
        "OwnerUserPrincipalName": "<user@domain.com>",
        "OwnerUserObjectId": "<Your ObjectId>"
    }
    }

    response = requests.post(url, headers=headers, json=body)

    return response












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

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# # Client Code


# CELL ********************

# get oauth token
token = get_api_token_via_akv(kv_uri, client_id_secret, tenant_id_secret, client_secret_name)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# run pipeline
workspace_id = 'a8cbda3d-903e-4154-97d9-9a91c95abb42'
item_id = '27c03e8e-4f36-4018-936e-21b6d0e39cd5'

resp = run_on_demand_pipeline_job(workspace_id, item_id, token)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

resp

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }
