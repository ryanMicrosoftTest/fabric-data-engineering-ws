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

# ### Notebook for Creating Custom Pools
# - This will later be incorporated into a CICD process for deploying custom environments

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

def _valid_node_sizes(node_size:str):
    node_size = node_size.lower()

    if node_size not in ('small', 'medium', 'large', 'xlarge', 'xxlarge'):
        raise ValueError(f"Invalid node size {node_size}.  Must be one of 'small', 'medium', 'large', 'xlarge', 'xxlarge'")

def _valid_autoscale(autoscale_dict:dict):
    if autoscale_dict['enabled'] not in ('true', 'false'):
        raise ValueError(f'Autoscale must be one of true or false, got: {autoscale_dict["enabled"]}')
    if autoscale_dict['minNodeCount'] > autoscale_dict['maxNodeCount']:
        raise ValueError(f'Autoscale minNodeCount must be less than maxNodeCount, got: {autoscale_dict}')

def _valid_dynamicExecutorAllocation(dynamicExecutorAllocation_dict:dict):
    if dynamicExecutorAllocation_dict['enabled'] not in ('true', 'false'):
        raise ValueError(f'dynamicExecutorAllocation must be one of true or false, got: {dynamicExecutorAllocation_dict["enabled"]}')
    if dynamicExecutorAllocation_dict['minExecutors'] > dynamicExecutorAllocation_dict['maxExecutors']:
        raise ValueError(f'dynamicExecutorAllocation minExecutors must be less than maxExecutors, got: {dynamicExecutorAllocation_dict}')

def create_custom_pool(workspace_id:str, api_token:str, pool_name:str, node_size:str, autoscale_dict:dict, dynamicExecutorAllocation_dict:dict):
    """
    Create Workspace Custom Pool
    https://learn.microsoft.com/en-us/rest/api/fabric/spark/custom-pools/create-workspace-custom-pool?tabs=HTTP
    POST https://api.fabric.microsoft.com/v1/workspaces/{workspaceId}/spark/pools

    args:
        workspace_id:str: The guid of the workspace
        api_token:str: The api token to authenticate with the API

    """
    # validations
    _valid_node_sizes(node_size)
    _valid_autoscale(autoscale_dict)
    _valid_dynamicExecutorAllocation(dynamicExecutorAllocation_dict)

    url = f'https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/spark/pools'

    headers = {
    "Authorization": f"Bearer {api_token}",
    "Content-Type": "application/json"
    }

    pool_payload = {
        "name": f"{pool_name}",
        "nodeFamily": "MemoryOptimized",
        "nodeSize": f"{node_size}",
        "autoScale": autoscale_dict,
        "dynamicExecutorAllocation": dynamicExecutorAllocation_dict
        }

    response = requests.post(url, headers=headers, json=pool_payload)

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

workspace_id = 'a8cbda3d-903e-4154-97d9-9a91c95abb42'
pool_name = 'test_custom_deploy_pool'
node_size = 'small'
autoscale_dict = {
    "enabled": "true",
    "minNodeCount": 1,
    "maxNodeCount": 2
  }
dynamicExecutorAllocation_dict = {
    "enabled": "true",
    "minExecutors": 1,
    "maxExecutors": 1
  }

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

#  test pool creation
resp = create_custom_pool(workspace_id=workspace_id, api_token=token, pool_name=pool_name, node_size=node_size, autoscale_dict=autoscale_dict, dynamicExecutorAllocation_dict=dynamicExecutorAllocation_dict)

resp.content

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

# test as class

import requests


class CustomPools:
    @staticmethod
    def _valid_node_sizes(node_size:str):
        node_size = node_size.lower()

        if node_size not in ('small', 'medium', 'large', 'xlarge', 'xxlarge'):
            raise ValueError(f"Invalid node size {node_size}.  Must be one of 'small', 'medium', 'large', 'xlarge', 'xxlarge'")

        node_map = {
            'small': 'Small',
            'medium': 'Medium',
            'large': 'Large',
            'xlarge': 'XLarge',
            'xxlarge': 'XXLarge'
        }

        return node_map[node_size]
    
    @staticmethod
    def _valid_autoscale(autoscale_dict:dict):
        if autoscale_dict['enabled'] not in ('true', 'false'):
            raise ValueError(f'Autoscale must be one of true or false, got: {autoscale_dict["enabled"]}')
        if autoscale_dict['minNodeCount'] > autoscale_dict['maxNodeCount']:
            raise ValueError(f'Autoscale minNodeCount must be less than maxNodeCount, got: {autoscale_dict}')

    @staticmethod
    def _valid_dynamicExecutorAllocation(dynamicExecutorAllocation_dict:dict):
        if dynamicExecutorAllocation_dict['enabled'] not in ('true', 'false'):
            raise ValueError(f'dynamicExecutorAllocation must be one of true or false, got: {dynamicExecutorAllocation_dict["enabled"]}')
        if dynamicExecutorAllocation_dict['minExecutors'] > dynamicExecutorAllocation_dict['maxExecutors']:
            raise ValueError(f'dynamicExecutorAllocation minExecutors must be less than maxExecutors, got: {dynamicExecutorAllocation_dict}')

    @staticmethod
    def create_custom_pool(workspace_id:str, api_token:str, pool_name:str, node_size:str, autoscale_dict:dict, dynamicExecutorAllocation_dict:dict):
        """
        Create Workspace Custom Pool
        https://learn.microsoft.com/en-us/rest/api/fabric/spark/custom-pools/create-workspace-custom-pool?tabs=HTTP
        POST https://api.fabric.microsoft.com/v1/workspaces/{workspaceId}/spark/pools

        args:
            workspace_id:str: The guid of the workspace
            api_token:str: The api token to authenticate with the API

        """
        # validations
        node_size = CustomPools._valid_node_sizes(node_size)
        CustomPools._valid_autoscale(autoscale_dict)
        CustomPools._valid_dynamicExecutorAllocation(dynamicExecutorAllocation_dict)

        url = f'https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/spark/pools'

        headers = {
        "Authorization": f"Bearer {api_token}",
        "Content-Type": "application/json"
        }

        pool_payload = {
            "name": f"{pool_name}",
            "nodeFamily": "MemoryOptimized",
            "nodeSize": f"{node_size}",
            "autoScale": autoscale_dict,
            "dynamicExecutorAllocation": dynamicExecutorAllocation_dict
            }

        response = requests.post(url, headers=headers, json=pool_payload)

        return response

    @staticmethod
    def get_custom_pool(workspace_id:str, pool_id:str, api_token:str):
        """
        Get custom pool
        https://learn.microsoft.com/en-us/rest/api/fabric/spark/custom-pools/get-workspace-custom-pool?tabs=HTTP
        GET https://api.fabric.microsoft.com/v1/workspaces/{workspaceId}/spark/pools/{poolId}

        args:
            workspace_id:str: The guid of the workspace
            pool_id:str:  The guid of the pool
            api_token:str: The token used to authenticate

        """
        url = f'https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/spark/pools/{pool_id}'

        headers = {
        "Authorization": f"Bearer {api_token}",
        "Content-Type": "application/json"
        }


        response = requests.get(url, headers=headers)

        return response

    @staticmethod
    def delete_custom_pool(workspace_id:str, pool_id:str, api_token:str):
        """
        Delete custom pool
        https://learn.microsoft.com/en-us/rest/api/fabric/spark/custom-pools/delete-workspace-custom-pool?tabs=HTTP
        DELETE https://api.fabric.microsoft.com/v1/workspaces/{workspaceId}/spark/pools/{poolId}

        args:
            workspace_id:str: The guid of the workspace
            pool_id:str: The guid of the pool
            api_token:str: The token used to authenticate

        """
        url = f'https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/spark/pools/{pool_id}'

        headers = {
        "Authorization": f"Bearer {api_token}",
        "Content-Type": "application/json"
        }

        response = requests.delete(url, headers=headers)

        if response.status_code==200:
            print(f'INFO: Pool with id: {pool_id} successfully deleted')

        return response
    
    @staticmethod
    def list_custom_pools(workspace_id:str, api_token:str):
        """
        List custom pools
        https://learn.microsoft.com/en-us/rest/api/fabric/spark/custom-pools/list-workspace-custom-pools?tabs=HTTP
        GET https://api.fabric.microsoft.com/v1/workspaces/{workspaceId}/spark/pools

        args:
            workspace_id:str: The guid of the workspace
            api_token:str: The token used to authenticate
        """
        url = f'https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/spark/pools'

        headers = {
        "Authorization": f"Bearer {api_token}",
        "Content-Type": "application/json"
        }

        response = requests.get(url, headers=headers)


        return response

    @staticmethod
    def update_custom_pool(workspace_id:str, api_token:str, pool_id:str, pool_name:str, node_size:str, autoscale_dict:dict, dynamicExecutorAllocation_dict:dict):
        """
        https://learn.microsoft.com/en-us/rest/api/fabric/spark/custom-pools/update-workspace-custom-pool?tabs=HTTP
        PATCH https://api.fabric.microsoft.com/v1/workspaces/{workspaceId}/spark/pools/{poolId}

        args:
            workspace_id:str: The guid of the workspace
            pool_id:str: The guid of the pool
            api_token:str: The token used to authenticate
        """
        # validations
        code_size = CustomPools._valid_node_sizes(node_size)
        CustomPools._valid_autoscale(autoscale_dict)
        CustomPools._valid_dynamicExecutorAllocation(dynamicExecutorAllocation_dict)

        url = f'https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/spark/pools/{pool_id}'

        headers = {
        "Authorization": f"Bearer {api_token}",
        "Content-Type": "application/json"
        }

        pool_payload = {
            "name": f"{pool_name}",
            "nodeFamily": "MemoryOptimized",
            "nodeSize": f"{node_size}",
            "autoScale": autoscale_dict,
            "dynamicExecutorAllocation": dynamicExecutorAllocation_dict
            }

        response = requests.patch(url, headers=headers, json=pool_payload)

        return response

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

pool_name = 'delete_test_3'
resp = CustomPools.create_custom_pool(workspace_id=workspace_id, api_token=token, pool_name=pool_name, node_size=node_size, autoscale_dict=autoscale_dict, dynamicExecutorAllocation_dict=dynamicExecutorAllocation_dict)

resp

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

# 35acf02c-4b87-4ae6-9221-ff5cafd430b4
resp = CustomPools.get_custom_pool(workspace_id=workspace_id, pool_id='a3c9e2a9-8b46-4866-af67-b14742eed247', api_token=token)

resp.json()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# delete custom pool
resp = CustomPools.delete_custom_pool(workspace_id=workspace_id, pool_id='8bda342-b4dc-42c4-bda4-1ed32595de26', api_token=token)

resp

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

resp.content

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# get custom pools
resp = CustomPools.list_custom_pools(workspace_id=workspace_id, api_token=token)

resp

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# patch testing, try to rename
resp = CustomPools.update_custom_pool(workspace_id=workspace_id, api_token=token, pool_id='a3c9e2a9-8b46-4866-af67-b14742eed247', pool_name='renamed patch', node_size=node_size, autoscale_dict=autoscale_dict, dynamicExecutorAllocation_dict=dynamicExecutorAllocation_dict)

resp

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

resp.content

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
