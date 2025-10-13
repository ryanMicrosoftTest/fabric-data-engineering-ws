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

# # Enable SQL Audit on Lakehouse SQL Analytics Endpoint 
# - Get Lakehouse (to ensure correct id is used)
# - Get SQL Audit Settings
# - Set Audit Actions and Groups
# - Update SQL Audit Settings

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
import asyncio
import aiohttp
from azure.identity import DefaultAzureCredential, ClientSecretCredential
from azure.keyvault.secrets import SecretClient
import os
import notebookutils
import pandas as pd
import json
from typing import Optional, Dict, Any
import logging

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

def get_lakehouse(workspace_id:str, lakehouse_id:str, token:str):
    """
    workspace_id:str: The guid of the workspace where the lakehouse resides
    lakehouse_id:str: The guid of the lakehouse

    GET https://api.fabric.microsoft.com/v1/workspaces/{workspaceId}/lakehouses/{lakehouseId}
    """
    url = f'https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/lakehouses/{lakehouse_id}'

    headers = {
    "Authorization": f"Bearer {token}",
    "Content-Type": "application/json"
    }   

    resp = requests.get(url, headers=headers)

    return resp


def update_sql_audit_settings(workspace_id:str, item_id:str, token=str):
    """
    workspace_id:str: The guid of the workspace where the lakehouse resides
    item_id:str: The guid of the lakehouse
    token:str: The token to authenticate with

    url = PATCH 'https://api.fabric.microsoft.com/v1/workspaces/{workspaceId}/sqlEndpoints/{itemId}/settings/sqlAudit'
    """
    url = f'https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/sqlEndpoints/{item_id}/settings/sqlAudit'

    req_body = {
        "state": "Enabled",
        "retentionDays": 1000
    }

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }

    resp = requests.patch(url=url, json=req_body, headers=headers)
    return resp



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# client call
### test
token = get_api_token_via_akv(kv_uri, client_id_secret, tenant_id_secret, client_secret_name)


# update sql audit settings
workspace_id = 'a8cbda3d-903e-4154-97d9-9a91c95abb42'
lakehouse_id = 'd550d915-f3a8-418b-b3f4-c2cb369838c3'

lakehouse_resp = get_lakehouse(workspace_id, lakehouse_id, token)

lakehouse_resp.json()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

sql_properties_id = lakehouse_resp.json()['properties']['sqlEndpointProperties']['id']

resp = update_sql_audit_settings(workspace_id, sql_properties_id, token)

resp.json()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }
