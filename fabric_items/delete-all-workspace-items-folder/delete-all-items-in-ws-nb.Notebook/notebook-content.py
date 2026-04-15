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

def list_all_connections(api_token:str):
    """
    GET https://api.fabric.microsoft.com/v1/connections
    https://learn.microsoft.com/en-us/rest/api/fabric/core/connections/list-connections?tabs=HTTP

    """
    url = f'https://api.fabric.microsoft.com/v1/connections'

    headers = {
    "Authorization": f"Bearer {api_token}",
    "Content-Type": "application/json"
    }    

    response = requests.get(url, headers=headers)

    return response

def list_cxn_role_assignments():
    """
    https://learn.microsoft.com/en-us/rest/api/fabric/core/connections/list-connection-role-assignments?tabs=HTTP

    GET https://api.fabric.microsoft.com/v1/connections/{connectionId}/roleAssignments
    """
    pass

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

def update_cxn_role_assignment(connection_id:str, api_token:str, cxn_role_assignment_id:str='c65f2c4b-7fe6-4274-b8c9-2bcfbb6d784b', role:str='User'):
    """
    https://learn.microsoft.com/en-us/rest/api/fabric/core/connections/update-connection-role-assignment?tabs=HTTP
    Defaults to user for now
    assume 2b1b3570-b1e9-4e2f-9f8b-e47653dbf652

    PATCH https://api.fabric.microsoft.com/v1/connections/{connectionId}/roleAssignments/{connectionRoleAssignmentId}
    """
    url = f'https://api.fabric.microsoft.com/v1/connections/{connection_id}/roleAssignments/{cxn_role_assignment_id}'

    headers = {
    "Authorization": f"Bearer {api_token}",
    "Content-Type": "application/json"
    }    

    body = {
        "role": "User"
    }

    response = requests.patch(url, headers=headers, json=body)

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
"""
ws_items_list = workspace_items_df['id'].tolist()

for _ in ws_items_list:
    # delete ws item
    del_resp = delete_all_items_in_workspace(workspace_id, _, token)

"""

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# add spn to all connections
token = 'eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiIsIng1dCI6IlBjWDk4R1g0MjBUMVg2c0JEa3poUW1xZ3dNVSIsImtpZCI6IlBjWDk4R1g0MjBUMVg2c0JEa3poUW1xZ3dNVSJ9.eyJhdWQiOiJodHRwczovL2FwaS5mYWJyaWMubWljcm9zb2Z0LmNvbSIsImlzcyI6Imh0dHBzOi8vc3RzLndpbmRvd3MubmV0LzM1YWNmMDJjLTRiODctNGFlNi05MjIxLWZmNWNhZmQ0MzBiNC8iLCJpYXQiOjE3Njk1NDM1MTQsIm5iZiI6MTc2OTU0MzUxNCwiZXhwIjoxNzY5NTQ4NTQzLCJhY2N0IjowLCJhY3IiOiIxIiwiYWlvIjoiQVlRQWUvOGJBQUFBdHVWZVZrQmtnQlRBbGYxczNDUFJsTDZmZUprRmxaTDhSZDFwMzJSTGZ6ZmVQb2FtWGkzMjltVXpnYjBKOERjenZVcGtsdWlRSzVVRTZhc3dMRktUNXllaFd1SWpTQ3AvQ2hlQ3NpcDAxM1Rqb3M4L1J3ZnlnTy9kSEtNNnFOSkhLZ1hEbUFTOXBsdFl3VzZhejc5ekNZcWtIQk5WQWU1dkFaWHhQUGIwWS9ZPSIsImFtciI6WyJwd2QiLCJyc2EiLCJtZmEiXSwiYXBwaWQiOiIxOGZiY2ExNi0yMjI0LTQ1ZjYtODViMC1mN2JmMmIzOWIzZjMiLCJhcHBpZGFjciI6IjAiLCJkZXZpY2VpZCI6ImQ4OGUzYTM3LTdmN2EtNGUyNC1hMmRhLTUyY2VlMDIwMjgyNSIsImZhbWlseV9uYW1lIjoiQWRtaW5pc3RyYXRvciIsImdpdmVuX25hbWUiOiJTeXN0ZW0iLCJpZHR5cCI6InVzZXIiLCJpcGFkZHIiOiIxNzIuMjAwLjcwLjg5IiwibmFtZSI6IlN5c3RlbSBBZG1pbmlzdHJhdG9yIiwib2lkIjoiYzY1ZjJjNGItN2ZlNi00Mjc0LWI4YzktMmJjZmJiNmQ3ODRiIiwicHVpZCI6IjEwMDMyMDAzODk4NzUwQjMiLCJyaCI6IjEuQWNvQUxQQ3NOWWRMNWtxU0lmOWNyOVF3dEFrQUFBQUFBQUFBd0FBQUFBQUFBQUQ2QUZyS0FBLiIsInNjcCI6IkFwcC5SZWFkLkFsbCBDYXBhY2l0eS5SZWFkLkFsbCBDYXBhY2l0eS5SZWFkV3JpdGUuQWxsIENvbm5lY3Rpb24uUmVhZC5BbGwgQ29ubmVjdGlvbi5SZWFkV3JpdGUuQWxsIENvbnRlbnQuQ3JlYXRlIERhc2hib2FyZC5SZWFkLkFsbCBEYXNoYm9hcmQuUmVhZFdyaXRlLkFsbCBEYXRhZmxvdy5SZWFkLkFsbCBEYXRhZmxvdy5SZWFkV3JpdGUuQWxsIERhdGFzZXQuUmVhZC5BbGwgRGF0YXNldC5SZWFkV3JpdGUuQWxsIEdhdGV3YXkuUmVhZC5BbGwgR2F0ZXdheS5SZWFkV3JpdGUuQWxsIEl0ZW0uRXhlY3V0ZS5BbGwgSXRlbS5FeHRlcm5hbERhdGFTaGFyZS5BbGwgSXRlbS5SZWFkV3JpdGUuQWxsIEl0ZW0uUmVzaGFyZS5BbGwgT25lTGFrZS5SZWFkLkFsbCBPbmVMYWtlLlJlYWRXcml0ZS5BbGwgUGlwZWxpbmUuRGVwbG95IFBpcGVsaW5lLlJlYWQuQWxsIFBpcGVsaW5lLlJlYWRXcml0ZS5BbGwgUmVwb3J0LlJlYWRXcml0ZS5BbGwgUmVwcnQuUmVhZC5BbGwgU3RvcmFnZUFjY291bnQuUmVhZC5BbGwgU3RvcmFnZUFjY291bnQuUmVhZFdyaXRlLkFsbCBUYWcuUmVhZC5BbGwgVGVuYW50LlJlYWQuQWxsIFRlbmFudC5SZWFkV3JpdGUuQWxsIFVzZXJTdGF0ZS5SZWFkV3JpdGUuQWxsIFdvcmtzcGFjZS5HaXRDb21taXQuQWxsIFdvcmtzcGFjZS5HaXRVcGRhdGUuQWxsIFdvcmtzcGFjZS5SZWFkLkFsbCBXb3Jrc3BhY2UuUmVhZFdyaXRlLkFsbCIsInNpZCI6IjAwYjllMjA5LWNkZGUtNWRkNy05ZTliLTU3MTZkZTlhNDQ3MiIsInNpZ25pbl9zdGF0ZSI6WyJrbXNpIl0sInN1YiI6IlNJaUptdUtWOWdDeHdXWkp1VjhuX1pUVFRaREFKRC03ajlRb1VxWWNLZDQiLCJ0aWQiOiIzNWFjZjAyYy00Yjg3LTRhZTYtOTIyMS1mZjVjYWZkNDMwYjQiLCJ1bmlxdWVfbmFtZSI6ImFkbWluQE1uZ0Vudk1DQVAzNzI4OTIub25taWNyb3NvZnQuY29tIiwidXBuIjoiYWRtaW5ATW5nRW52TUNBUDM3Mjg5Mi5vbm1pY3Jvc29mdC5jb20iLCJ1dGkiOiJsQ3J6akdBZE0waXd6ZU95UzdnckFBIiwidmVyIjoiMS4wIiwid2lkcyI6WyI2MmU5MDM5NC02OWY1LTQyMzctOTE5MC0wMTIxNzcxNDVlMTAiLCJiNzlmYmY0ZC0zZWY5LTQ2ODktODE0My03NmIxOTRlODU1MDkiXSwieG1zX2FjdF9mY3QiOiIzIDUiLCJ4bXNfZnRkIjoiN3BWMnRIeGtLOENYUmx6RGhjM2VteFNxMXp4VDBEX2dKd3ZwY3lRRER2Y0JkWE5sWVhOMExXUnpiWE0iLCJ4bXNfaWRyZWwiOiIyNCAxIiwieG1zX3BsIjoiZW4iLCJ4bXNfc3ViX2ZjdCI6IjQgMyJ9.TJq4yIZ2p2qiUyeSCnnbWVsDQNZucc-WwusUz82Og4WKT8f298TJB_yRlKrRv-VU56ZuUXlibfnzDHGJCyoFQ9OznRJ076kV4idBhYIUa9CaTbQC9e1TYE5hFPOQnoRBDFeL25dH77V6A34LAoXivpzKnD9b8oVWEnCXJY5QfyATxxLtcRSVrpyP1OSTT7GRxGvuyxVDlt4GuVAUmUJ_vfBemDBWYmHazoFHco_fA2i8B8_NTBu_ki6n9YjuJyMGByYP-BOJBcsseZG1aG6t4sXSxOTTgdBYaXI4iyXsGIXikeXY5GdbTjZy9AkS4NbM9lZAtrklwNXFlzK9H2Zj7g'
cxn_list = list_all_connections(token)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

cxn_list.json()['value']

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

cxn_df = pd.DataFrame(cxn_list.json()['value'])

cxn_df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

cxn_id_list = cxn_df['id'].tolist()

cxn_id_list

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

cxn_df[cxn_df['displayName']=='api example admin']

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

for _ in cxn_id_list:
    # add non-prod-spn to connection
    resp = update_cxn_role_assignment(_, token)
    print(resp)

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
