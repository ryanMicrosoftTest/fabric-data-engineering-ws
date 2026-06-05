# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# # Orchestration Notebook


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
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# get all the records to process from the data source
# data source = abfss://a8cbda3d-903e-4154-97d9-9a91c95abb42@onelake.dfs.fabric.microsoft.com/dbf98102-9bba-459f-9614-d33c6fcf1a51/Files/hc_tables.txt

df = pd.read_csv('abfss://a8cbda3d-903e-4154-97d9-9a91c95abb42@onelake.dfs.fabric.microsoft.com/dbf98102-9bba-459f-9614-d33c6fcf1a51/Files/hc_tables.txt', header=None)

df.columns = ['table_name', 'environment_name']

df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
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

def list_environments_in_workspace(workspace_id:str, api_token:str):
    """
    List all the environments in the workspace

    https://learn.microsoft.com/en-us/rest/api/fabric/environment/items/list-environments?tabs=HTTP
    GET https://api.fabric.microsoft.com/v1/workspaces/{workspaceId}/environments
    """
    url = f'https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/environments'

    headers = {
    "Authorization": f"Bearer {api_token}",
    "Content-Type": "application/json"
    }    

    response = requests.get(url, headers=headers)

    return response

def build_execute_table_nb_payload(table_name:str, env_id:str, env:str):
    """
    Tightly coupled function to specific table payload

    """
    # build body
    body = {
    "executionData": {
        "parameters": {
            "table": {                 
                "value": table,        
                "type": "string"
            }
        },
        "configuration": {
            "environment": {
                "id": env_id,
                "name": env
            },
            "useStarterPool": False,
            "useWorkspacePool": None
                }
            }   
        }

    return body


def execute_notebook(workspace_id:str, artifact_id:str, payload:dict, api_token:str):
    """
    https://api.fabric.microsoft.com/v1/workspaces/{{WORKSPACE_ID}}/items/{{ARTIFACT_ID}}/jobs/instances?jobType=RunNotebook
    session limits = 5
    pipeline = 50


    """
    url = f'https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/items/{artifact_id}/jobs/instances?jobType=RunNotebook'
    

    headers = {
    "Authorization": f"Bearer {api_token}",
    "Content-Type": "application/json"
    }    

    response = requests.post(url, headers=headers, json=payload)

    return response


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

### spark monitoring functions

def list_livy_sessions(workspace_id:str, api_token:str):
    """
    https://learn.microsoft.com/en-us/rest/api/fabric/spark/livy-sessions/list-livy-sessions?tabs=HTTP
    GET https://api.fabric.microsoft.com/v1/workspaces/{workspaceId}/spark/livySessions


    """
    url = f'https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/spark/livySessions'
    

    headers = {
    "Authorization": f"Bearer {api_token}",
    "Content-Type": "application/json"
    }    

    response = requests.get(url, headers=headers)

    return response

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

kv_uri = 'https://kvfabricprodeus2rh.vault.azure.net/'
client_id_secret = 'fuam-spn-client-id'
tenant_id_secret = 'fuam-spn-tenant-id'
client_secret_name = 'fuam-spn-secret'

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# get API token
token = get_api_token_via_akv(kv_uri, client_id_secret, tenant_id_secret, client_secret_name)

# list environments
workspace_id = 'a8cbda3d-903e-4154-97d9-9a91c95abb42'

env_resp = list_environments_in_workspace(workspace_id, token)
env_list = env_resp.json()['value']

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

env_df = pd.DataFrame(env_list)

env_df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

env_df[env_df['displayName']=='small_as_1_10_env']['id'].values[0]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# create a table processing list
table_processing_list = []
notebook_id = '1da52dbc-809b-44c0-9565-9e4dcd4ca471'

# pass each table name into 

for row in df.itertuples():
    # getr table and environment
    table = row[1]
    env = str.strip(row[2])

    # lookup environment id
    env_id = env_df[env_df['displayName']==env]['id'].values[0]

    # add to list
    entry = {
        "table": table,
        "env_id": env_id,
        "env_name": env
    }
    # keeping for later
    table_processing_list.append(entry)

    # build payload for run notebook request
    body = build_execute_table_nb_payload(table, env_id, env)
    
    # execute run notebook request
    run_resp = execute_notebook(workspace_id, notebook_id, body, token)




# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

table_processing_list[0]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

env_id

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# POST https://api.fabric.microsoft.com/v1/workspaces/{{WORKSPACE_ID}}/items/{{ARTIFACT_ID}}/jobs/instances?jobType=RunNotebook

body = {
    "executionData": {
        "parameters": {
            "parameterName": {
                "value": table,
                "type": "string"
            }
        },
        "configuration": {
            "environment": {
                "id": env_id,
                "name": env
            },  
        "useStarterPool": False,

        }
    }
}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# build payload
body = build_execute_table_nb_payload(table, env_id, env)

body

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# run notebook
notebook_id = '1da52dbc-809b-44c0-9565-9e4dcd4ca471'

run_resp = execute_notebook(workspace_id, notebook_id, body, token)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

run_resp

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Get Details on Run from Spark Monitoring APIs

# CELL ********************

list_livy_sessions_resp = list_livy_sessions(workspace_id, token)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# create spark_df
spark_df = pd.DataFrame(list_livy_sessions_resp.json()['value'])

spark_df.columns

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# select values between two times
spark_df_copy = spark_df.copy()

spark_df_copy['submittedDateTime'] = pd.to_datetime(spark_df_copy['submittedDateTime'], utc=True)

# define time boundaries
start = pd.to_datetime("2026-01-29T14:30:36Z", utc=True)
end   = pd.to_datetime("2026-01-29T16:50:36Z", utc=True)


mask = spark_df_copy['submittedDateTime'].between(start, end)
filtered = spark_df_copy.loc[mask]

filtered

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

filtered['isHighConcurrency']

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
