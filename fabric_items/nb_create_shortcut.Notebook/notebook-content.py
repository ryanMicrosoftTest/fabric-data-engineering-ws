# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# # Notebook to Create Shortcuts
# This will Take in arguments from Variable Library and Create Shortcuts

# PARAMETERS CELL ********************

shortcut_name = "healthdata"
shortcut_location = "Files"
target_type = "AdlsGen2"
target_connection_id = "1f126619-984e-4ab2-b5fa-42d5680242f7"
target_location = "https://healthsaadlsrheus.dfs.core.windows.net"
target_subpath = "/healthdata"
data_type = "Folder"
workspace_id = "7afc490e-115f-472c-a205-17dc6a5bee52"
item_id = "6a63ffcd-fca4-4a4a-be88-96fe8a4b12cc"
kv_uri = "https://kvfabricprodeus2rh.vault.azure.net/"
tenant_id_secret = "tenantID"
client_id_secret = "spn-client-id"
client_secret_name = "spn-secret"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import json
from azure.identity import ClientSecretCredential
import requests

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%pip show azure.identity

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%pip show notebookutils

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print(shortcut_name)
print(shortcut_location)
print(target_type)
print(target_connection_id)
print(target_location)
print(target_subpath)
print(data_type)
print(f'Workspace ID: {workspace_id}')
print(f'Item ID: {item_id}')
print(f'Key Vault URI: "{kv_uri}')
print(f'Tenant ID Secret: {tenant_id_secret}')
print(f'Client ID Secret: {client_id_secret}')
print(f'Client Secret Name: {client_secret_name}')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# print(f'Workspace ID: {workspace_id}')
# print(f'Item ID: {item_id}')
# print(f'Shortcut Name: {shortcut_name}')
# print(f'Shortcut Path: {shortcut_path}')
# print(f'Shortcut Target: {shortcut_target}')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

class ShortcutUtils:
    
    @staticmethod
    def build_shortcut_create_payload(name: str,target_type: str,target_connection: str,target_location: str,target_subpath: str,shortcut_location: str):
        """
        This is a function to create the payload which will be used by the
        create_shortcut function to create a shortcut in a lakehouse

        name: str
        target_type: str
        target_connection: str
        target_location: str
        target_subpath: str
        shortcut_location: str
        """
        return {
            "name": name,
            "path": f"/{shortcut_location}",
            "target": {
                "type": target_type,
                "adlsGen2": {
                    "connectionId": target_connection,
                    "location": target_location,
                    "subpath": target_subpath
                }
            }
        }
    
    @staticmethod
    def create_shortcut(workspace_id: str, item_id: str, target: dict, api_token: str):
        """
        Create a new shortcut in Microsoft Fabric OneLake.

        API Reference:
        https://learn.microsoft.com/en-us/rest/api/fabric/core/onelake-shortcuts/create-shortcut?tabs=HTTP

        Args:
            workspace_id (str): UUID of the workspace where the shortcut is to be created.
            item_id (str): UUID of the item where the shortcut will be created.
            target (dict): JSON payload for the shortcut (must follow API schema).
            api_token (str): API token for authentication.

        Returns:
            requests.Response: The HTTP response object.
        """
        url = (
            f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/items/{item_id}/shortcuts"
            "?shortcutConflictPolicy=CreateOrOverwrite"
        )

        headers = {
            "Authorization": f"Bearer {api_token}",
            "Content-Type": "application/json"
        }

        response = requests.post(url, headers=headers, json=target)

        if 200 <= response.status_code < 300:
            print(f"Shortcut Request Successful: {response.content}")
        else:
            print(f"Error in creating shortcut: {response.status_code} - {response.content}")

        return response



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

def get_variable_library(workspace_id:str, variable_library_id:str, api_token:str):
    """
    https://learn.microsoft.com/en-us/rest/api/fabric/variablelibrary/items/get-variable-library?tabs=HTTP
    GET https://api.fabric.microsoft.com/v1/workspaces/{workspaceId}/VariableLibraries/{variableLibraryId}

    """
    url = f'https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/VariableLibraries/{variable_library_id}'

    headers = {
    "Authorization": f"Bearer {api_token}",
    "Content-Type": "application/json"
    }


    response = requests.get(url, headers=headers)

    return response

def build_shortcut_create_payload(name:str, target_type:str, target_connection:str, target_location:str, target_subpath:str, shortcut_location:str):
    """
    This is a function to create the payload which will be used by the create_shortcut function to create a shortcut in a lakehouse

    name:str
    target_type:str
    target_connection:str,
    target_location:str
    target_subpath:str,
    shortcut_location:str
    """
    

    return {
    "name": f"{name}",
    "path": f"/{shortcut_location}",
    "target": {
      "type": f"{target_type}",
      "adlsGen2": {
        "connectionId": f"{target_connection}",
        "location": f"{target_location}",
        "subpath": f"{target_subpath}"
      }
    }
  }

def create_shortcut(workspace_id:str, item_id:str, target:str, api_token:str):
    """
    https://learn.microsoft.com/en-us/rest/api/fabric/core/onelake-shortcuts/create-shortcut?tabs=HTTP

    Create a new shortcut

    workspace_id:str: The uuid of the workspace where the shortcut is to be created
    item_id:str: The uuid of the item to be created
    api_token:str: The API Token used for authentication with the endpoiont
    """
    url = f'https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/items/{item_id}/shortcuts?shortcutConflictPolicy=CreateOrOverwrite'

    headers = {
    "Authorization": f"Bearer {api_token}",
    "Content-Type": "application/json"
    }

    response = requests.post(url, headers=headers,json=target)

    if response.status_code >=200 and response.status_code <300:
        print(f'Shortcut Request Successful: {response.content}')

        return response
    else:
        print(f'Error in creating shortcut: {response.content}')

        return response


def create_deployment_pipeline(json_payload:dict, api_token:str):
    """
    https://learn.microsoft.com/en-us/rest/api/fabric/core/deployment-pipelines/create-deployment-pipeline?tabs=HTTP
    POST https://api.fabric.microsoft.com/v1/deploymentPipelines

    json_payload:dict: The json payload to create the deployment pipeline {
  "displayName": "My Deployment Pipeline Name",
  "description": "My deployment pipeline description",
  "stages": [
    {
      "displayName": "Development",
      "description": "Development stage description",
      "isPublic": false
    },
    {
      "displayName": "Test",
      "description": "Test stage description",
      "isPublic": false
    },
    {
      "displayName": "Production",
      "description": "Production stage description",
      "isPublic": true
    }
  ]
}
    api_token:str: The API token used to authenticate with the API
    """
    # validate payload is valid
    tup_resp = validate_deployment_pipeline_schema(json_payload)

    url = f'https://api.fabric.microsoft.com/v1/deploymentPipelines'

    headers = {
    "Authorization": f"Bearer {api_token}",
    "Content-Type": "application/json"
    }    

    response = requests.post(url, headers=headers, json=json_payload)

    return response

def add_group_to_deployment_pipeline(deployment_pipeline_id:str, json_payload:dict, api_token:str):
    """
    https://learn.microsoft.com/en-us/rest/api/fabric/core/deployment-pipelines/add-deployment-pipeline-role-assignment?tabs=HTTP#add-a-group-role-assignment-to-a-deployment-pipeline-example
    POST https://api.fabric.microsoft.com/v1/deploymentPipelines/{deploymentPipelineId}/roleAssignments

    deployment_pipeline_id:str: The UUID of the Deployment Pipeline
    payload_type:str: One of user or group
    api_token:str: The API token used to authenticate with the API
    """
    # check payload_type for processing

    tup_resp = validate_group_role_assignment_schema(json_payload)

    if tup_resp[0]:
        print('Schema Successful')
    else:
        error_message=tup_resp[1]
        print(f'Schema Validation Failed: {error_message}')
        return {"error": error_message, "status_code": 400}

    url = f'https://api.fabric.microsoft.com/v1/deploymentPipelines/{deployment_pipeline_id}/roleAssignments'

    headers = {
    "Authorization": f"Bearer {api_token}",
    "Content-Type": "application/json"
    }    

    response = requests.post(url, headers=headers, json=json_payload)

    return response

def assign_workspace_to_stage(deployment_pipeline_id:str, stage_id:str, workspace_id:str, api_token:str):
    """
    https://learn.microsoft.com/en-us/rest/api/fabric/core/deployment-pipelines/assign-workspace-to-stage?tabs=HTTP
    POST https://api.fabric.microsoft.com/v1/deploymentPipelines/{deploymentPipelineId}/stages/{stageId}/assignWorkspace

    deployment_pipeline_id:str UUID of the deployment pipeline
    stage_id: uuid of the stage
    workspace_id:str: The uuid of the workspace to be assigned
    api_token:str: The API token used to authenticate with the API



    The payload for assigning the workspace

    {
        "workspaceId": "4de5bcc4-2c88-4efe-b827-4ee7b289b496"
    }
    """
    url = f'https://api.fabric.microsoft.com/v1/deploymentPipelines/{deployment_pipeline_id}/stages/{stage_id}/assignWorkspace'

    headers = {
    "Authorization": f"Bearer {api_token}",
    "Content-Type": "application/json"
    }

    json_payload = {
        "workspaceId": f"{workspace_id}"
    }

    print(f'Printing URL: {url}')
    print(f'Printing Headers: {headers}')
    print(f'Printing Payload: {json_payload}')

    response = requests.post(url, headers=headers, json=json_payload)

    return response

def validate_group_role_assignment_schema(data):
    """
    Validates if data follows the schema:
    {
      "principal": {
        "id": str,
        "type": str  # should be one of the valid types
      },
      "role": str  # should be one of the valid roles
    }
    
    Args:
        data: Dictionary or JSON object to validate
        
    Returns:
        tuple: (is_valid, error_message)
    """
    # Check if data is a dictionary
    if not isinstance(data, dict):
        return False, "Data must be a dictionary"
    
    # Check if principal exists and is a dictionary
    if "principal" not in data:
        return False, "Missing 'principal' field"
    if not isinstance(data["principal"], dict):
        return False, "'principal' must be a dictionary"
    
    # Check if principal has id and type
    principal = data["principal"]
    if "id" not in principal:
        return False, "Missing 'principal.id' field"
    if not isinstance(principal["id"], str):
        return False, "'principal.id' must be a string"
    
    if "type" not in principal:
        return False, "Missing 'principal.type' field"
    if not isinstance(principal["type"], str):
        return False, "'principal.type' must be a string"
    
    # Validate principal type (add more valid types if needed)
    valid_principal_types = ["User", "Group", "ServicePrincipal"]
    if principal["type"] not in valid_principal_types:
        return False, f"'principal.type' must be one of: {', '.join(valid_principal_types)}"
    
    # Check if role exists and is a string
    if "role" not in data:
        return False, "Missing 'role' field"
    if not isinstance(data["role"], str):
        return False, "'role' must be a string"
    
    # Validate role (add more valid roles if needed)
    valid_roles = ["Admin"]
    if data["role"] not in valid_roles:
        print('Currently only Admin Role is supported: https://learn.microsoft.com/en-us/rest/api/fabric/core/deployment-pipelines/add-deployment-pipeline-role-assignment?tabs=HTTP#deploymentpipelinerole')
        return False, f"'role' must be one of: {', '.join(valid_roles)}"
    
    # All checks passed
    return True, "Schema is valid"

def validate_deployment_pipeline_schema(data):
    """
    Validates if data follows the deployment pipeline creation schema:
    {
      "displayName": str,
      "description": str,
      "stages": [
        {
          "displayName": str,
          "description": str,
          "isPublic": bool
        },
        ...
      ]
    }
    
    Args:
        data: Dictionary or JSON object to validate
        
    Returns:
        tuple: (is_valid, error_message)
    """
    # Check if data is a dictionary
    if not isinstance(data, dict):
        return False, "Data must be a dictionary"
    
    # Check required top-level fields
    required_fields = ["displayName", "description", "stages"]
    for field in required_fields:
        if field not in data:
            return False, f"Missing required field '{field}'"
    
    # Check displayName and description are strings
    if not isinstance(data["displayName"], str):
        return False, "'displayName' must be a string"
    if not isinstance(data["description"], str):
        return False, "'description' must be a string"
    
    # Check stages is a list
    if not isinstance(data["stages"], list):
        return False, "'stages' must be a list"
    
    # Check if stages is empty
    if len(data["stages"]) == 0:
        return False, "'stages' cannot be empty"
    
    # Validate each stage
    for i, stage in enumerate(data["stages"]):
        # Check if stage is a dictionary
        if not isinstance(stage, dict):
            return False, f"Stage at index {i} must be a dictionary"
        
        # Check required stage fields
        stage_required_fields = ["displayName", "description", "isPublic"]
        for field in stage_required_fields:
            if field not in stage:
                return False, f"Missing required field '{field}' in stage at index {i}"
        
        # Check stage field types
        if not isinstance(stage["displayName"], str):
            return False, f"'displayName' must be a string in stage at index {i}"
        if not isinstance(stage["description"], str):
            return False, f"'description' must be a string in stage at index {i}"
        if not isinstance(stage["isPublic"], bool):
            return False, f"'isPublic' must be a boolean in stage at index {i}"
    
    # Check for duplicate stage names (optional validation)
    stage_names = [stage["displayName"] for stage in data["stages"]]
    if len(stage_names) != len(set(stage_names)):
        return False, "Stage displayNames must be unique"
    
    # All checks passed
    return True, "Schema is valid"

def delete_item_from_workspace(workspace_id:str, item_id:str, api_token:str):
    """
    Deletes an item from the workspace
    DELETE https://api.fabric.microsoft.com/v1/workspaces/{workspaceId}/items/{itemId}

    https://learn.microsoft.com/en-us/rest/api/fabric/core/items/delete-item?tabs=HTTP

    workspace_id:str: The uuid of the workspace where the item is
    item_id:str: The uuid of the item to be deleted
    token:str: The API token used for authentication
    """
    url = f'https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/items/{item_id}'

    headers = {
    "Authorization": f"Bearer {api_token}",
    "Content-Type": "application/json"
    }


    response = requests.delete(url, headers=headers)

    return response


def clean_json_string(escaped_json_string):
    """
    Convert an escaped JSON string to a properly formatted JSON object.
    
    Args:
        escaped_json_string (str): JSON string with escaped quotes and newlines
        
    Returns:
        dict: Parsed JSON object
    """
    try:
        # First, decode the escaped characters
        # Replace \\n with actual newlines and \\\" with \"
        cleaned_string = escaped_json_string.replace('\\n', '\n').replace('\\"', '"')
        
        # Parse the JSON string
        json_object = json.loads(cleaned_string)
        
        return json_object
    
    except json.JSONDecodeError as e:
        print(f"Error parsing JSON: {e}")
        return None

def format_json_output(json_object, indent=2):
    """
    Format JSON object as a pretty-printed string.
    
    Args:
        json_object (dict): JSON object to format
        indent (int): Number of spaces for indentation
        
    Returns:
        str: Formatted JSON string
    """
    return json.dumps(json_object, indent=indent)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# get api token
token = get_api_token_via_akv(kv_uri, client_id_secret, tenant_id_secret, client_secret_name)

# build shortcut payload
shortcut_payload = build_shortcut_create_payload(shortcut_name, target_type, target_connection_id,target_location, target_subpath, shortcut_location)



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

shortcut_payload

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# create shortcut
shortcut_resp = create_shortcut(workspace_id, item_id, shortcut_payload, token)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

shortcut_resp.content

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

shortcut_resp

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
