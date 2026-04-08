"""
Example of how to run
python trigger-fabric-pipeline.py \
    --kv-uri "https://kvfabricprodeus2rh.vault.azure.net/" \
    --client-id-secret "fuam-spn-client-id" \
    --tenant-id-secret "fuam-spn-tenant-id" \
    --client-secret-name "fuam-spn-secret" \
    --workspace-id "a8cbda3d-903e-4154-97d9-9a91c95abb42" \
    --item-id "27c03e8e-4f36-4018-936e-21b6d0e39cd5"
"""
import requests
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
import msal
import argparse
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def get_api_token_via_akv(kv_uri:str, client_id_secret:str, tenant_id_secret:str, client_secret_name:str)->str:
    """
    Function to retrieve an api token used to authenticate with Microsoft Fabric APIs

    kv_uri:str: The uri of the azure key vault
    client_id_secret:str: The name of the key used to store the value for the client id in the akv
    tenant_id_secret:str: The name of the key used to store the value for the tenant id in the akv
    client_secret_name:str: The name of the key used to store the value for the client secret in the akv

    """
    # Use DefaultAzureCredential for authentication to Key Vault
    credential = DefaultAzureCredential()
    secret_client = SecretClient(vault_url=kv_uri, credential=credential)

    # Retrieve secrets from Azure Key Vault
    client_id = secret_client.get_secret(client_id_secret).value
    tenant_id = secret_client.get_secret(tenant_id_secret).value
    client_secret = secret_client.get_secret(client_secret_name).value

    # Use MSAL to acquire token
    authority = f'https://login.microsoftonline.com/{tenant_id}'
    scope = ['https://analysis.windows.net/powerbi/api/.default']

    app = msal.ConfidentialClientApplication(
        client_id,
        authority=authority,
        client_credential=client_secret
    )

    result = app.acquire_token_for_client(scopes=scope)

    if 'access_token' in result:
        return result['access_token']
    else:
        raise Exception(f"Failed to acquire token: {result.get('error_description', 'Unknown error')}")

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

    response = requests.post(url, headers=headers, json=body, timeout=600)
    response.raise_for_status()

    return response


### Main Section ###############
def main():
    parser = argparse.ArgumentParser(description='Trigger a Microsoft Fabric pipeline via REST API')
    
    parser.add_argument('--kv-uri', required=True, help='The URI of the Azure Key Vault')
    parser.add_argument('--client-id-secret', required=True, help='The name of the secret storing the client ID in AKV')
    parser.add_argument('--tenant-id-secret', required=True, help='The name of the secret storing the tenant ID in AKV')
    parser.add_argument('--client-secret-name', required=True, help='The name of the secret storing the client secret in AKV')
    parser.add_argument('--workspace-id', required=True, help='The Fabric workspace ID')
    parser.add_argument('--item-id', required=True, help='The Fabric pipeline item ID')
    
    args = parser.parse_args()
    
    # Get OAuth token
    token = get_api_token_via_akv(
        args.kv_uri, 
        args.client_id_secret, 
        args.tenant_id_secret, 
        args.client_secret_name
    )
    
    logger.info(f'Running Pipeline {args.item_id} in Workspace {args.workspace_id}')

    # Run pipeline
    resp = run_on_demand_pipeline_job(args.workspace_id, args.item_id, token)
    
    logger.info(f"Response Status Code: {resp.status_code}")
    logger.debug(f"Response: {resp.text}")
    
    return resp


if __name__ == '__main__':
    main()