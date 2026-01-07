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

# # Purview Fabric Scanning Notebook
# This notebook is for setting up manual scans of Microsoft Fabric

# CELL ********************

import msal
from azure.identity import DefaultAzureCredential, ClientSecretCredential
from azure.keyvault.secrets import SecretClient
import requests
import pandas as pd

import json

class ServicePrincipal:
    def __init__(self, client_id:str, tenant_id:str, spn_secret_name:str, vault_url:str):
        self.client_id = client_id
        self.tenant_id = tenant_id
        self.authority = f"https://login.microsoftonline.com/{self.tenant_id}"
        self.spn_secret_name = spn_secret_name
        self.vault_url = vault_url
        self.client_secret = self._get_spn_secret()


        # self.app = msal.ConfidentialClientApplication(
        #     self.client_id, authority=self.authority,
        #     client_credential=self.client_secret.value
        # )
        self.access_token = self.get_access_token()
        

    def _get_spn_secret(self):
        self.secret = notebookutils.credentials.getSecret(self.vault_url, self.spn_secret_name)

        return self.secret

    def get_access_token(self):
        """
        """
        credential = ClientSecretCredential(self.tenant_id, self.client_id, self.client_secret)
        scope = 'https://analysis.windows.net/powerbi/api/.default'
        token = credential.get_token(scope).token
        self.token = token

        return token


    def get_access_token_on_behalf_of(self, scopes, user_assertion):
        result = self.app.acquire_token_on_behalf_of(scopes, user_assertion)
        return result['access_token']

    def get_access_token_by_authorization_code(self, scopes, code, redirect_uri):
        result = self.app.acquire_token_by_authorization_code(code, scopes, redirect_uri=redirect_uri)
        return result['access_token']

    def get_access_token_by_refresh_token(self, scopes, refresh_token):
        result = self.app.acquire_token_by_refresh_token(refresh_token, scopes)
        return result['access_token']
    

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************


class Scan:
    """
    This will be used to initiate a scan and hold all data that results from a scan
    Will require an SPN instance to authenticate
    """
    def __init__(self, spn:ServicePrincipal, modified_since=None, exclude_personal_workspaces=True, exclude_inactive_workspaces=False):
        self.spn = spn
        self.modified_since = modified_since
        self.exclude_personal_workspaces = exclude_personal_workspaces
        self.exclude_inactive_workspaces = exclude_inactive_workspaces

        # initiate the scan
        self.get_modified_workspaces()
        self.scan_request = self.post_workspace_info(workspace_list=self.workspace_json_list)
        

    def get_modified_workspaces(self)->list:
        """
        modified_since: str: Last modified date and time in UTC. The format is 'YYYY-MM-DDTHH:MM:SSZ'.
        exclude_personal_workspaces: bool: Exclude personal workspaces.
        exclude_inactive_workspaces: bool: Exclude inactive workspaces.

        return list: ModifiedWorkspace: List of workspaces that have been modified.
        """
        if self.modified_since==None and self.exclude_personal_workspaces==True and self.exclude_inactive_workspaces==False:
            
            url = f'https://api.powerbi.com/v1.0/myorg/admin/workspaces/modified?excludePersonalWorkspaces={self.exclude_personal_workspaces}'
        else:
            pass

        print(f'INFO: Getting Modified Workspaces')
        response = requests.get(url, headers={'Authorization': f'Bearer {self.spn.access_token}'})
        print(f'INFO: Response: {response}')
        print(f'INFO: Response JSON: {response.json()}')

        # pass to create_workspace_json_list method to create a list of workspaces
        self.workspaces_list = self.create_workspace_json_list(response.json())
        print(f'INFO:  Workspaces List: {self.workspaces_list}')

        return response.json()
    
    def create_workspace_json_list(self, workspace_list:json)->json:
        """
        Creates a list of workspaces to be passed to the post_workspace_info method
        """
        workspace_json_list = {
            "workspaces": []
        }

        print(f'INFO: Running Create Workspace Json List Method')
        print(f'INFO:  Size of workspace list: {len(workspace_list)}')
        print(f'INFO:  workspace_list Type: {type(workspace_list)}')

        assert isinstance(workspace_list, list), f"Expected a list, got {type(workspace_list)}"
        for item in workspace_list:
            value = item['id']
            workspace_json_list['workspaces'].append(value)

        self.workspace_json_list = workspace_json_list


    def post_workspace_info(self, workspace_list:list, dataset_expressions:bool=None, 
                            dataset_schema:bool=None, datasource_details:bool=None,
                            get_artifact_users:bool=None, lineage:bool=None):
        """
        Initiates a call to receive metadata for the requested list of workspaces
        dataset_expressions: bool: Whether to return dataset expressions (DAX and Mashup queries)
        dataset_schema: bool: Whether to return dataset schema(tables, columns, measures)
        datasource_details: bool: Whether to return datasource details (connection string, credential details)
        get_artifact_users: bool: Whether to return uer details for a Power BI item (report, dashboard)
        lineage: bool: Whether to return lineage information for a dataset
        """
        if dataset_expressions==None and dataset_schema==None and datasource_details==None and get_artifact_users==None and lineage==None:
            url = 'https://api.powerbi.com/v1.0/myorg/admin/workspaces/getInfo'
        else:  
            pass

        response = requests.post(url, headers={'Authorization': f'Bearer {self.spn.access_token}', 'Content-Type': 'application/json'}, json=workspace_list)

        return response.json()
    
    def get_scan_status(self, scan_id:str)->json:
        """
        Gets the scan status for the specified scan

        return json: ScanStatus: The status of the scan
        """
        url = f'https://api.powerbi.com/v1.0/myorg/admin/workspaces/scanStatus/{scan_id}'

        response = requests.get(url, headers={'Authorization': f'Bearer {self.spn.access_token}'})

        if response.json()['status'] == 'Succeeded':
            self.scan_status = response.json()
            self.scan_result = self.get_scan_result(scan_id=scan_id)
        else:
            print('ERROR: Scan has not completed successfully')


        return response.json()

    def get_scan_result(self, scan_id:str)->json:
        """
        Gets the scan results for the specified scan
        """
        url = f'https://api.powerbi.com/v1.0/myorg/admin/workspaces/scanResult/{scan_id}'

        response = requests.get(url, headers={'Authorization': f'Bearer {self.spn.access_token}'})

        self.scan_results = response.json()

        return response.json()

        

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# try with fuam.  Note FUAM does not have tenant.read.all permissions

kv_uri = 'https://kvfabricprodeus2rh.vault.azure.net/'
client_id = '989a7c20-3e2b-4c6d-9d65-c58920d1ff6e'
tenant_id = '35acf02c-4b87-4ae6-9221-ff5cafd430b4'
client_secret_name = 'fuam-spn-secret'

spn = ServicePrincipal(client_id=client_id, tenant_id=tenant_id, spn_secret_name=client_secret_name, vault_url=kv_uri)


scan = Scan(spn)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

scan.workspaces_list

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# try with fabric-prod-spn which does have Tenant.Read.All with Admin consent required Yes

kv_uri = 'https://kvfabricprodeus2rh.vault.azure.net/'
client_id = '15a84224-c1e2-45e0-a2f5-fc8f5206f81d'
tenant_id = '35acf02c-4b87-4ae6-9221-ff5cafd430b4'
client_secret_name = 'fabric-prod-secret'

spn = ServicePrincipal(client_id=client_id, tenant_id=tenant_id, spn_secret_name=client_secret_name, vault_url=kv_uri)


scan = Scan(spn)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

mod_workspaces = scan.get_modified_workspaces()

workspaces_list = scan.create_workspace_json_list(mod_workspaces)
workspaces_list


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

workspaces_list


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

workspace_json_list = scan.create_workspace_json_list(mod_workspaces)

workspace_json_list

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# check code
workspace_json_list = {
            "workspaces": []
        }

for item in mod_workspaces:
    value = item['id']
    workspace_json_list['workspaces'].append(value)

workspace_json_list = workspace_json_list

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

workspace_json_list

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

def listTenantSettings(spn:ServicePrincipal):
    """
    GET https://api.fabric.microsoft.com/v1/admin/tenantsettings
    """
    url = 'https://api.fabric.microsoft.com/v1/admin/tenantsettings'

    response = requests.get(url, headers={'Authorization': f'Bearer {spn.access_token}'})

    return response.json()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

tenant_settings = listTenantSettings(spn)

tenant_df = pd.DataFrame(tenant_settings['tenantSettings'])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

tenant_df[tenant_df['settingName']=='Service principals can access read-only admin APIs']

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

tenant_df.iloc[0]['title']

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
