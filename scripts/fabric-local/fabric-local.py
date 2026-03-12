
import socket
import logging
import subprocess
import threading
import time
from azure.identity import DefaultAzureCredential, ClientSecretCredential
from azure.keyvault.secrets import SecretClient
from deltalake import DeltaTable

# logging.basicConfig(level=logging.WARNING)
# logging.getLogger('urllib3').setLevel(logging.DEBUG)
# logging.getLogger('azure.core.pipeline.policies.http_logging_policy').setLevel(logging.DEBUG)


def resolve_dns(hostname: str) -> set:
    """Resolve hostname to IPs and display results. Returns the set of IPs."""
    print(f'\n--- DNS Resolution: {hostname} ---')
    ips = set()
    try:
        results = socket.getaddrinfo(hostname, 443, socket.AF_INET)
        ips = set(addr[4][0] for addr in results)
        for ip in ips:
            print(f'  Resolves to: {ip}')
            if ip.startswith('10.') or ip.startswith('192.168.') or \
               any(ip.startswith(f'172.{i}.') for i in range(16, 32)):
                print(f'Private IP — traffic routed via Private Endpoint')
            else:
                print(f'Public IP — traffic routed over public internet')
    except socket.gaierror as e:
        print(f"  DNS resolution failed: {e}")
    return ips

def get_api_token_via_akv(kv_uri:str, client_id_secret:str, tenant_id_secret:str, client_secret_name:str)->str:
    """
    Function to retrieve an api token used to authenticate with Microsoft Fabric APIs

    kv_uri:str: The uri of the azure key vault
    client_id_secret:str: The name of the key used to store the value for the client id in the akv
    tenant_id_secret:str: The name of the key used to store the value for the tenant id in the akv
    client_secret_name:str: The name of the key used to store the value for the client secret in the akv

    """
    credential = DefaultAzureCredential()
    secret_client = SecretClient(vault_url=kv_uri, credential=credential)

    client_id = secret_client.get_secret(client_id_secret).value
    tenant_id = secret_client.get_secret(tenant_id_secret).value
    client_secret = secret_client.get_secret(client_secret_name).value

    credential = ClientSecretCredential(tenant_id, client_id, client_secret)
    scope = 'https://storage.azure.com/.default'
    token = credential.get_token(scope).token

    return token

#================================================================================= MAIN EXECUTION ====================================================================================================================


### get secrets via akv === Replace with your own values
kv_uri = 'https://kvfabricprodeus2rh.vault.azure.net/'
client_id_secret = 'fuam-spn-client-id'
tenant_id_secret = 'fuam-spn-tenant-id'
client_secret_name = 'fuam-spn-secret'
# read from onelake path
path = 'abfss://b196641e-3340-48b6-975f-df7bb9e3aaee@onelake.dfs.fabric.microsoft.com/0a69b54c-a7ff-4e66-b136-9ed481d43f83/Tables/insider_transactions'


token = get_api_token_via_akv(kv_uri, client_id_secret, tenant_id_secret, client_secret_name)

# DNS resolution check — shows whether traffic routes via Private Endpoint or public internet
resolve_dns('kvfabricprodeus2rh.vault.azure.net')
blob_ips = resolve_dns('onelake.blob.fabric.microsoft.com')
dfs_ips = resolve_dns('onelake.dfs.fabric.microsoft.com')


print('\n--- Reading Delta Table ---')
dt = DeltaTable(
    path,
    storage_options={
        'bearer_token': token,
        'use_fabric_endpoint': 'true'
    }
)

df = dt.to_pandas()

print(df.head())