"""
OneLake local connectivity test.

Example usage:
    python fabric-local.py \
        --kv-uri "https://<your-keyvault>.vault.azure.net/" \
        --client-id-secret "spn-client-id" \
        --tenant-id-secret "spn-tenant-id" \
        --client-secret-name "spn-secret" \
        --workspace-path-1 "abfss://<ws-id>@onelake.dfs.fabric.microsoft.com/<lakehouse-id>/Tables/<table>" \
        --workspace-path-2 "abfss://<ws-id>@onelake.dfs.fabric.microsoft.com/<lakehouse-id>/Tables/<table>"

All arguments can also be supplied via environment variables:
    KV_URI, CLIENT_ID_SECRET, TENANT_ID_SECRET, CLIENT_SECRET_NAME,
    WORKSPACE_PATH_1, WORKSPACE_PATH_2
"""
import argparse
import io
import os
import socket

import pandas as pd
import pyarrow.parquet as pq
import requests
from azure.identity import ClientSecretCredential, DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
from azure.storage.filedatalake import DataLakeServiceClient
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


def get_api_token_via_akv(kv_uri: str, client_id_secret: str, tenant_id_secret: str, client_secret_name: str) -> str:
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


def main():
    parser = argparse.ArgumentParser(description='Test local OneLake connectivity via Private Endpoint')
    parser.add_argument('--kv-uri', default=os.environ.get('KV_URI'),
                        required=not os.environ.get('KV_URI'),
                        help='Azure Key Vault URI (env: KV_URI)')
    parser.add_argument('--client-id-secret', default=os.environ.get('CLIENT_ID_SECRET'),
                        required=not os.environ.get('CLIENT_ID_SECRET'),
                        help='AKV secret name for the SPN client ID (env: CLIENT_ID_SECRET)')
    parser.add_argument('--tenant-id-secret', default=os.environ.get('TENANT_ID_SECRET'),
                        required=not os.environ.get('TENANT_ID_SECRET'),
                        help='AKV secret name for the tenant ID (env: TENANT_ID_SECRET)')
    parser.add_argument('--client-secret-name', default=os.environ.get('CLIENT_SECRET_NAME'),
                        required=not os.environ.get('CLIENT_SECRET_NAME'),
                        help='AKV secret name for the SPN client secret (env: CLIENT_SECRET_NAME)')
    parser.add_argument('--workspace-path-1', default=os.environ.get('WORKSPACE_PATH_1'),
                        required=not os.environ.get('WORKSPACE_PATH_1'),
                        help='abfss:// path to workspace 1 Delta table (env: WORKSPACE_PATH_1)')
    parser.add_argument('--workspace-path-2', default=os.environ.get('WORKSPACE_PATH_2'),
                        required=not os.environ.get('WORKSPACE_PATH_2'),
                        help='abfss:// path to workspace 2 Delta table (env: WORKSPACE_PATH_2)')
    args = parser.parse_args()

    kv_uri = args.kv_uri
    client_id_secret = args.client_id_secret
    tenant_id_secret = args.tenant_id_secret
    client_secret_name = args.client_secret_name
    workspace_path_1 = args.workspace_path_1
    workspace_path_2 = args.workspace_path_2

    #================================================================================= MAIN EXECUTION ====================================================================================================================
    kv_dns = kv_uri.replace('https://', '').replace('/', '')
    workspace_id_ws_1 = workspace_path_1.split('//')[1].split('@')[0]
    workspace_guid_ws_1 = workspace_id_ws_1.replace('-', '')
    account_name_ws_1 = f"{workspace_guid_ws_1}.z{workspace_guid_ws_1[:2]}"

    token = get_api_token_via_akv(kv_uri, client_id_secret, tenant_id_secret, client_secret_name)

    # DNS resolution check — shows whether traffic routes via Private Endpoint or public internet
    resolve_dns(kv_dns)
    blob_ips = resolve_dns('onelake.blob.fabric.microsoft.com')
    dfs_ips = resolve_dns('onelake.dfs.fabric.microsoft.com')

    print('\n--- Reading Delta Table ---')
    dt = DeltaTable(
        workspace_path_1,
        storage_options={
            'bearer_token': token,
            'use_fabric_endpoint': 'true',
            'account_name': account_name_ws_1
        }
    )

    df = dt.to_pandas()

    print(df.head())


    """
    the deltalake library won't work because it resolves to onelake

    ### Read from Private-Workspace-Two
    print('\n--- Reading Delta Table on private-workspace-two ---')
    # abfss://bd130da3-e9c1-4922-be03-e2560fc6465c@onelake.dfs.fabric.microsoft.com/22568ace-9152-47bf-883c-0c8c59b060eb/Tables/insider_transactions
    path = 'abfss://bd130da3-e9c1-4922-be03-e2560fc6465c@onelake.dfs.fabric.microsoft.com/22568ace-9152-47bf-883c-0c8c59b060eb/Tables/insider_transactions'

    dt = DeltaTable(
         'abfss://bd130da3-e9c1-4922-be03-e2560fc6465c@onelake.dfs.fabric.microsoft.com/22568ace-9152-47bf-883c-0c8c59b060eb/Tables/insider_transactions',
         storage_options={
             'bearer_token': token,
             'use_fabric_endpoint': 'true',
             'endpoint': 'https://bd130da3e9c14922be03e2560fc6465c.zbd.blob.fabric.microsoft.com'
         }
     )

    df = dt.to_pandas()

    print(df.head())
    """

    # --- Reading from private-workspace-two via DFS REST API ---
    print('\n--- Reading from private-workspace-two via DFS REST API ---')

    # Build SPN credential from Key Vault
    kv_credential = DefaultAzureCredential()
    kv_client = SecretClient(vault_url=kv_uri, credential=kv_credential)
    spn_credential = ClientSecretCredential(
        kv_client.get_secret(tenant_id_secret).value,
        kv_client.get_secret(client_id_secret).value,
        kv_client.get_secret(client_secret_name).value
    )

    # Get a bearer token for the DFS REST API call
    token = spn_credential.get_token('https://storage.azure.com/.default').token

    # Workspace 2 DFS endpoint
    filesystem = workspace_path_2.split('//')[1].split('@')[0]
    workspace_guid = filesystem.replace('-', '')
    dfs_base = f"https://{workspace_guid}.z{workspace_guid[:2]}.dfs.fabric.microsoft.com"
    path = workspace_path_2.split('.com/')[1]

    # List files via SDK (DFS works)
    service_client = DataLakeServiceClient(account_url=dfs_base, credential=spn_credential)

    headers = {"Authorization": f"Bearer {token}"}

    # List files (SDK works for this)
    fs_client = service_client.get_file_system_client(filesystem)
    paths = fs_client.get_paths(path=path)
    parquet_files = [p.name for p in paths if p.name.endswith('.parquet')]
    print(f"  Found {len(parquet_files)} parquet files")

    # Download via DFS endpoint directly (bypasses blob cert issue)
    dfs = []
    for pf in parquet_files:
        resp = requests.get(f"{dfs_base}/{filesystem}/{pf}", headers=headers)
        if resp.status_code == 200:
            table = pq.read_table(io.BytesIO(resp.content))
            dfs.append(table.to_pandas())
        else:
            print(f"  Error {resp.status_code}: {resp.text}")

    if dfs:
        df = pd.concat(dfs, ignore_index=True)
        print(df.head())


if __name__ == '__main__':
    main()
