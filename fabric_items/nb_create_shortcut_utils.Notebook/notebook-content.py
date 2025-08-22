# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "environment": {
# META       "environmentId": "4d3272ae-0804-81fa-43d0-eb1439b80f8c",
# META       "workspaceId": "00000000-0000-0000-0000-000000000000"
# META     }
# META   }
# META }

# CELL ********************

shortcut_name = "healthdata"
workspace_id = "a8cbda3d-903e-4154-97d9-9a91c95abb42"
lakehouse_id = "4bd2cd19-0177-4fa9-9ab7-a67a69233f72"
shortcut_location = "Files"
target_type = "AdlsGen2"
target_connection_id = "1f126619-984e-4ab2-b5fa-42d5680242f7"
target_location = "https://healthsaadlsrheus.dfs.core.windows.net"
target_subpath = "/healthdata"
data_type = "Folder"
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

from fabric_utils.credential_utils import CredentuialUtils
from fabric_utils.shortcut import ShortcutUtils

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

kv_uri = "https://kvfabricprodeus2rh.vault.azure.net/"
tenant_id_secret = "tenantID"
client_id_secret = "spn-client-id"

client_id = notebookutils.credentials.getSecret(kv_uri, client_id_secret)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# get api token
token = CredentuialUtils.get_api_token_via_akv(kv_uri, client_id_secret, tenant_id_secret, client_secret_name)

# build shortcut payload
shortcut_payload = ShortcutUtils.build_shortcut_create_payload(shortcut_name, target_type, target_connection_id,target_location, target_subpath, shortcut_location)



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

ShortcutUtils.create_shortcut(workspace_id=workspace_id, item_id=lakehouse_id, target=shortcut_payload, api_token=token)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
