# Fabric notebook source
# OneLake User-Role Mapping Notebook
# ====================================
# Thin wrapper that reads user-mapping YAMLs from the lakehouse,
# parses them, and applies membership changes via the OneLake Security library.
#
# Parameters (passed from pipeline):
#   - workspace_id: Target workspace GUID
#   - lakehouse_id: Target lakehouse GUID
#   - tenant_id: Entra tenant ID (stamped on every member)
#   - kv_uri: Azure Key Vault URI
#   - client_id_secret: AKV secret name for SPN client ID
#   - tenant_id_secret: AKV secret name for SPN tenant ID
#   - client_secret_name: AKV secret name for SPN client secret
#   - yaml_directory: Lakehouse path to role-mapping YAMLs (default: Files/role-mappings)

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

# Parameters — these are overridden by the pipeline at runtime
workspace_id = ""
lakehouse_id = ""
tenant_id = ""
kv_uri = ""
client_id_secret = ""
tenant_id_secret = ""
client_secret_name = ""
yaml_directory = "Files/role-mappings"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Authentication — SPN via Azure Key Vault
from azure.identity import ClientSecretCredential
import notebookutils
import uuid

_client_id = notebookutils.credentials.getSecret(kv_uri, client_id_secret)
_tenant_id = notebookutils.credentials.getSecret(kv_uri, tenant_id_secret)
_client_secret = notebookutils.credentials.getSecret(kv_uri, client_secret_name)

credential = ClientSecretCredential(_tenant_id, _client_id, _client_secret)
api_token = credential.get_token("https://analysis.windows.net/powerbi/api/.default").token

run_id = str(uuid.uuid4())
print(f"✅ Authenticated | Run ID: {run_id}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Read YAML files from lakehouse
from onelake_security.yaml_parser import parse_user_mapping
from onelake_security.file_tracker import compute_content_hash

yaml_files = notebookutils.fs.ls(yaml_directory)
yaml_files = [f for f in yaml_files if f.name.endswith((".yml", ".yaml"))]

print(f"📂 Found {len(yaml_files)} YAML file(s) in {yaml_directory}")

# Parse all user mappings
user_mappings = []
parse_errors = []

for f in yaml_files:
    try:
        content = notebookutils.fs.head(f.path, maxBytes=1024 * 1024)
        mapping = parse_user_mapping(content)
        user_mappings.append((f.name, content, mapping))
        member_count = len(mapping.entra_members)
        print(f"   ✅ {f.name} → {mapping.role_name} ({member_count} member(s))")
    except Exception as e:
        parse_errors.append((f.name, str(e)))
        print(f"   ❌ {f.name} → Parse error: {e}")

if parse_errors:
    print(f"\n⚠️ {len(parse_errors)} file(s) failed to parse")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Apply user mappings
from onelake_security.api_client import OneLakeSecurityClient
from onelake_security.workflow_service import process_user_mappings
from onelake_security.audit import AuditLogger

client = OneLakeSecurityClient(api_token=api_token)
audit = AuditLogger(correlation_id=run_id)

if user_mappings:
    mappings = [m for (_, _, m) in user_mappings]

    result = process_user_mappings(
        client=client,
        workspace_id=workspace_id,
        item_id=lakehouse_id,
        tenant_id=tenant_id,
        user_mappings=mappings,
    )

    # Log each mapping operation
    for file_name, content, mapping in user_mappings:
        member_ids = [m.object_id for m in mapping.entra_members]
        audit.log(
            operation="MAPPING_APPLIED",
            role_name=mapping.role_name,
            workspace_id=workspace_id,
            item_id=lakehouse_id,
            source_file=file_name,
            content_hash=compute_content_hash(content),
            success=result.success,
            error=result.error,
        )

    if result.success:
        print(f"\n🎉 Success: {result.mappings_applied} mapping(s) applied")
    else:
        print(f"\n❌ Failed: {result.error}")
else:
    print("\n⏭️ No user mappings to process")

# Log parse errors
for file_name, error in parse_errors:
    audit.log(
        operation="PARSE_ERROR",
        role_name="(unknown)",
        workspace_id=workspace_id,
        item_id=lakehouse_id,
        source_file=file_name,
        success=False,
        error=error,
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Output audit records
if audit.records:
    audit_df = spark.createDataFrame(audit.to_dicts())
    display(audit_df)

    if not result.success:
        raise Exception(f"User mapping failed: {result.error}")

print(f"✅ Run complete | {run_id}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
