# Fabric notebook source
# OneLake Role Creation Notebook
# ================================
# Thin wrapper that reads role-definition YAMLs from the lakehouse,
# parses them, and applies them via the OneLake Security library.
#
# Parameters (passed from pipeline):
#   - workspace_id: Target workspace GUID
#   - lakehouse_id: Target lakehouse GUID
#   - kv_uri: Azure Key Vault URI
#   - client_id_secret: AKV secret name for SPN client ID
#   - tenant_id_secret: AKV secret name for SPN tenant ID
#   - client_secret_name: AKV secret name for SPN client secret
#   - yaml_directory: Lakehouse path to role-definition YAMLs (default: Files/role-definitions)
#   - validate_first: Run dry-run before applying (default: true)

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
kv_uri = ""
client_id_secret = ""
tenant_id_secret = ""
client_secret_name = ""
yaml_directory = "Files/role-definitions"
validate_first = "true"

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

# Get SPN credentials from Key Vault
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
from onelake_security.yaml_parser import parse_role_definition
from onelake_security.file_tracker import FileTracker, compute_content_hash

yaml_files = notebookutils.fs.ls(yaml_directory)
yaml_files = [f for f in yaml_files if f.name.endswith((".yml", ".yaml"))]

print(f"📂 Found {len(yaml_files)} YAML file(s) in {yaml_directory}")

# Parse all role definitions
role_definitions = []
parse_errors = []

for f in yaml_files:
    try:
        content = notebookutils.fs.head(f.path, maxBytes=1024 * 1024)
        role_def = parse_role_definition(content)
        role_definitions.append((f.name, content, role_def))
        print(f"   ✅ {f.name} → {role_def.name} (state={role_def.state.value})")
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

# Apply role definitions
from onelake_security.api_client import OneLakeSecurityClient
from onelake_security.workflow_service import process_role_definitions
from onelake_security.audit import AuditLogger

client = OneLakeSecurityClient(api_token=api_token)
audit = AuditLogger(correlation_id=run_id)

if role_definitions:
    defs = [rd for (_, _, rd) in role_definitions]

    result = process_role_definitions(
        client=client,
        workspace_id=workspace_id,
        item_id=lakehouse_id,
        role_definitions=defs,
        validate_first=(validate_first.lower() == "true"),
    )

    # Log each role operation
    for file_name, content, role_def in role_definitions:
        audit.log(
            operation=f"ROLE_{'DELETED' if role_def.state.value == 'absent' else 'APPLIED'}",
            role_name=role_def.name,
            workspace_id=workspace_id,
            item_id=lakehouse_id,
            source_file=file_name,
            content_hash=compute_content_hash(content),
            success=result.success,
            error=result.error,
        )

    if result.success:
        print(f"\n🎉 Success: {result.roles_applied} role(s) applied")
    else:
        print(f"\n❌ Failed: {result.error}")
else:
    print("\n⏭️ No role definitions to process")

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

# Output audit records (as DataFrame for visibility, and for SQL DB write)
if audit.records:
    audit_df = spark.createDataFrame(audit.to_dicts())
    display(audit_df)

    # Return result for pipeline orchestration
    if not result.success:
        raise Exception(f"Role creation failed: {result.error}")

print(f"✅ Run complete | {run_id}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
