# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "environment": {
# META       "environmentId": "6906e97a-1cd0-b77a-4804-38d004c238ac",
# META       "workspaceId": "00000000-0000-0000-0000-000000000000"
# META     }
# META   }
# META }

# MARKDOWN ********************

# # OneLake User-Role Mapping Notebook
# 
# Thin wrapper that reads user-mapping YAMLs from the lakehouse, parses them, and applies membership changes via the OneLake Security library.
# 
# **Parameters (passed from pipeline):**
# - `target_workspace_id`: Target workspace GUID (where roles exist)
# - `target_lakehouse_id`: Target lakehouse GUID (where roles exist)
# - `tenant_id`: Entra tenant ID (stamped on every member)
# - `kv_uri`: Azure Key Vault URI
# - `client_id_secret`: AKV secret name for SPN client ID
# - `tenant_id_secret`: AKV secret name for SPN tenant ID
# - `client_secret_name`: AKV secret name for SPN client secret
# - `yaml_directory`: Path to role-mapping YAMLs in the notebook's default lakehouse (default: `Files/role-mappings`)

# PARAMETERS CELL ********************

### Parameters — these are overridden by the pipeline at runtime

# Target lakehouse — where the OneLake security roles will be CREATED
target_workspace_id = 'a8cbda3d-903e-4154-97d9-9a91c95abb42'
target_lakehouse_id = '0386880f-c134-41be-923c-00150c5fbafe'

# Source — ABFSS path to the YAML definition files (no lakehouse attachment needed)
# Format: abfss://<workspace_id>@onelake.dfs.fabric.microsoft.com/<lakehouse_id>/Files/role-mappings
yaml_directory = 'abfss://9db9447e-fecc-4c22-8b9b-3ceed9e5925c@onelake.dfs.fabric.microsoft.com/045f7d55-2ef1-4b4c-b462-acec26f30904/Files/role-mappings'

# credential info
kv_uri = 'https://kvfabricprodeus2rh.vault.azure.net/'
client_id_secret = 'fuam-spn-client-id'
tenant_id_secret = 'fuam-spn-tenant-id'
client_secret_name = 'fuam-spn-secret'

# validation check enabled
validate_first = 'true'

# Audit — Fabric SQL Database connection
audit_sql_server = 'ftykynmhjpteverb75ok7vbqwq-pzclthom7yrezc43htxntzmslq.database.fabric.microsoft.com'        # e.g., xyz123.database.fabric.microsoft.com
audit_sql_database = 'onelake-audit-sql-database-940f2eb5-5640-4f63-89f0-05083e7bf107'                          # e.g., governance-audit-db

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from azure.identity import ClientSecretCredential
import notebookutils
import uuid
import struct
import pandas as pd
import pyodbc
from onelake_security.yaml_parser import parse_user_mapping
from onelake_security.file_tracker import compute_content_hash
from onelake_security.api_client import OneLakeSecurityClient
from onelake_security.workflow_service import process_user_mappings
from onelake_security.audit import AuditLogger

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

### Authentication — SPN via Azure Key Vault

client_id = notebookutils.credentials.getSecret(kv_uri, client_id_secret)
tenant_id = notebookutils.credentials.getSecret(kv_uri, tenant_id_secret)
client_secret = notebookutils.credentials.getSecret(kv_uri, client_secret_name)

credential = ClientSecretCredential(tenant_id, client_id, client_secret)
api_token = credential.get_token('https://analysis.windows.net/powerbi/api/.default').token

run_id = str(uuid.uuid4())
print(f"✅ Authenticated | Run ID: {run_id}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

### Read YAML files from lakehouse
yaml_files = notebookutils.fs.ls(yaml_directory)
yaml_files = [f for f in yaml_files if f.name.endswith((".yml", ".yaml"))]

print(f"📂 Found {len(yaml_files)} YAML file(s) in {yaml_directory}")

# Parse all user mappings
user_mappings = []
parse_errors = []

for f in yaml_files:
    try:
        content = notebookutils.fs.head(f.path, 1024 * 1024)
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

### Apply user mappings
client = OneLakeSecurityClient(api_token=api_token)
audit = AuditLogger(correlation_id=run_id)

result = MappingWorkflowResult(success=True, mappings_applied=0)
    if user_mappings:
    mappings = [m for (_, _, m) in user_mappings]

    result = process_user_mappings(
        client=client,
        workspace_id=target_workspace_id,
        item_id=target_lakehouse_id,
        tenant_id=tenant_id,
        user_mappings=mappings,
    )

    # Log each mapping operation
    for file_name, content, mapping in user_mappings:
        audit.log(
            operation="MAPPING_APPLIED",
            role_name=mapping.role_name,
            workspace_id=target_workspace_id,
            item_id=target_lakehouse_id,
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
        workspace_id=target_workspace_id,
        item_id=target_lakehouse_id,
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

# Write audit records to Fabric SQL Database
if audit.records:
    # Display in notebook for visibility
    audit_df = pd.DataFrame(audit.to_dicts())
    display(spark.createDataFrame(audit_df))

    # Write to SQL Database using SPN token
    sql_token = credential.get_token("https://database.windows.net/.default").token
    token_bytes = sql_token.encode("utf-16-le")
    token_struct = struct.pack(f"<I{len(token_bytes)}s", len(token_bytes), token_bytes)

    conn = pyodbc.connect(
        f"Driver={{ODBC Driver 18 for SQL Server}};"
        f"Server={audit_sql_server};"
        f"Database={audit_sql_database};"
        f"Encrypt=yes;TrustServerCertificate=no;",
        attrs_before={1256: token_struct}
    )
    cursor = conn.cursor()

    from onelake_security.models import MemberType
    for file_name, content, mapping in user_mappings:
        members = mapping.entra_members
        cursor.execute(
            "INSERT INTO dbo.role_mapping_audit_tbl "
            "(correlation_id, operation, role_name, target_workspace_id, target_lakehouse_id, "
            " source_file, content_hash, success, error, member_count, user_count, group_count, spn_count) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            run_id,
            'MAPPING_APPLIED',
            mapping.role_name,
            target_workspace_id,
            target_lakehouse_id,
            file_name,
            compute_content_hash(content),
            1 if result.success else 0,
            result.error,
            len(members),
            sum(1 for m in members if m.object_type == MemberType.USER),
            sum(1 for m in members if m.object_type == MemberType.GROUP),
            sum(1 for m in members if m.object_type == MemberType.SERVICE_PRINCIPAL),
        )

    for file_name, error in parse_errors:
        cursor.execute(
            "INSERT INTO dbo.role_mapping_audit_tbl "
            "(correlation_id, operation, role_name, target_workspace_id, target_lakehouse_id, "
            " source_file, success, error) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            run_id, 'PARSE_ERROR', '(unknown)', target_workspace_id,
            target_lakehouse_id, file_name, 0, error,
        )

    conn.commit()
    cursor.close()
    conn.close()
    print(f"📝 {len(audit.records)} audit record(s) written to {audit_sql_database}.dbo.role_mapping_audit_tbl")

    if not result.success:
        raise Exception(f"User mapping failed: {result.error}")

print(f"✅ Run complete | {run_id}")

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
