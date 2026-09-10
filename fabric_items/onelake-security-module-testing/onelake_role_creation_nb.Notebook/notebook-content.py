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

# # OneLake Role Creation Notebook
# 
# Thin wrapper that reads role-definition YAMLs from the lakehouse, parses them, and applies them via the OneLake Security library.
# 
# **Parameters (passed from pipeline):**
# - `target_workspace_id`: Target workspace GUID (where roles are created)
# - `target_lakehouse_id`: Target lakehouse GUID (where roles are created)
# - `kv_uri`: Azure Key Vault URI
# - `client_id_secret`: AKV secret name for SPN client ID
# - `tenant_id_secret`: AKV secret name for SPN tenant ID
# - `client_secret_name`: AKV secret name for SPN client secret
# - `yaml_directory`: Path to role-definition YAMLs in the notebook's default lakehouse (default: `Files/role-definitions`)
# - `validate_first`: Run dry-run before applying (default: `true`)

# PARAMETERS CELL ********************

### Parameters — these are overridden by the pipeline at runtime

# Target lakehouse — where the OneLake security roles will be CREATED
target_workspace_id = 'a8cbda3d-903e-4154-97d9-9a91c95abb42'
target_lakehouse_id = '0386880f-c134-41be-923c-00150c5fbafe'

# Source — ABFSS path to the YAML definition files (no lakehouse attachment needed)
# Format: abfss://<workspace_id>@onelake.dfs.fabric.microsoft.com/<lakehouse_id>/Files/role-definitions
yaml_directory = 'abfss://9db9447e-fecc-4c22-8b9b-3ceed9e5925c@onelake.dfs.fabric.microsoft.com/045f7d55-2ef1-4b4c-b462-acec26f30904/Files/role-definitions'

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
import pyodbc
import pandas as pd
from onelake_security.yaml_parser import parse_role_definition
from onelake_security.file_tracker import FileTracker, compute_content_hash
from onelake_security.api_client import OneLakeSecurityClient
from onelake_security.workflow_service import process_role_definitions, RoleWorkflowResult
from onelake_security.audit import AuditLogger

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Authentication — SPN via Azure Key Vault


# Get SPN credentials from Key Vault
client_id = notebookutils.credentials.getSecret(kv_uri, client_id_secret)
tenant_id = notebookutils.credentials.getSecret(kv_uri, tenant_id_secret)
client_secret = notebookutils.credentials.getSecret(kv_uri, client_secret_name)

credential = ClientSecretCredential(tenant_id, client_id, client_secret)
api_token = credential.get_token("https://analysis.windows.net/powerbi/api/.default").token

run_id = str(uuid.uuid4())
print(f"Authenticated | Run ID: {run_id}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Read YAML files from lakehouse
yaml_files = notebookutils.fs.ls(yaml_directory)
yaml_files = [f for f in yaml_files if f.name.endswith((".yml", ".yaml"))]

print(f"📂 Found {len(yaml_files)} YAML file(s) in {yaml_directory}")

# Parse all role definitions
role_definitions = []
parse_errors = []

for f in yaml_files:
    try:
        content = notebookutils.fs.head(f.path, 1024 * 1024)
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

client = OneLakeSecurityClient(api_token=api_token)
audit = AuditLogger(correlation_id=run_id)

result = RoleWorkflowResult(success=True, roles_applied=0)
if role_definitions:
    defs = [rd for (_, _, rd) in role_definitions]

    result = process_role_definitions(
        client=client,
        workspace_id=target_workspace_id,
        item_id=target_lakehouse_id,
        role_definitions=defs,
        validate_first=(validate_first.lower() == "true"),
    )

    # Log each role operation
    for file_name, content, role_def in role_definitions:
        audit.log(
            operation=f"ROLE_{'DELETED' if role_def.state.value == 'absent' else 'APPLIED'}",
            role_name=role_def.name,
            workspace_id=target_workspace_id,
            item_id=target_lakehouse_id,
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

### Write audit records to Fabric SQL Database

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

    for file_name, content, role_def in role_definitions:
        has_rls = any(t.row_filter for t in role_def.tables) if role_def.tables else False
        has_cls = any(t.column_names and t.column_names != ['*'] for t in role_def.tables) if role_def.tables else False
        cursor.execute(
            "INSERT INTO dbo.role_creation_audit_tbl "
            "(correlation_id, operation, role_name, target_workspace_id, target_lakehouse_id, "
            " source_file, content_hash, success, error, role_state, table_count, has_rls, has_cls) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            run_id,
            f"ROLE_{'DELETED' if role_def.state.value == 'absent' else 'APPLIED'}",
            role_def.name,
            target_workspace_id,
            target_lakehouse_id,
            file_name,
            compute_content_hash(content),
            1 if result.success else 0,
            result.error,
            role_def.state.value,
            len(role_def.tables),
            1 if has_rls else 0,
            1 if has_cls else 0,
        )

    for file_name, error in parse_errors:
        cursor.execute(
            "INSERT INTO dbo.role_creation_audit_tbl "
            "(correlation_id, operation, role_name, target_workspace_id, target_lakehouse_id, "
            " source_file, success, error) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            run_id, 'PARSE_ERROR', '(unknown)', target_workspace_id,
            target_lakehouse_id, file_name, 0, error,
        )

    conn.commit()
    cursor.close()
    conn.close()
    print(f"📝 {len(audit.records)} audit record(s) written to {audit_sql_database}.dbo.role_creation_audit_tbl")

    if not result.success:
        raise Exception(f"Role creation failed: {result.error}")

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
