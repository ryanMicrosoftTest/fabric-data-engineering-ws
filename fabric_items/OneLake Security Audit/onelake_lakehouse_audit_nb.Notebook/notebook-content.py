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

# # OneLake Security Audit — Lakehouse + Workspace Role Audit
# 
# Read-only audit of **every OneLake data access role** on a target lakehouse, plus **every
# workspace-level role assignment** on the workspace this notebook lives in. Nothing is created,
# changed, or deleted.
# 
# Powered by `onelake_security.lakehouse_audit` and `onelake_security.workspace_audit` (v0.4.0+).
# 
# **Two access planes, both audited**
# 
# | Plane | What it controls | Section |
# |---|---|---|
# | OneLake data access roles | Table / column / row access inside one lakehouse | 1–5 |
# | Workspace roles | Admin, Member, Contributor, Viewer over the whole workspace | 6 |
# 
# A workspace **Admin** sits above every OneLake role — so auditing table grants without auditing
# workspace roles misses the people with the broadest access.
# 
# **What it produces**
# 
# | Output | Description |
# |---|---|
# | Summary | Role count, member count, orphaned roles, RLS/CLS coverage |
# | Role detail | One row per role × path × member — friendly Entra names, not bare GUIDs |
# | Effective access | One row per role × path × **person**, security groups expanded |
# | Workspace roles | Every Admin/Member/Contributor/Viewer grant, groups expanded to people |
# | `audit.*` | JSON / CSV / Markdown snapshot of the lakehouse roles |
# | `workspace_roles.*` | JSON / CSV / Markdown snapshot of the workspace roles |
# 
# **Friendly names**
# 
# Both APIs identify principals by **object ID**. `EntraDirectoryClient` resolves each ID through
# Microsoft Graph into a display name, a real object type (User / Group / ServicePrincipal), and a
# UPN, then expands security groups into the principals inside them. IDs that cannot be resolved
# carry a reason (deleted, cross-tenant, or not visible) instead of a silent `n/a`.
# 
# **Parameters**
# - `target_workspace_id` / `target_lakehouse_id` / `target_lakehouse_name` — the lakehouse to audit
# - `workspace_audit_target` — workspace to audit for roles; blank means *the workspace this notebook is running in*
# - `output_directory` — ABFSS folder where the report files are written
# - `kv_uri`, `client_id_secret`, `tenant_id_secret`, `client_secret_name` — SPN credentials in Azure Key Vault
# - `resolve_entra_names` — resolve member object IDs to Entra display names
# - `expand_entra_groups` — expand each group into its users
# - `include_raw` — embed the untouched API payload in the JSON output
# - `write_delta_table` — also append the flattened rows to a Delta table for audit history
# 
# **Permissions**
# The SPN needs **Contributor or higher on the workspace** (to read role assignments) and Graph
# `Directory.Read.All` — or `User.Read.All` + `Group.Read.All` + `GroupMember.Read.All` — for name
# resolution and group expansion.


# PARAMETERS CELL ********************

### Parameters — overridden by the pipeline at runtime

# Lakehouse being audited
target_workspace_id = 'a8cbda3d-903e-4154-97d9-9a91c95abb42'
target_lakehouse_id = '0386880f-c134-41be-923c-00150c5fbafe'
target_lakehouse_name = 'healthcare_lakehouse'

# Workspace whose role assignments are audited.
# Leave blank to audit the workspace this notebook is running in.
workspace_audit_target = ''

# Where the audit files are written (ABFSS path in OneLake)
output_directory = (
    f'abfss://{target_workspace_id}@onelake.dfs.fabric.microsoft.com/'
    f'{target_lakehouse_id}/Files/onelake-security-audits'
)

# Credential info
kv_uri = 'https://kvfabricprodeus2rh.vault.azure.net/'
client_id_secret = 'fuam-spn-client-id'
tenant_id_secret = 'fuam-spn-tenant-id'
client_secret_name = 'fuam-spn-secret'

# Output options
include_raw = 'false'           # embed raw API payload in the JSON report
resolve_entra_names = 'true'    # resolve member object IDs to Entra display names
expand_entra_groups = 'true'    # expand each group into the users inside it
write_delta_table = 'false'     # append flattened rows to a Delta history table

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from azure.identity import ClientSecretCredential
import notebookutils
import uuid
import pandas as pd

import onelake_security
from onelake_security.api_client import OneLakeSecurityClient
from onelake_security.entra_directory import EntraDirectoryClient, DirectoryLookupError
from onelake_security.lakehouse_audit import (
    audit_lakehouse,
    resolve_member_names,
    expand_group_members,
)
from onelake_security.workspace_audit import audit_workspace_roles

print(f'onelake_security version: {onelake_security.__version__}')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Authentication — SPN via Azure Key Vault

client_id = notebookutils.credentials.getSecret(kv_uri, client_id_secret)
tenant_id = notebookutils.credentials.getSecret(kv_uri, tenant_id_secret)
client_secret = notebookutils.credentials.getSecret(kv_uri, client_secret_name)

credential = ClientSecretCredential(tenant_id, client_id, client_secret)
api_token = credential.get_token('https://analysis.windows.net/powerbi/api/.default').token

# Graph token — used to resolve member object IDs to friendly Entra names
graph_token = credential.get_token('https://graph.microsoft.com/.default').token

run_id = str(uuid.uuid4())
print(f'Authenticated | Run ID: {run_id}')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 1 — Run the audit
# 
# A single read against `GET /dataAccessRoles` is flattened into a `LakehouseAuditReport`.

# CELL ********************

client = OneLakeSecurityClient(api_token=api_token)

report = audit_lakehouse(
    client=client,
    workspace_id=target_workspace_id,
    lakehouse_item_id=target_lakehouse_id,
    lakehouse_name=target_lakehouse_name,
)

print(f'Audited  : {report.lakehouse_name} ({report.lakehouse_item_id})')
print(f'Workspace: {report.workspace_id}')
print(f'Captured : {report.captured_at.isoformat()}')
print(f'ETag     : {report.etag}')
print(f'Roles    : {report.role_count}')
print(f'Members  : {report.total_members}')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 2 — Resolve member object IDs to friendly Entra names
# 
# The roles API hands back object IDs only. `EntraDirectoryClient` maps each one to a display name
# and a real object type, then expands every security group into the principals inside it — so the
# audit names people and groups instead of GUIDs.

# CELL ********************

directory = None
resolution_error = None
unresolved = []
group_expansion = {}

if resolve_entra_names.lower() == 'true':
    try:
        directory = EntraDirectoryClient(graph_token=graph_token)
        unresolved = resolve_member_names(report, directory)
        resolved_count = report.total_members - len(unresolved)
        print(f'Resolved names for {resolved_count} of {report.total_members} member(s)')

        for item in unresolved:
            print(f'  unresolved {item.object_id} — {item.reason}')

        if expand_entra_groups.lower() == 'true':
            group_expansion = expand_group_members(report, directory)
            for group_id, count in group_expansion.items():
                print(f'  group {group_id} -> {count} principal(s)')
    except DirectoryLookupError as e:
        resolution_error = str(e)
        print(f'Name resolution skipped — {resolution_error}')
else:
    print('Name resolution disabled')

member_view = pd.DataFrame([
    {
        'role_name': r.name,
        'member_name': m.friendly_name,
        'member_type': m.object_type or 'unknown',
        'sign_in': m.user_principal_name or '',
        'object_id': m.object_id,
        'in_group': m.group_member_count,
        'note': m.resolution_note or '',
    }
    for r in report.roles
    for m in r.entra_members
])

if member_view.empty:
    print('No Entra members assigned to any role.')
else:
    display(spark.createDataFrame(member_view))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 3 — Summary by role
# 
# One row per role: how many members it has, how many paths it grants, and whether row-level or column-level security is applied.

# CELL ********************

role_summary = pd.DataFrame([
    {
        'role_name': r.name,
        'role_id': r.role_id,
        'members': r.member_count,
        'is_orphaned': r.is_orphaned,
        'paths_granted': len(r.permissions),
        'has_rls': any(p.has_row_security for p in r.permissions),
        'has_cls': any(p.has_column_security for p in r.permissions),
        'paths': ', '.join(r.table_paths),
    }
    for r in report.roles
])

if role_summary.empty:
    print('No data access roles defined on this lakehouse.')
else:
    display(spark.createDataFrame(role_summary))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 4 — Full detail: role × path × member
# 
# The fully flattened grain — every effective grant, including the exact column list (CLS) and row predicate (RLS) per path.

# CELL ********************

rows = report.to_rows()
detail_df = pd.DataFrame(rows)

if detail_df.empty:
    print('No grants to display.')
else:
    display(spark.createDataFrame(
        detail_df[[
            'role_name', 'path', 'actions', 'effect',
            'column_names', 'row_filter',
            'member_kind', 'member_display_name', 'member_object_type',
            'member_upn', 'member_group_member_count', 'member_group_members',
            'member_object_id', 'member_resolution_note',
            'member_source_path', 'member_item_access', 'is_orphaned',
        ]]
    ))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 5 — Effective access: who can actually read what
# 
# Security groups are exploded into the principals inside them, so each row is one person against
# one path. This is the grain to hand an auditor asking "who can see this table?".

# CELL ********************

effective_rows = report.to_effective_user_rows()
effective_df = pd.DataFrame(effective_rows)

if effective_df.empty:
    print('No effective grants to display.')
else:
    display(spark.createDataFrame(
        effective_df[[
            'role_name', 'path', 'actions', 'column_names', 'row_filter',
            'principal_name', 'principal_type', 'principal_upn',
            'granted_via', 'principal_object_id',
        ]]
    ))
    print(f'Distinct principals with access: {effective_df["principal_object_id"].nunique()}')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 6 — Workspace-level roles
# 
# Everything above governs data *inside* one lakehouse. This section audits the other access plane:
# who holds **Admin / Member / Contributor / Viewer** on the workspace itself.
# 
# By default this audits **the workspace this notebook is running in**, resolved from the notebook
# runtime context. Set `workspace_audit_target` to audit a different workspace.
# 
# Security groups are expanded to the people inside them, and each principal is then collapsed to a
# single row at their **highest** effective role — so a user who is a direct Viewer but also sits in
# an Admin group is correctly reported as an Admin.

# CELL ********************

# Resolve which workspace to audit — the current one unless overridden
context = notebookutils.runtime.context
current_workspace_id = (
    workspace_audit_target
    or context.get('currentWorkspaceId')
    or context.get('workspaceId')
    or target_workspace_id
)

workspace_report = audit_workspace_roles(
    client=client,
    workspace_id=current_workspace_id,
    directory=directory,
    expand_groups=(expand_entra_groups.lower() == 'true'),
)

print(f'Workspace   : {workspace_report.workspace_name} ({workspace_report.workspace_id})')
print(f'Assignments : {workspace_report.assignment_count}')
print(f'By role     : {workspace_report.role_counts}')
print(f'Elevated    : {len(workspace_report.elevated_assignments)} (Admin/Member)')
print(f'Groups      : {len(workspace_report.group_assignments)}'
      f'  empty: {len(workspace_report.empty_groups)}'
      f'  not expanded: {len(workspace_report.unexpanded_groups)}')

workspace_rows = workspace_report.to_rows()
workspace_df = pd.DataFrame(workspace_rows)

if workspace_df.empty:
    print('No workspace role assignments returned — check the SPN has access to this workspace.')
else:
    display(spark.createDataFrame(
        workspace_df[[
            'role', 'principal_name', 'principal_type', 'principal_upn',
            'is_elevated', 'group_member_count', 'group_members',
            'principal_object_id', 'aad_app_id',
        ]]
    ))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### 6a — Every user with workspace access
# 
# One row per person at their highest effective role, with the grant path that got them there.
# Group grants are flattened, so this is the list to hand an auditor asking *"who can get into this
# workspace?"*.

# CELL ********************

workspace_effective_rows = workspace_report.to_effective_user_rows()
workspace_effective_df = pd.DataFrame(workspace_effective_rows)

if workspace_effective_df.empty:
    print('No effective workspace principals to display.')
else:
    display(spark.createDataFrame(
        workspace_effective_df[[
            'effective_role', 'principal_name', 'principal_type',
            'principal_upn', 'granted_via', 'principal_object_id',
        ]]
    ))

    print()
    for role_name in ['Admin', 'Member', 'Contributor', 'Viewer']:
        holders = [
            r['principal_name']
            for r in workspace_effective_rows
            if r['effective_role'] == role_name
        ]
        if holders:
            print(f'{role_name:<12} {len(holders):>2}  {", ".join(holders)}')

    empty = workspace_report.empty_groups
    if empty:
        print()
        print('Group grants that reach nobody (empty security groups):')
        for a in empty:
            print(f'  {a.role:<12} {a.friendly_name}')

from IPython.display import Markdown
workspace_markdown = workspace_report.to_markdown()
display(Markdown(workspace_markdown))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 7 — Findings
# 
# Governance signals worth reviewing: roles that grant access to nobody, roles with unrestricted paths, member IDs that could not be resolved, and where fine-grained security is (and isn't) applied.

# CELL ********************

orphaned = report.orphaned_roles
wildcard_roles = [r for r in report.roles if any(p.path in ('*', '/*') for p in r.permissions)]
rls_roles = [r for r in report.roles if any(p.has_row_security for p in r.permissions)]
cls_roles = [r for r in report.roles if any(p.has_column_security for p in r.permissions)]
unresolved_members = report.unresolved_members

print(f'Roles................ {report.role_count}')
print(f'Total members........ {report.total_members}')
print(f'Orphaned (0 members). {len(orphaned)}  {[r.name for r in orphaned]}')
print(f'Wildcard path grants. {len(wildcard_roles)}  {[r.name for r in wildcard_roles]}')
print(f'Roles using RLS...... {len(rls_roles)}  {[r.name for r in rls_roles]}')
print(f'Roles using CLS...... {len(cls_roles)}  {[r.name for r in cls_roles]}')
print(f'Unresolved members... {len(unresolved_members)}  {[m.object_id for m in unresolved_members]}')

print()
for r in report.roles:
    for p in r.permissions:
        if p.has_row_security:
            print(f'RLS  {r.name} :: {p.path} -> {p.row_filter}')
        if p.has_column_security:
            print(f'CLS  {r.name} :: {p.path} -> {", ".join(p.column_names)}')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 8 — Markdown review summary

# CELL ********************

from IPython.display import Markdown

markdown_report = report.to_markdown()
display(Markdown(markdown_report))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 9 — Save the audit to disk
# 
# Writes timestamped and `_latest` copies of both audits to OneLake: the lakehouse role snapshot
# (`.json` / `.csv` / `_effective_access.csv` / `.md`) and the workspace role snapshot
# (`_workspace_roles.json` / `.csv` / `_effective.csv` / `.md`).
# 
# > `save_report()` in the library writes to a local filesystem path. In Spark we render the same
# > content via `to_json()` / `to_rows()` / `to_markdown()` and write it to ABFSS with
# > `notebookutils.fs.put`.

# CELL ********************

import csv
import io


def to_csv(records):
    buffer = io.StringIO()
    if records:
        writer = csv.DictWriter(buffer, fieldnames=list(records[0].keys()))
        writer.writeheader()
        writer.writerows(records)
    return buffer.getvalue()


stamp = report.captured_at.strftime('%Y%m%dT%H%M%SZ')
prefix = f'{output_directory}/{target_lakehouse_name}'
ws_prefix = f'{output_directory}/{workspace_report.workspace_name or current_workspace_id}'

json_content = report.to_json(include_raw=(include_raw.lower() == 'true'))
csv_content = to_csv(rows)
effective_content = to_csv(effective_rows)

ws_json_content = workspace_report.to_json()
ws_csv_content = to_csv(workspace_rows)
ws_effective_content = to_csv(workspace_effective_rows)

artifacts = {
    # Lakehouse data access roles
    f'{prefix}_{stamp}.json': json_content,
    f'{prefix}_{stamp}.csv': csv_content,
    f'{prefix}_{stamp}_effective_access.csv': effective_content,
    f'{prefix}_{stamp}.md': markdown_report,
    f'{prefix}_latest.json': json_content,
    f'{prefix}_latest.csv': csv_content,
    f'{prefix}_latest_effective_access.csv': effective_content,
    f'{prefix}_latest.md': markdown_report,
    # Workspace roles
    f'{ws_prefix}_{stamp}_workspace_roles.json': ws_json_content,
    f'{ws_prefix}_{stamp}_workspace_roles.csv': ws_csv_content,
    f'{ws_prefix}_{stamp}_workspace_roles_effective.csv': ws_effective_content,
    f'{ws_prefix}_{stamp}_workspace_roles.md': workspace_markdown,
    f'{ws_prefix}_latest_workspace_roles.json': ws_json_content,
    f'{ws_prefix}_latest_workspace_roles.csv': ws_csv_content,
    f'{ws_prefix}_latest_workspace_roles_effective.csv': ws_effective_content,
    f'{ws_prefix}_latest_workspace_roles.md': workspace_markdown,
}

for path, content in artifacts.items():
    notebookutils.fs.put(path, content, True)
    print(f'Wrote {path}')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

### Optional — append the flattened rows to a Delta table for audit history

if write_delta_table.lower() == 'true' and rows:
    audit_table_path = (
        f'abfss://{target_workspace_id}@onelake.dfs.fabric.microsoft.com/'
        f'{target_lakehouse_id}/Tables/onelake_security_audit'
    )
    history_df = spark.createDataFrame(detail_df.assign(run_id=run_id))
    history_df.write.format('delta').mode('append').save(audit_table_path)
    print(f'Appended {history_df.count()} row(s) to {audit_table_path}')
else:
    print('Delta history table write skipped.')

print(f'Audit complete | Run ID: {run_id}')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
