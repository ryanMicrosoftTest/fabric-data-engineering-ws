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
from onelake_security.workflow_service import process_user_mappings, MappingWorkflowResult
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

# MARKDOWN ********************

# # Changes for Auditing Lakehouse for OneLake Security Roles

# CELL ********************

"""Lakehouse audit — read-only snapshot of a lakehouse's OneLake security roles.

Given a lakehouse (workspace + item), fetches every data access role via the
Fabric API and flattens it into a structured, serializable report that can be
written to disk as JSON, CSV, or Markdown.

This module never mutates roles — it only reads.
"""

from __future__ import annotations

import csv
import json
import os
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional

from onelake_security.api_client import OneLakeSecurityClient


@dataclass(frozen=True)
class AuditedMember:
    """A member assigned to a role, as reported by the API."""

    object_id: str
    object_type: Optional[str] = None
    tenant_id: Optional[str] = None
    source: str = "microsoftEntraMembers"

    def to_dict(self) -> dict:
        return {
            "object_id": self.object_id,
            "object_type": self.object_type,
            "tenant_id": self.tenant_id,
            "source": self.source,
        }


@dataclass(frozen=True)
class AuditedTablePermission:
    """Effective access rules for one path inside a role's decision rule."""

    path: str
    actions: list[str] = field(default_factory=list)
    effect: str = "Permit"
    column_names: Optional[list[str]] = None
    row_filter: Optional[str] = None

    @property
    def has_column_security(self) -> bool:
        return bool(self.column_names)

    @property
    def has_row_security(self) -> bool:
        return bool(self.row_filter)

    def to_dict(self) -> dict:
        return {
            "path": self.path,
            "actions": list(self.actions),
            "effect": self.effect,
            "column_names": list(self.column_names) if self.column_names else None,
            "row_filter": self.row_filter,
            "has_column_security": self.has_column_security,
            "has_row_security": self.has_row_security,
        }


@dataclass(frozen=True)
class AuditedRole:
    """A single data access role, flattened for review."""

    name: str
    role_id: Optional[str] = None
    permissions: list[AuditedTablePermission] = field(default_factory=list)
    entra_members: list[AuditedMember] = field(default_factory=list)
    fabric_item_members: list[dict] = field(default_factory=list)
    raw: Optional[dict] = None

    @property
    def member_count(self) -> int:
        return len(self.entra_members) + len(self.fabric_item_members)

    @property
    def is_orphaned(self) -> bool:
        """True when a role grants access but has no members assigned."""
        return self.member_count == 0

    @property
    def table_paths(self) -> list[str]:
        return [p.path for p in self.permissions]

    def to_dict(self, include_raw: bool = False) -> dict:
        data = {
            "name": self.name,
            "role_id": self.role_id,
            "member_count": self.member_count,
            "is_orphaned": self.is_orphaned,
            "table_paths": self.table_paths,
            "permissions": [p.to_dict() for p in self.permissions],
            "entra_members": [m.to_dict() for m in self.entra_members],
            "fabric_item_members": list(self.fabric_item_members),
        }
        if include_raw:
            data["raw"] = self.raw
        return data


@dataclass
class LakehouseAuditReport:
    """Full audit snapshot of one lakehouse's security roles."""

    workspace_id: str
    item_id: str
    roles: list[AuditedRole] = field(default_factory=list)
    lakehouse_name: Optional[str] = None
    etag: Optional[str] = None
    captured_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    @property
    def role_count(self) -> int:
        return len(self.roles)

    @property
    def orphaned_roles(self) -> list[AuditedRole]:
        """Roles with no members — common security review finding."""
        return [r for r in self.roles if r.is_orphaned]

    @property
    def total_members(self) -> int:
        return sum(r.member_count for r in self.roles)

    def to_dict(self, include_raw: bool = False) -> dict:
        return {
            "workspace_id": self.workspace_id,
            "item_id": self.item_id,
            "lakehouse_name": self.lakehouse_name,
            "etag": self.etag,
            "captured_at": self.captured_at.isoformat(),
            "role_count": self.role_count,
            "total_members": self.total_members,
            "orphaned_role_names": [r.name for r in self.orphaned_roles],
            "roles": [r.to_dict(include_raw=include_raw) for r in self.roles],
        }

    def to_json(self, include_raw: bool = False, indent: int = 2) -> str:
        return json.dumps(self.to_dict(include_raw=include_raw), indent=indent)

    def to_rows(self) -> list[dict]:
        """Flatten to one row per (role, path, member) for CSV/table output."""
        rows: list[dict] = []
        for role in self.roles:
            permissions = role.permissions or [AuditedTablePermission(path="")]
            members: list[Optional[AuditedMember]] = list(role.entra_members) or [None]
            for perm in permissions:
                for member in members:
                    rows.append({
                        "workspace_id": self.workspace_id,
                        "item_id": self.item_id,
                        "lakehouse_name": self.lakehouse_name or "",
                        "captured_at": self.captured_at.isoformat(),
                        "role_name": role.name,
                        "role_id": role.role_id or "",
                        "member_count": role.member_count,
                        "is_orphaned": role.is_orphaned,
                        "path": perm.path,
                        "effect": perm.effect,
                        "actions": ";".join(perm.actions),
                        "column_names": ";".join(perm.column_names or []),
                        "row_filter": perm.row_filter or "",
                        "member_object_id": member.object_id if member else "",
                        "member_object_type": member.object_type if member else "",
                        "member_tenant_id": member.tenant_id if member else "",
                    })
        return rows

    def to_markdown(self) -> str:
        """Human-readable summary suitable for a PR comment or wiki page."""
        lines = [
            f"# OneLake Security Audit — {self.lakehouse_name or self.item_id}",
            "",
            f"- Workspace: `{self.workspace_id}`",
            f"- Lakehouse item: `{self.item_id}`",
            f"- Captured: {self.captured_at.isoformat()}",
            f"- Roles: {self.role_count}",
            f"- Total members: {self.total_members}",
            f"- Orphaned roles: {len(self.orphaned_roles)}",
            "",
        ]
        for role in self.roles:
            lines.append(f"## {role.name}")
            lines.append("")
            lines.append(f"- Role id: `{role.role_id or 'n/a'}`")
            lines.append(f"- Members: {role.member_count}")
            lines.append("")
            lines.append("| Path | Effect | Actions | Columns | Row filter |")
            lines.append("| --- | --- | --- | --- | --- |")
            for perm in role.permissions:
                lines.append(
                    f"| `{perm.path}` | {perm.effect} | "
                    f"{', '.join(perm.actions) or '—'} | "
                    f"{', '.join(perm.column_names or []) or 'all'} | "
                    f"{perm.row_filter or '—'} |"
                )
            lines.append("")
            if role.entra_members:
                lines.append("| Member object id | Type |")
                lines.append("| --- | --- |")
                for member in role.entra_members:
                    lines.append(
                        f"| `{member.object_id}` | {member.object_type or 'n/a'} |"
                    )
            else:
                lines.append("_No members assigned._")
            lines.append("")
        return "\n".join(lines)


def audit_lakehouse(
    client: OneLakeSecurityClient,
    workspace_id: str,
    item_id: str,
    lakehouse_name: Optional[str] = None,
) -> LakehouseAuditReport:
    """Fetch and flatten every data access role on a lakehouse.

    Args:
        client: Authenticated API client.
        workspace_id: Target workspace GUID.
        item_id: Target lakehouse GUID.
        lakehouse_name: Optional friendly name recorded in the report.

    Returns:
        LakehouseAuditReport ready to inspect or persist.
    """
    if not workspace_id:
        raise ValueError("workspace_id is required")
    if not item_id:
        raise ValueError("item_id is required")

    api_roles, etag = client.list_roles(workspace_id, item_id)

    return LakehouseAuditReport(
        workspace_id=workspace_id,
        item_id=item_id,
        lakehouse_name=lakehouse_name,
        etag=etag,
        roles=[api_role_to_audited_role(r) for r in api_roles or []],
    )


def api_role_to_audited_role(api_role: dict) -> AuditedRole:
    """Convert one Fabric API role dict into an AuditedRole."""
    permissions: list[AuditedTablePermission] = []

    for rule in api_role.get("decisionRules") or []:
        effect = rule.get("effect", "Permit")
        paths: list[str] = []
        actions: list[str] = []

        for attribute in rule.get("permission") or []:
            name = attribute.get("attributeName")
            values = attribute.get("attributeValueIncludedIn") or []
            if name == "Path":
                paths.extend(values)
            elif name == "Action":
                actions.extend(values)

        constraints = rule.get("constraints") or {}
        columns_by_path: dict[str, list[str]] = {}
        for column in constraints.get("columns") or []:
            table_path = column.get("tablePath")
            if table_path:
                columns_by_path[table_path] = column.get("columnNames") or []

        rows_by_path: dict[str, str] = {}
        for row in constraints.get("rows") or []:
            table_path = row.get("tablePath")
            if table_path:
                rows_by_path[table_path] = row.get("value")

        for path in paths:
            permissions.append(
                AuditedTablePermission(
                    path=path,
                    actions=list(actions),
                    effect=effect,
                    column_names=columns_by_path.get(path),
                    row_filter=rows_by_path.get(path),
                )
            )

    members = api_role.get("members") or {}
    entra_members = [
        AuditedMember(
            object_id=m.get("objectId", ""),
            object_type=m.get("objectType"),
            tenant_id=m.get("tenantId"),
        )
        for m in members.get("microsoftEntraMembers") or []
    ]

    return AuditedRole(
        name=api_role.get("name", ""),
        role_id=api_role.get("id"),
        permissions=permissions,
        entra_members=entra_members,
        fabric_item_members=list(members.get("fabricItemMembers") or []),
        raw=api_role,
    )


def save_report(
    report: LakehouseAuditReport,
    path: str,
    format: str = "json",
    include_raw: bool = False,
) -> str:
    """Write an audit report to disk.

    Args:
        report: The report to persist.
        path: Destination file path. Parent directories are created.
        format: One of "json", "csv", or "markdown".
        include_raw: Include the untouched API payload (json format only).

    Returns:
        The path that was written.
    """
    fmt = format.lower()
    if fmt not in ("json", "csv", "markdown", "md"):
        raise ValueError(
            f"Unsupported format '{format}' — use 'json', 'csv', or 'markdown'"
        )

    parent = os.path.dirname(os.path.abspath(path))
    os.makedirs(parent, exist_ok=True)

    if fmt == "json":
        with open(path, "w", encoding="utf-8") as f:
            f.write(report.to_json(include_raw=include_raw))
    elif fmt == "csv":
        rows = report.to_rows()
        fieldnames = list(rows[0].keys()) if rows else _CSV_FIELDNAMES
        with open(path, "w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)
    else:
        with open(path, "w", encoding="utf-8") as f:
            f.write(report.to_markdown())

    return path


_CSV_FIELDNAMES = [
    "workspace_id",
    "item_id",
    "lakehouse_name",
    "captured_at",
    "role_name",
    "role_id",
    "member_count",
    "is_orphaned",
    "path",
    "effect",
    "actions",
    "column_names",
    "row_filter",
    "member_object_id",
    "member_object_type",
    "member_tenant_id",
]


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

# MARKDOWN ********************

# # Original Notebook Below

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
