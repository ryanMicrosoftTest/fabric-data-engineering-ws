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
