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
from dataclasses import dataclass, field, replace
from datetime import datetime, timezone
from typing import Optional

from onelake_security.api_client import OneLakeSecurityClient
from onelake_security.entra_directory import (
    DirectoryObject,
    EntraDirectoryClient,
    UnresolvedObject,
)


@dataclass(frozen=True)
class AuditedGroupMember:
    """A principal found inside a security group that holds a role.

    Populated by `expand_group_members()` so an audit can answer "who
    actually has access" rather than only naming the group.
    """

    object_id: str
    display_name: Optional[str] = None
    object_type: Optional[str] = None
    user_principal_name: Optional[str] = None

    @property
    def friendly_name(self) -> str:
        return self.display_name or self.user_principal_name or self.object_id

    def to_dict(self) -> dict:
        return {
            "object_id": self.object_id,
            "display_name": self.display_name,
            "object_type": self.object_type,
            "user_principal_name": self.user_principal_name,
            "friendly_name": self.friendly_name,
        }


@dataclass(frozen=True)
class AuditedMember:
    """A member assigned to a role, as reported by the API.

    `display_name` and `object_type` are populated only after the report is
    enriched from Entra — the Fabric roles API returns object IDs alone.
    `group_members` is filled in only when group expansion is requested.
    """

    object_id: str
    object_type: Optional[str] = None
    tenant_id: Optional[str] = None
    source: str = "microsoftEntraMembers"
    display_name: Optional[str] = None
    user_principal_name: Optional[str] = None
    resolution_note: Optional[str] = None
    group_members: list[AuditedGroupMember] = field(default_factory=list)

    @property
    def friendly_name(self) -> str:
        """Best available human-readable label for this member."""
        return self.display_name or self.user_principal_name or self.object_id

    @property
    def is_resolved(self) -> bool:
        return bool(self.display_name or self.user_principal_name)

    @property
    def is_group(self) -> bool:
        return self.object_type == "Group"

    @property
    def group_member_count(self) -> int:
        return len(self.group_members)

    def to_dict(self) -> dict:
        return {
            "object_id": self.object_id,
            "object_type": self.object_type,
            "tenant_id": self.tenant_id,
            "source": self.source,
            "display_name": self.display_name,
            "user_principal_name": self.user_principal_name,
            "friendly_name": self.friendly_name,
            "is_resolved": self.is_resolved,
            "resolution_note": self.resolution_note,
            "group_member_count": self.group_member_count,
            "group_members": [g.to_dict() for g in self.group_members],
        }


@dataclass(frozen=True)
class AuditedFabricItemMember:
    """A Fabric item granted access to a role (e.g. the lakehouse itself).

    Unlike Entra members, these are identified by a OneLake source path
    and carry their own item-level access verbs.
    """

    source_path: str
    item_access: list[str] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "source_path": self.source_path,
            "item_access": list(self.item_access),
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
    fabric_item_members: list[AuditedFabricItemMember] = field(default_factory=list)
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
            "fabric_item_members": [m.to_dict() for m in self.fabric_item_members],
        }
        if include_raw:
            data["raw"] = self.raw
        return data


@dataclass
class LakehouseAuditReport:
    """Full audit snapshot of one lakehouse's security roles."""

    workspace_id: str
    lakehouse_item_id: str
    lakehouse_name: Optional[str] = None
    etag: Optional[str] = None
    captured_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    roles: list[AuditedRole] = field(default_factory=list)

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

    @property
    def unresolved_members(self) -> list[AuditedMember]:
        """Entra members whose object ID never mapped to a friendly name."""
        return [
            m
            for r in self.roles
            for m in r.entra_members
            if not m.is_resolved
        ]

    def to_dict(self, include_raw: bool = False) -> dict:
        return {
            "workspace_id": self.workspace_id,
            "lakehouse_item_id": self.lakehouse_item_id,
            "lakehouse_name": self.lakehouse_name,
            "etag": self.etag,
            "captured_at": self.captured_at.isoformat(),
            "role_count": self.role_count,
            "total_members": self.total_members,
            "unresolved_member_ids": [m.object_id for m in self.unresolved_members],
            "orphaned_role_names": [r.name for r in self.orphaned_roles],
            "roles": [r.to_dict(include_raw=include_raw) for r in self.roles],
        }

    def to_json(self, include_raw: bool = False, indent: int = 2) -> str:
        return json.dumps(self.to_dict(include_raw=include_raw), indent=indent)

    def to_rows(self) -> list[dict]:
        """Flatten to one row per (role, path, member) for CSV/table output.

        Entra members and Fabric item members both produce rows, so a role
        whose only grant is a Fabric item is never reported as memberless.
        """
        rows: list[dict] = []
        for role in self.roles:
            permissions = role.permissions or [AuditedTablePermission(path="")]
            members: list[tuple] = [
                ("microsoftEntraMember", m) for m in role.entra_members
            ] + [
                ("fabricItemMember", m) for m in role.fabric_item_members
            ] or [("none", None)]

            for perm in permissions:
                for member_kind, member in members:
                    is_entra = member_kind == "microsoftEntraMember"
                    is_item = member_kind == "fabricItemMember"
                    rows.append({
                        "workspace_id": self.workspace_id,
                        "lakehouse_item_id": self.lakehouse_item_id,
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
                        "member_kind": member_kind,
                        "member_object_id": member.object_id if is_entra else "",
                        "member_display_name": member.friendly_name if is_entra else "",
                        "member_object_type": member.object_type if is_entra else "",
                        "member_upn": member.user_principal_name if is_entra else "",
                        "member_tenant_id": member.tenant_id if is_entra else "",
                        "member_is_resolved": member.is_resolved if is_entra else "",
                        "member_resolution_note": (
                            member.resolution_note or "" if is_entra else ""
                        ),
                        "member_group_member_count": (
                            member.group_member_count if is_entra else ""
                        ),
                        "member_group_members": (
                            "; ".join(g.friendly_name for g in member.group_members)
                            if is_entra
                            else ""
                        ),
                        "member_source_path": member.source_path if is_item else "",
                        "member_item_access": (
                            ";".join(member.item_access) if is_item else ""
                        ),
                    })
        return rows

    def to_effective_user_rows(self) -> list[dict]:
        """Flatten to one row per (role, path, effective principal).

        A group member is exploded into the principals inside it — after
        `expand_group_members()` has run — so this answers "which people can
        read this table?" rather than "which groups are attached to it?".
        Groups that were never expanded still appear as a single row.
        """
        rows: list[dict] = []
        for role in self.roles:
            permissions = role.permissions or [AuditedTablePermission(path="")]
            for perm in permissions:
                for member in role.entra_members:
                    principals: list[tuple[str, str, Optional[str], Optional[str]]] = []
                    if member.group_members:
                        principals = [
                            (
                                g.object_id,
                                g.friendly_name,
                                g.object_type,
                                g.user_principal_name,
                            )
                            for g in member.group_members
                        ]
                    else:
                        principals = [(
                            member.object_id,
                            member.friendly_name,
                            member.object_type,
                            member.user_principal_name,
                        )]

                    for object_id, name, obj_type, upn in principals:
                        rows.append({
                            "workspace_id": self.workspace_id,
                            "lakehouse_item_id": self.lakehouse_item_id,
                            "lakehouse_name": self.lakehouse_name or "",
                            "captured_at": self.captured_at.isoformat(),
                            "role_name": role.name,
                            "path": perm.path,
                            "actions": ";".join(perm.actions),
                            "effect": perm.effect,
                            "column_names": ";".join(perm.column_names or []),
                            "row_filter": perm.row_filter or "",
                            "principal_object_id": object_id,
                            "principal_name": name,
                            "principal_type": obj_type or "",
                            "principal_upn": upn or "",
                            "granted_via": (
                                member.friendly_name
                                if member.group_members
                                else "direct"
                            ),
                            "granted_via_object_id": (
                                member.object_id if member.group_members else ""
                            ),
                        })
        return rows

    def to_markdown(self) -> str:
        """Human-readable summary suitable for a PR comment or wiki page."""
        lines = [
            f"# OneLake Security Audit — {self.lakehouse_name or self.lakehouse_item_id}",
            "",
            f"- Workspace: `{self.workspace_id}`",
            f"- Lakehouse item: `{self.lakehouse_item_id}`",
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
                lines.append("| Member | Type | Object id | Notes |")
                lines.append("| --- | --- | --- | --- |")
                for member in role.entra_members:
                    if member.group_members:
                        note = f"{member.group_member_count} member(s) in group"
                    else:
                        note = member.resolution_note or "—"
                    lines.append(
                        f"| {member.friendly_name} | "
                        f"{member.object_type or 'n/a'} | "
                        f"`{member.object_id}` | {note} |"
                    )
                lines.append("")
                for member in role.entra_members:
                    if not member.group_members:
                        continue
                    lines.append(f"**Inside `{member.friendly_name}`**")
                    lines.append("")
                    lines.append("| Principal | Type | Sign-in |")
                    lines.append("| --- | --- | --- |")
                    for nested in member.group_members:
                        lines.append(
                            f"| {nested.friendly_name} | "
                            f"{nested.object_type or 'n/a'} | "
                            f"{nested.user_principal_name or '—'} |"
                        )
                    lines.append("")
            if role.fabric_item_members:
                lines.append("| Fabric item member | Item access |")
                lines.append("| --- | --- |")
                for item in role.fabric_item_members:
                    lines.append(
                        f"| `{item.source_path}` | "
                        f"{', '.join(item.item_access) or 'n/a'} |"
                    )
                lines.append("")
            if not role.entra_members and not role.fabric_item_members:
                lines.append("_No members assigned._")
                lines.append("")
        return "\n".join(lines)


def audit_lakehouse(
    client: OneLakeSecurityClient,
    workspace_id: str,
    lakehouse_item_id: str,
    lakehouse_name: Optional[str] = None,
    directory: Optional[EntraDirectoryClient] = None,
    expand_groups: bool = False,
    transitive: bool = True,
) -> LakehouseAuditReport:
    """Fetch and flatten every data access role on a lakehouse.

    Args:
        client: Authenticated API client.
        workspace_id: Target workspace GUID.
        lakehouse_item_id: Target lakehouse GUID (the Fabric item id).
        lakehouse_name: Optional friendly name recorded in the report.
        directory: Optional Entra client. When supplied, member object IDs
            are resolved to display names and true object types.
        expand_groups: Also list the principals inside each group member.
            Requires `directory`.
        transitive: When expanding groups, flatten nested groups to users.

    Returns:
        LakehouseAuditReport ready to inspect or persist.
    """
    if not workspace_id:
        raise ValueError("workspace_id is required")
    if not lakehouse_item_id:
        raise ValueError("lakehouse_item_id is required")

    api_roles, etag = client.list_roles(workspace_id, lakehouse_item_id)

    report = LakehouseAuditReport(
        workspace_id=workspace_id,
        lakehouse_item_id=lakehouse_item_id,
        lakehouse_name=lakehouse_name,
        etag=etag,
        roles=[api_role_to_audited_role(r) for r in api_roles or []],
    )

    if directory is not None:
        resolve_member_names(report, directory)
        if expand_groups:
            expand_group_members(report, directory, transitive=transitive)

    return report


def resolve_member_names(
    report: LakehouseAuditReport,
    directory: EntraDirectoryClient,
) -> list[UnresolvedObject]:
    """Enrich a report's Entra members with display names and object types.

    The Fabric roles API returns bare object IDs, so this looks each one up in
    Entra and fills in `display_name`, `object_type`, and
    `user_principal_name` in place. IDs that cannot be resolved get a
    `resolution_note` explaining why, instead of silently staying "n/a".

    Args:
        report: Report to enrich (mutated in place).
        directory: Entra directory client.

    Returns:
        UnresolvedObjects for IDs that could not be mapped — deleted objects,
        objects in another tenant, or objects the caller cannot see.
    """
    object_ids = {
        member.object_id
        for role in report.roles
        for member in role.entra_members
        if member.object_id
    }
    if not object_ids:
        return []

    resolution = directory.resolve(object_ids)
    reasons = {u.object_id: u.reason for u in resolution.unresolved}

    for role in report.roles:
        for index, member in enumerate(role.entra_members):
            match = resolution.resolved.get(member.object_id)
            if match is None:
                role.entra_members[index] = replace(
                    member,
                    resolution_note=reasons.get(member.object_id),
                )
                continue
            role.entra_members[index] = replace(
                member,
                display_name=match.display_name,
                object_type=match.object_type or member.object_type,
                user_principal_name=match.user_principal_name,
                resolution_note=None,
            )

    return resolution.unresolved


def expand_group_members(
    report: LakehouseAuditReport,
    directory: EntraDirectoryClient,
    transitive: bool = True,
) -> dict[str, int]:
    """Explode each group member into the principals inside it.

    A role assigned to `AAD-NEUROLOGY-READERS` tells a reviewer nothing about
    who can actually read the data. This fills `group_members` on every member
    whose object type is Group, so the report names the people behind the
    group.

    Call `resolve_member_names()` first — group members are identified by the
    object type that resolution stamps on them.

    Args:
        report: Report to enrich (mutated in place).
        directory: Entra directory client.
        transitive: Flatten nested groups so only end principals are listed.

    Returns:
        Mapping of group object_id -> number of principals found.
    """
    counts: dict[str, int] = {}

    for role in report.roles:
        for index, member in enumerate(role.entra_members):
            if not member.is_group or not member.object_id:
                continue

            nested = [
                _to_group_member(obj)
                for obj in directory.get_group_members(
                    member.object_id, transitive=transitive
                )
            ]
            counts[member.object_id] = len(nested)
            role.entra_members[index] = replace(member, group_members=nested)

    return counts


def _to_group_member(obj: DirectoryObject) -> AuditedGroupMember:
    return AuditedGroupMember(
        object_id=obj.object_id,
        display_name=obj.display_name,
        object_type=obj.object_type,
        user_principal_name=obj.user_principal_name,
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

    fabric_item_members = [
        AuditedFabricItemMember(
            source_path=m.get("sourcePath", ""),
            item_access=list(m.get("itemAccess") or []),
        )
        for m in members.get("fabricItemMembers") or []
    ]

    return AuditedRole(
        name=api_role.get("name", ""),
        role_id=api_role.get("id"),
        permissions=permissions,
        entra_members=entra_members,
        fabric_item_members=fabric_item_members,
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
    "lakehouse_item_id",
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
    "member_kind",
    "member_object_id",
    "member_display_name",
    "member_object_type",
    "member_upn",
    "member_tenant_id",
    "member_is_resolved",
    "member_resolution_note",
    "member_group_member_count",
    "member_group_members",
    "member_source_path",
    "member_item_access",
]
