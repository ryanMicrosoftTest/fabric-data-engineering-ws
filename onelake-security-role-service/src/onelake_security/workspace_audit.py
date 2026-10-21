"""Workspace role audit — who holds Admin / Member / Contributor / Viewer.

Workspace roles are a different access plane from OneLake data access roles.
A OneLake role can restrict a user to two columns of one table, but a
workspace **Admin** sits above all of that — so an audit that only reads
`dataAccessRoles` misses the people with the broadest access in the tenant.

This module reads `GET /workspaces/{id}/roleAssignments` and flattens it into
a report. Security groups are expanded through Microsoft Graph so the output
names the individual people behind a group grant, not just the group.

This module never mutates assignments — it only reads.
"""

from __future__ import annotations

import csv
import json
import os
from dataclasses import dataclass, field, replace
from datetime import datetime, timezone
from typing import Optional

from onelake_security.api_client import OneLakeSecurityClient
from onelake_security.entra_directory import DirectoryObject, EntraDirectoryClient
from onelake_security.lakehouse_audit import AuditedGroupMember


# Highest privilege first — used to collapse a principal's multiple grants.
ROLE_PRIORITY = ("Admin", "Member", "Contributor", "Viewer")

# Roles that can reshape or exfiltrate the workspace, not merely read it.
ELEVATED_ROLES = ("Admin", "Member")


def role_rank(role: Optional[str]) -> int:
    """Lower is more privileged. Unknown roles sort last."""
    try:
        return ROLE_PRIORITY.index(role or "")
    except ValueError:
        return len(ROLE_PRIORITY)


@dataclass(frozen=True)
class WorkspaceRoleAssignment:
    """One workspace-level grant to a user, group, or service principal."""

    principal_id: str
    role: str
    display_name: Optional[str] = None
    principal_type: Optional[str] = None
    user_principal_name: Optional[str] = None
    aad_app_id: Optional[str] = None
    group_type: Optional[str] = None
    assignment_id: Optional[str] = None
    group_members: list[AuditedGroupMember] = field(default_factory=list)
    group_expanded: bool = False
    raw: Optional[dict] = None

    @property
    def friendly_name(self) -> str:
        return self.display_name or self.user_principal_name or self.principal_id

    @property
    def is_group(self) -> bool:
        return self.principal_type == "Group"

    @property
    def is_elevated(self) -> bool:
        return self.role in ELEVATED_ROLES

    @property
    def group_member_count(self) -> int:
        return len(self.group_members)

    @property
    def is_empty_group(self) -> bool:
        """A group grant that reaches nobody — the grant is inert.

        Only meaningful once expansion has actually run; an unexpanded group
        is unknown, not empty.
        """
        return self.is_group and self.group_expanded and not self.group_members

    def to_dict(self) -> dict:
        return {
            "assignment_id": self.assignment_id,
            "principal_id": self.principal_id,
            "display_name": self.display_name,
            "friendly_name": self.friendly_name,
            "principal_type": self.principal_type,
            "role": self.role,
            "is_elevated": self.is_elevated,
            "user_principal_name": self.user_principal_name,
            "aad_app_id": self.aad_app_id,
            "group_type": self.group_type,
            "group_expanded": self.group_expanded,
            "is_empty_group": self.is_empty_group,
            "group_member_count": self.group_member_count,
            "group_members": [g.to_dict() for g in self.group_members],
        }


@dataclass
class WorkspaceAuditReport:
    """Full snapshot of a workspace's role assignments."""

    workspace_id: str
    workspace_name: Optional[str] = None
    capacity_id: Optional[str] = None
    captured_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    assignments: list[WorkspaceRoleAssignment] = field(default_factory=list)

    @property
    def assignment_count(self) -> int:
        return len(self.assignments)

    @property
    def role_counts(self) -> dict[str, int]:
        counts: dict[str, int] = {}
        for assignment in self.assignments:
            counts[assignment.role] = counts.get(assignment.role, 0) + 1
        return counts

    @property
    def group_assignments(self) -> list[WorkspaceRoleAssignment]:
        return [a for a in self.assignments if a.is_group]

    @property
    def elevated_assignments(self) -> list[WorkspaceRoleAssignment]:
        """Admin and Member grants — the ones worth challenging in a review."""
        return [a for a in self.assignments if a.is_elevated]

    @property
    def unexpanded_groups(self) -> list[WorkspaceRoleAssignment]:
        """Group grants whose membership was never looked up."""
        return [a for a in self.group_assignments if not a.group_expanded]

    @property
    def empty_groups(self) -> list[WorkspaceRoleAssignment]:
        """Group grants that were expanded and reach nobody.

        A governance finding in its own right: the workspace shows a role
        assignment, but no person or principal actually receives it.
        """
        return [a for a in self.group_assignments if a.is_empty_group]

    def effective_principals(self) -> list[dict]:
        """Collapse every grant to one row per person, at their highest role.

        A user who is both a direct Viewer and a member of an Admin group is
        effectively an Admin — this reports them once, as Admin, and records
        every path that got them there.
        """
        best: dict[str, dict] = {}

        for assignment in self.assignments:
            if assignment.is_empty_group:
                continue

            reached: list[tuple[str, str, Optional[str], Optional[str], str]] = []
            if assignment.group_members:
                reached = [
                    (
                        g.object_id,
                        g.friendly_name,
                        g.object_type,
                        g.user_principal_name,
                        assignment.friendly_name,
                    )
                    for g in assignment.group_members
                ]
            else:
                reached = [(
                    assignment.principal_id,
                    assignment.friendly_name,
                    assignment.principal_type,
                    assignment.user_principal_name,
                    "direct",
                )]

            for object_id, name, obj_type, upn, via in reached:
                entry = best.get(object_id)
                if entry is None:
                    entry = {
                        "principal_object_id": object_id,
                        "principal_name": name,
                        "principal_type": obj_type or "",
                        "principal_upn": upn or "",
                        "effective_role": assignment.role,
                        "granted_via": [],
                    }
                    best[object_id] = entry

                if role_rank(assignment.role) < role_rank(entry["effective_role"]):
                    entry["effective_role"] = assignment.role

                path = f"{assignment.role} via {via}"
                if path not in entry["granted_via"]:
                    entry["granted_via"].append(path)

        rows = list(best.values())
        rows.sort(key=lambda r: (role_rank(r["effective_role"]), r["principal_name"]))
        for row in rows:
            row["granted_via"] = "; ".join(row["granted_via"])
        return rows

    def to_dict(self) -> dict:
        return {
            "workspace_id": self.workspace_id,
            "workspace_name": self.workspace_name,
            "capacity_id": self.capacity_id,
            "captured_at": self.captured_at.isoformat(),
            "assignment_count": self.assignment_count,
            "role_counts": self.role_counts,
            "elevated_principals": [a.friendly_name for a in self.elevated_assignments],
            "empty_group_names": [a.friendly_name for a in self.empty_groups],
            "unexpanded_group_names": [a.friendly_name for a in self.unexpanded_groups],
            "assignments": [a.to_dict() for a in self.assignments],
            "effective_principals": self.effective_principals(),
        }

    def to_json(self, indent: int = 2) -> str:
        return json.dumps(self.to_dict(), indent=indent)

    def to_rows(self) -> list[dict]:
        """One row per assignment — the grain the API returns."""
        return [
            {
                "workspace_id": self.workspace_id,
                "workspace_name": self.workspace_name or "",
                "captured_at": self.captured_at.isoformat(),
                "role": a.role,
                "is_elevated": a.is_elevated,
                "principal_name": a.friendly_name,
                "principal_type": a.principal_type or "",
                "principal_upn": a.user_principal_name or "",
                "principal_object_id": a.principal_id,
                "aad_app_id": a.aad_app_id or "",
                "group_type": a.group_type or "",
                "group_expanded": a.group_expanded,
                "is_empty_group": a.is_empty_group,
                "group_member_count": a.group_member_count,
                "group_members": "; ".join(
                    g.friendly_name for g in a.group_members
                ),
            }
            for a in self.assignments
        ]

    def to_effective_user_rows(self) -> list[dict]:
        """One row per person, at their highest effective workspace role."""
        return [
            {
                "workspace_id": self.workspace_id,
                "workspace_name": self.workspace_name or "",
                "captured_at": self.captured_at.isoformat(),
                **row,
            }
            for row in self.effective_principals()
        ]

    def to_markdown(self) -> str:
        """Human-readable summary suitable for a review or wiki page."""
        counts = self.role_counts
        lines = [
            f"# Workspace Role Audit — {self.workspace_name or self.workspace_id}",
            "",
            f"- Workspace: `{self.workspace_id}`",
            f"- Captured: {self.captured_at.isoformat()}",
            f"- Assignments: {self.assignment_count}",
            "- Roles: "
            + ", ".join(f"{r} {counts[r]}" for r in ROLE_PRIORITY if r in counts),
            f"- Elevated (Admin/Member): {len(self.elevated_assignments)}",
            f"- Empty group grants: {len(self.empty_groups)}",
            "",
            "## Assignments",
            "",
            "| Role | Principal | Type | Sign-in / App id | In group |",
            "| --- | --- | --- | --- | --- |",
        ]
        for a in sorted(
            self.assignments, key=lambda x: (role_rank(x.role), x.friendly_name)
        ):
            identifier = a.user_principal_name or a.aad_app_id or "—"
            if not a.is_group:
                membership = "—"
            elif not a.group_expanded:
                membership = "not expanded"
            elif not a.group_members:
                membership = "0 — empty group"
            else:
                membership = str(a.group_member_count)
            lines.append(
                f"| {a.role} | {a.friendly_name} | {a.principal_type or 'n/a'} | "
                f"{identifier} | {membership} |"
            )
        lines.append("")

        if self.empty_groups:
            lines.append(
                "> **Empty group grants** — these roles are assigned to a group "
                "with no members, so they currently grant access to nobody: "
                + ", ".join(f"`{a.friendly_name}` ({a.role})" for a in self.empty_groups)
            )
            lines.append("")

        for a in self.group_assignments:
            if not a.group_members:
                continue
            lines.append(f"**Inside `{a.friendly_name}` ({a.role})**")
            lines.append("")
            lines.append("| Principal | Type | Sign-in |")
            lines.append("| --- | --- | --- |")
            for g in a.group_members:
                lines.append(
                    f"| {g.friendly_name} | {g.object_type or 'n/a'} | "
                    f"{g.user_principal_name or '—'} |"
                )
            lines.append("")

        lines.append("## Effective access — one row per person")
        lines.append("")
        lines.append("| Effective role | Principal | Type | Sign-in | Granted via |")
        lines.append("| --- | --- | --- | --- | --- |")
        for row in self.effective_principals():
            lines.append(
                f"| {row['effective_role']} | {row['principal_name']} | "
                f"{row['principal_type'] or 'n/a'} | "
                f"{row['principal_upn'] or '—'} | {row['granted_via']} |"
            )
        lines.append("")
        return "\n".join(lines)


def audit_workspace_roles(
    client: OneLakeSecurityClient,
    workspace_id: str,
    workspace_name: Optional[str] = None,
    directory: Optional[EntraDirectoryClient] = None,
    expand_groups: bool = True,
    transitive: bool = True,
) -> WorkspaceAuditReport:
    """Read every workspace role assignment and flatten it for review.

    Args:
        client: Authenticated API client.
        workspace_id: Target workspace GUID.
        workspace_name: Optional name. Looked up from the API when omitted.
        directory: Optional Entra client, required for group expansion.
        expand_groups: List the principals inside each group grant.
        transitive: Flatten nested groups down to end principals.

    Returns:
        WorkspaceAuditReport ready to inspect or persist.
    """
    if not workspace_id:
        raise ValueError("workspace_id is required")

    capacity_id = None
    if workspace_name is None:
        workspace = client.get_workspace(workspace_id)
        workspace_name = workspace.get("displayName")
        capacity_id = workspace.get("capacityId")

    assignments = [
        _to_assignment(item)
        for item in client.list_workspace_role_assignments(workspace_id)
    ]

    report = WorkspaceAuditReport(
        workspace_id=workspace_id,
        workspace_name=workspace_name,
        capacity_id=capacity_id,
        assignments=assignments,
    )

    if directory is not None and expand_groups:
        expand_workspace_groups(report, directory, transitive=transitive)

    return report


def expand_workspace_groups(
    report: WorkspaceAuditReport,
    directory: EntraDirectoryClient,
    transitive: bool = True,
) -> dict[str, int]:
    """Explode each group assignment into the principals inside it.

    Args:
        report: Report to enrich (mutated in place).
        directory: Entra directory client.
        transitive: Flatten nested groups so only end principals are listed.

    Returns:
        Mapping of group principal_id -> number of principals found.
    """
    counts: dict[str, int] = {}

    for index, assignment in enumerate(report.assignments):
        if not assignment.is_group or not assignment.principal_id:
            continue

        members = [
            _to_group_member(obj)
            for obj in directory.get_group_members(
                assignment.principal_id, transitive=transitive
            )
        ]
        counts[assignment.principal_id] = len(members)
        report.assignments[index] = replace(
            assignment, group_members=members, group_expanded=True
        )

    return counts


def api_assignment_to_role_assignment(item: dict) -> WorkspaceRoleAssignment:
    """Convert one API role assignment payload into a WorkspaceRoleAssignment."""
    return _to_assignment(item)


def save_workspace_report(
    report: WorkspaceAuditReport,
    path: str,
    format: str = "json",
) -> str:
    """Write a workspace audit report to disk.

    Args:
        report: The report to persist.
        path: Destination file path. Parent directories are created.
        format: One of "json", "csv", "effective_csv", or "markdown".

    Returns:
        The path that was written.
    """
    fmt = format.lower()
    if fmt not in ("json", "csv", "effective_csv", "markdown", "md"):
        raise ValueError(
            f"Unsupported format '{format}' — use 'json', 'csv', "
            f"'effective_csv', or 'markdown'"
        )

    parent = os.path.dirname(os.path.abspath(path))
    os.makedirs(parent, exist_ok=True)

    if fmt == "json":
        with open(path, "w", encoding="utf-8") as f:
            f.write(report.to_json())
    elif fmt in ("csv", "effective_csv"):
        rows = (
            report.to_rows() if fmt == "csv" else report.to_effective_user_rows()
        )
        fieldnames = (
            list(rows[0].keys())
            if rows
            else (_CSV_FIELDNAMES if fmt == "csv" else _EFFECTIVE_FIELDNAMES)
        )
        with open(path, "w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)
    else:
        with open(path, "w", encoding="utf-8") as f:
            f.write(report.to_markdown())

    return path


# --- Private helpers ---


def _to_assignment(item: dict) -> WorkspaceRoleAssignment:
    principal = item.get("principal") or {}
    user_details = principal.get("userDetails") or {}
    spn_details = principal.get("servicePrincipalDetails") or {}
    group_details = principal.get("groupDetails") or {}

    return WorkspaceRoleAssignment(
        assignment_id=item.get("id"),
        principal_id=principal.get("id", ""),
        role=item.get("role", ""),
        display_name=principal.get("displayName"),
        principal_type=principal.get("type"),
        user_principal_name=user_details.get("userPrincipalName"),
        aad_app_id=spn_details.get("aadAppId"),
        group_type=group_details.get("groupType"),
        raw=item,
    )


def _to_group_member(obj: DirectoryObject) -> AuditedGroupMember:
    return AuditedGroupMember(
        object_id=obj.object_id,
        display_name=obj.display_name,
        object_type=obj.object_type,
        user_principal_name=obj.user_principal_name,
    )


_CSV_FIELDNAMES = [
    "workspace_id",
    "workspace_name",
    "captured_at",
    "role",
    "is_elevated",
    "principal_name",
    "principal_type",
    "principal_upn",
    "principal_object_id",
    "aad_app_id",
    "group_type",
    "group_expanded",
    "is_empty_group",
    "group_member_count",
    "group_members",
]

_EFFECTIVE_FIELDNAMES = [
    "workspace_id",
    "workspace_name",
    "captured_at",
    "principal_object_id",
    "principal_name",
    "principal_type",
    "principal_upn",
    "effective_role",
    "granted_via",
]
