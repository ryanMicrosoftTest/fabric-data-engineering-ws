"""Domain models for OneLake Security Role Service.

Pure data structures with validation — no I/O, no API calls.
These models are the shared contract between YAML parsing, API translation,
and reconciliation logic.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Optional


class RoleState(str, Enum):
    """Whether a role should exist or be removed."""

    PRESENT = "present"
    ABSENT = "absent"


class MemberType(str, Enum):
    """Type of Microsoft Entra ID member."""

    USER = "User"
    GROUP = "Group"
    SERVICE_PRINCIPAL = "ServicePrincipal"
    APPLICATION = "Application"


@dataclass(frozen=True)
class TablePermission:
    """Access rules for a single table in a lakehouse.

    Args:
        path: OneLake table path (e.g., /Tables/doctor_table, /Tables/schema/*, *)
        column_names: Columns to permit (CLS). None = all columns. ["*"] = all columns.
        row_filter: T-SQL SELECT predicate for RLS. None = all rows.
    """

    path: str
    column_names: Optional[list[str]] = None
    row_filter: Optional[str] = None

    def __post_init__(self):
        if not self.path:
            raise ValueError("TablePermission.path cannot be empty")


@dataclass(frozen=True)
class RoleDefinition:
    """Parsed representation of a role-definition YAML file.

    Args:
        name: Unique role name (used as key for upsert matching).
        state: Whether to create/update (present) or delete (absent).
        tables: List of table access rules.
        metadata: Optional dict with description, owner, tags (not sent to API).
    """

    name: str
    state: RoleState
    tables: list[TablePermission]
    metadata: Optional[dict] = field(default=None)

    def __post_init__(self):
        if not self.name:
            raise ValueError("RoleDefinition.name cannot be empty")
        if self.state == RoleState.PRESENT and not self.tables:
            raise ValueError(
                f"RoleDefinition '{self.name}' has state=present but no tables defined"
            )


@dataclass(frozen=True)
class EntraMember:
    """A Microsoft Entra ID user, group, or service principal.

    Args:
        object_id: Entra object ID (GUID).
        object_type: Type of member (User, Group, ServicePrincipal, Application).
        display_name: Human-readable name (for documentation only, not sent to API).
    """

    object_id: str
    object_type: MemberType
    display_name: Optional[str] = None

    def __post_init__(self):
        if not self.object_id:
            raise ValueError("EntraMember.object_id cannot be empty")


@dataclass(frozen=True)
class UserMapping:
    """Parsed representation of a user-mapping YAML file.

    This is authoritative — it defines the COMPLETE member list for the role.
    Members not in this list will be removed on reconciliation.

    Args:
        role_name: Must match a role name in role-definitions.
        entra_members: List of Entra members to assign to the role.
    """

    role_name: str
    entra_members: list[EntraMember]

    def __post_init__(self):
        if not self.role_name:
            raise ValueError("UserMapping.role_name cannot be empty")

