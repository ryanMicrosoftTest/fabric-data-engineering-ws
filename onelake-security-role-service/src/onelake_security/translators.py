"""Translators between domain models and Fabric API payloads.

Converts human-friendly domain models into the exact JSON structure
the Fabric Data Access Roles API expects, and vice versa.
"""

from __future__ import annotations

from onelake_security.models import (
    RoleDefinition,
    UserMapping,
    MemberType,
)


def role_definition_to_api_role(role_def: RoleDefinition) -> dict:
    """Convert a RoleDefinition into a Fabric API role dict.

    The result is a single role object suitable for inclusion in the
    PUT /dataAccessRoles request body's "value" array.

    Members are initialized empty — the user-mapping service adds them.
    No "id" field is included — the API assigns one for new roles,
    and the reconciler preserves it for existing ones.
    """
    # Collect all table paths for the permission scope
    all_paths = [t.path for t in role_def.tables]

    # Build permission array (Path + Action)
    permission = [
        {"attributeName": "Path", "attributeValueIncludedIn": all_paths},
        {"attributeName": "Action", "attributeValueIncludedIn": ["Read"]},
    ]

    # Build constraints from per-table CLS and RLS rules
    columns = []
    rows = []

    for table in role_def.tables:
        # CLS: only add constraint if specific columns are listed (not ["*"] or None)
        if table.column_names and table.column_names != ["*"]:
            columns.append({
                "tablePath": table.path,
                "columnNames": table.column_names,
                "columnEffect": "Permit",
                "columnAction": ["Read"],
            })

        # RLS: add row filter if present
        if table.row_filter:
            rows.append({
                "tablePath": table.path,
                "value": table.row_filter,
            })

    # Build decision rule
    decision_rule: dict = {
        "effect": "Permit",
        "permission": permission,
    }

    # Only include constraints if there are any
    if columns or rows:
        constraints: dict = {}
        if columns:
            constraints["columns"] = columns
        if rows:
            constraints["rows"] = rows
        decision_rule["constraints"] = constraints

    return {
        "name": role_def.name,
        "decisionRules": [decision_rule],
        "members": {
            "microsoftEntraMembers": [],
            "fabricItemMembers": [],
        },
    }


def user_mapping_to_api_members(mapping: UserMapping, tenant_id: str) -> dict:
    """Convert a UserMapping into the API members dict.

    Args:
        mapping: Parsed user mapping with Entra members.
        tenant_id: Tenant ID to stamp on every member (pipeline parameter).

    Returns:
        Members dict ready to replace a role's "members" field.
    """
    entra_members = []
    for member in mapping.entra_members:
        entra_members.append({
            "tenantId": tenant_id,
            "objectId": member.object_id,
            "objectType": member.object_type.value,
        })

    return {
        "microsoftEntraMembers": entra_members,
        "fabricItemMembers": [],
    }


def api_role_to_role_name(api_role: dict) -> str:
    """Extract the role name from an API role dict.

    Raises:
        KeyError: If the role dict has no 'name' field.
    """
    return api_role["name"]

