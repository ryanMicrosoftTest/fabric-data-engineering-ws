"""Reconciler for role membership — merge user mappings with existing API state.

Takes the current set of roles from the API and a UserMapping from YAML,
and produces a new full role list with updated members for the target role.
"""

from __future__ import annotations

import copy

from onelake_security.models import UserMapping
from onelake_security.translators import user_mapping_to_api_members


def reconcile_role_membership(
    existing_roles: list[dict],
    mapping: UserMapping,
    tenant_id: str,
) -> list[dict]:
    """Update members for a single role based on a user-mapping YAML.

    The mapping is AUTHORITATIVE — it replaces all microsoftEntraMembers
    for the target role. fabricItemMembers are preserved.

    Args:
        existing_roles: Current roles from GET /dataAccessRoles.
        mapping: Parsed user mapping from YAML.
        tenant_id: Tenant ID to stamp on every Entra member.

    Returns:
        New full role list ready for PUT /dataAccessRoles.

    Raises:
        ValueError: If the target role is not found in existing_roles.
    """
    roles = copy.deepcopy(existing_roles)
    idx = _find_role_index(roles, mapping.role_name)

    if idx is None:
        raise ValueError(
            f"Cannot map users to role '{mapping.role_name}' — "
            f"role not found in existing roles. "
            f"Create the role definition first."
        )

    new_members = user_mapping_to_api_members(mapping, tenant_id)

    # Preserve existing fabricItemMembers
    existing_fabric_members = (
        roles[idx].get("members", {}).get("fabricItemMembers", [])
    )
    new_members["fabricItemMembers"] = existing_fabric_members

    roles[idx]["members"] = new_members
    return roles


def _find_role_index(roles: list[dict], name: str) -> int | None:
    """Find a role by name (case-insensitive). Returns index or None."""
    for i, role in enumerate(roles):
        if role.get("name", "").lower() == name.lower():
            return i
    return None

