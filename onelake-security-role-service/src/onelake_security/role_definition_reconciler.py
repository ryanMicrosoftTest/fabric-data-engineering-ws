"""Reconciler for role definitions — merge YAML intent with existing API state.

Takes the current set of roles from the API and a list of role definitions
from YAML files, and produces a new full role list for PUT.
"""

from __future__ import annotations

import copy

from onelake_security.models import RoleDefinition, RoleState
from onelake_security.translators import role_definition_to_api_role


def reconcile_role_definitions(
    existing_roles: list[dict],
    role_definitions: list[RoleDefinition],
) -> list[dict]:
    """Merge role definitions into the existing API role list.

    For each RoleDefinition:
      - state=present: upsert (update if exists by name, insert if new)
      - state=absent: remove if exists, no-op if not found

    When updating, preserves the existing role's server-assigned 'id' and 'members'.

    Args:
        existing_roles: Current roles from GET /dataAccessRoles (list of dicts).
        role_definitions: Parsed role definitions from YAML files.

    Returns:
        New full role list ready for PUT /dataAccessRoles.
    """
    # Deep copy to avoid mutating the input
    roles = copy.deepcopy(existing_roles)

    for role_def in role_definitions:
        if role_def.state == RoleState.PRESENT:
            roles = _upsert_role(roles, role_def)
        elif role_def.state == RoleState.ABSENT:
            roles = _remove_role(roles, role_def.name)

    return roles


def _find_role_index(roles: list[dict], name: str) -> int | None:
    """Find a role by name (case-insensitive). Returns index or None."""
    for i, role in enumerate(roles):
        if role.get("name", "").lower() == name.lower():
            return i
    return None


def _upsert_role(roles: list[dict], role_def: RoleDefinition) -> list[dict]:
    """Insert or update a role in the list."""
    new_api_role = role_definition_to_api_role(role_def)
    idx = _find_role_index(roles, role_def.name)

    if idx is not None:
        existing = roles[idx]
        # Preserve server-assigned id
        if "id" in existing:
            new_api_role["id"] = existing["id"]
        # Preserve existing members (membership is managed separately)
        new_api_role["members"] = existing.get("members", new_api_role["members"])
        roles[idx] = new_api_role
    else:
        roles.append(new_api_role)

    return roles


def _remove_role(roles: list[dict], name: str) -> list[dict]:
    """Remove a role by name (case-insensitive). No-op if not found."""
    idx = _find_role_index(roles, name)
    if idx is not None:
        roles.pop(idx)
    return roles

