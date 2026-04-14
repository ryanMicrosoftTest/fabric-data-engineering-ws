"""YAML parsing and validation for role definitions and user mappings.

Converts raw YAML strings into validated domain models.
Raises ValueError for any structural or content validation failures.
"""

from __future__ import annotations

import yaml
from typing import Any

from onelake_security.models import (
    EntraMember,
    MemberType,
    RoleDefinition,
    RoleState,
    TablePermission,
    UserMapping,
)


def parse_role_definition(yaml_str: str) -> RoleDefinition:
    """Parse a role-definition YAML string into a RoleDefinition model.

    Args:
        yaml_str: Raw YAML content from a role-definition file.

    Returns:
        Validated RoleDefinition instance.

    Raises:
        ValueError: If the YAML is invalid, missing required fields, or has bad values.
    """
    doc = _load_yaml(yaml_str)
    _require_version(doc)

    role_block = doc.get("role")
    if not role_block or not isinstance(role_block, dict):
        raise ValueError("Missing required 'role' block")

    name = role_block.get("name")
    if not name:
        raise ValueError("Missing required 'role.name'")

    raw_state = doc.get("state", "present")
    try:
        state = RoleState(raw_state)
    except ValueError:
        raise ValueError(
            f"Invalid state '{raw_state}'. Must be 'present' or 'absent'"
        )

    tables = _parse_tables(role_block.get("tables", []), state, name)
    metadata = role_block.get("metadata")

    return RoleDefinition(name=name, state=state, tables=tables, metadata=metadata)


def parse_user_mapping(yaml_str: str) -> UserMapping:
    """Parse a user-mapping YAML string into a UserMapping model.

    Args:
        yaml_str: Raw YAML content from a user-mapping file.

    Returns:
        Validated UserMapping instance.

    Raises:
        ValueError: If the YAML is invalid, missing required fields, or has bad values.
    """
    doc = _load_yaml(yaml_str)
    _require_version(doc)

    mapping_block = doc.get("mapping")
    if not mapping_block or not isinstance(mapping_block, dict):
        raise ValueError("Missing required 'mapping' block")

    role_name = mapping_block.get("role_name")
    if not role_name:
        raise ValueError("Missing required 'mapping.role_name'")

    members_block = mapping_block.get("members", {})
    entra_list = members_block.get("entra_members", []) or []

    entra_members = []
    for i, raw_member in enumerate(entra_list):
        entra_members.append(_parse_entra_member(raw_member, index=i))

    return UserMapping(role_name=role_name, entra_members=entra_members)


# --- Private helpers ---


def _load_yaml(yaml_str: str) -> dict[str, Any]:
    """Load and validate basic YAML structure."""
    if not yaml_str or not yaml_str.strip():
        raise ValueError("YAML content is empty")
    try:
        doc = yaml.safe_load(yaml_str)
    except yaml.YAMLError as e:
        raise ValueError(f"Invalid YAML syntax: {e}")
    if not isinstance(doc, dict):
        raise ValueError("YAML root must be a mapping")
    return doc


def _require_version(doc: dict) -> None:
    """Ensure the document has a version field."""
    if "version" not in doc:
        raise ValueError("Missing required 'version' field")


def _parse_tables(
    raw_tables: list | None, state: RoleState, role_name: str
) -> list[TablePermission]:
    """Parse the tables list from a role definition."""
    if not raw_tables:
        if state == RoleState.PRESENT:
            raise ValueError(
                f"Role '{role_name}' has state=present but no tables defined. "
                "At least one table entry is required."
            )
        return []

    tables = []
    for raw_table in raw_tables:
        if isinstance(raw_table, dict):
            path = raw_table.get("path", "")

            columns_block = raw_table.get("columns")
            column_names = None
            if columns_block and isinstance(columns_block, dict):
                column_names = columns_block.get("names")

            row_filter = raw_table.get("row_filter")

            tables.append(
                TablePermission(
                    path=path,
                    column_names=column_names,
                    row_filter=row_filter,
                )
            )
        elif isinstance(raw_table, str):
            tables.append(TablePermission(path=raw_table))

    return tables


def _parse_entra_member(raw: dict, index: int) -> EntraMember:
    """Parse a single Entra member entry."""
    if not isinstance(raw, dict):
        raise ValueError(f"entra_members[{index}] must be a mapping")

    object_id = raw.get("object_id", "")
    if not object_id:
        raise ValueError(f"entra_members[{index}] missing required 'object_id'")

    raw_type = raw.get("object_type", "")
    if not raw_type:
        raise ValueError(f"entra_members[{index}] missing required 'object_type'")

    try:
        object_type = MemberType(raw_type)
    except ValueError:
        valid = ", ".join(t.value for t in MemberType)
        raise ValueError(
            f"entra_members[{index}] invalid object_type '{raw_type}'. "
            f"Must be one of: {valid}"
        )

    display_name = raw.get("display_name")

    return EntraMember(
        object_id=object_id,
        object_type=object_type,
        display_name=display_name,
    )

