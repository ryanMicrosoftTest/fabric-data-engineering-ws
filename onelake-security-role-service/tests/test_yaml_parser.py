"""Tests for YAML parsing and validation."""

import pytest
from onelake_security.yaml_parser import parse_role_definition, parse_user_mapping
from onelake_security.models import RoleState, MemberType


class TestParseRoleDefinition:
    """Parse role-definition YAML into RoleDefinition model."""

    def test_simple_role(self, simple_role_yaml):
        role = parse_role_definition(simple_role_yaml)
        assert role.name == "NeurologyReadRole"
        assert role.state == RoleState.PRESENT
        assert len(role.tables) == 1
        assert role.tables[0].path == "/Tables/doctor_table"
        assert role.tables[0].column_names == ["*"]
        assert "Neurology" in role.tables[0].row_filter

    def test_multi_table_role(self, multi_table_role_yaml):
        role = parse_role_definition(multi_table_role_yaml)
        assert role.name == "GastroRole"
        assert len(role.tables) == 2
        assert role.tables[0].path == "/Tables/doctor_table"
        assert role.tables[1].path == "/Tables/patient_records"
        assert role.tables[1].column_names == [
            "patient_id", "first_name", "last_name", "diagnosis_code"
        ]

    def test_wildcard_role(self, wildcard_role_yaml):
        role = parse_role_definition(wildcard_role_yaml)
        assert role.name == "FullReadRole"
        assert len(role.tables) == 1
        assert role.tables[0].path == "*"
        assert role.tables[0].column_names is None
        assert role.tables[0].row_filter is None

    def test_schema_level_role(self, schema_level_role_yaml):
        role = parse_role_definition(schema_level_role_yaml)
        assert role.name == "GoldReportingRole"
        assert len(role.tables) == 2
        assert role.tables[0].path == "/Tables/gold_reporting/*"
        assert role.tables[1].path == "/Tables/silver_sales/orders"

    def test_absent_role(self, absent_role_yaml):
        role = parse_role_definition(absent_role_yaml)
        assert role.name == "DeprecatedRole"
        assert role.state == RoleState.ABSENT

    def test_metadata_parsed(self, simple_role_yaml):
        role = parse_role_definition(simple_role_yaml)
        assert role.metadata is not None
        assert role.metadata["owner"] == "security-team@contoso.com"

    def test_missing_version_raises(self):
        bad_yaml = """
role:
  name: Test
  tables:
    - path: /Tables/t1
"""
        with pytest.raises(ValueError, match="version"):
            parse_role_definition(bad_yaml)

    def test_missing_role_name_raises(self):
        bad_yaml = """
version: "1.0"
state: present
role:
  tables:
    - path: /Tables/t1
"""
        with pytest.raises(ValueError, match="name"):
            parse_role_definition(bad_yaml)

    def test_invalid_state_raises(self):
        bad_yaml = """
version: "1.0"
state: maybe
role:
  name: Test
  tables:
    - path: /Tables/t1
"""
        with pytest.raises(ValueError, match="state"):
            parse_role_definition(bad_yaml)

    def test_no_tables_on_present_raises(self):
        bad_yaml = """
version: "1.0"
state: present
role:
  name: NoTablesRole
  tables: []
"""
        with pytest.raises(ValueError, match="table"):
            parse_role_definition(bad_yaml)

    def test_empty_yaml_raises(self):
        with pytest.raises(ValueError):
            parse_role_definition("")

    def test_invalid_yaml_syntax_raises(self):
        with pytest.raises(ValueError):
            parse_role_definition("{{{{not yaml")

    def test_default_state_is_present(self):
        """If state is omitted, default to present."""
        yaml_str = """
version: "1.0"
role:
  name: DefaultStateRole
  tables:
    - path: /Tables/t1
"""
        role = parse_role_definition(yaml_str)
        assert role.state == RoleState.PRESENT

    def test_columns_omitted_means_all(self):
        """If columns section is omitted, column_names should be None (all columns)."""
        yaml_str = """
version: "1.0"
state: present
role:
  name: NoColumnsRole
  tables:
    - path: /Tables/t1
"""
        role = parse_role_definition(yaml_str)
        assert role.tables[0].column_names is None


class TestParseUserMapping:
    """Parse user-mapping YAML into UserMapping model."""

    def test_simple_mapping(self, simple_mapping_yaml):
        mapping = parse_user_mapping(simple_mapping_yaml)
        assert mapping.role_name == "NeurologyReadRole"
        assert len(mapping.entra_members) == 2
        assert mapping.entra_members[0].object_id == "662b14b6-a936-46b0-bde0-9f4d759dc46e"
        assert mapping.entra_members[0].object_type == MemberType.USER

    def test_group_mapping(self, group_mapping_yaml):
        mapping = parse_user_mapping(group_mapping_yaml)
        assert mapping.role_name == "GastroRole"
        assert len(mapping.entra_members) == 2
        types = [m.object_type for m in mapping.entra_members]
        assert MemberType.USER in types
        assert MemberType.GROUP in types

    def test_spn_mapping(self, spn_mapping_yaml):
        mapping = parse_user_mapping(spn_mapping_yaml)
        assert len(mapping.entra_members) == 1
        assert mapping.entra_members[0].object_type == MemberType.SERVICE_PRINCIPAL

    def test_display_name_preserved(self, simple_mapping_yaml):
        mapping = parse_user_mapping(simple_mapping_yaml)
        assert mapping.entra_members[0].display_name == "Dr. Klerkx"

    def test_missing_version_raises(self):
        bad_yaml = """
mapping:
  role_name: Test
  members:
    entra_members: []
"""
        with pytest.raises(ValueError, match="version"):
            parse_user_mapping(bad_yaml)

    def test_missing_role_name_raises(self):
        bad_yaml = """
version: "1.0"
mapping:
  members:
    entra_members: []
"""
        with pytest.raises(ValueError, match="role_name"):
            parse_user_mapping(bad_yaml)

    def test_missing_object_id_raises(self):
        bad_yaml = """
version: "1.0"
mapping:
  role_name: Test
  members:
    entra_members:
      - display_name: "No ID"
        object_type: User
"""
        with pytest.raises(ValueError, match="object_id"):
            parse_user_mapping(bad_yaml)

    def test_invalid_object_type_raises(self):
        bad_yaml = """
version: "1.0"
mapping:
  role_name: Test
  members:
    entra_members:
      - object_id: "abc"
        object_type: InvalidType
"""
        with pytest.raises(ValueError, match="object_type"):
            parse_user_mapping(bad_yaml)

    def test_empty_members_is_valid(self):
        yaml_str = """
version: "1.0"
mapping:
  role_name: EmptyRole
  members:
    entra_members: []
"""
        mapping = parse_user_mapping(yaml_str)
        assert mapping.role_name == "EmptyRole"
        assert len(mapping.entra_members) == 0
