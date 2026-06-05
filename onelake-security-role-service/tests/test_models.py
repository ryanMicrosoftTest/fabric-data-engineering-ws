"""Tests for domain models — data structures with validation."""

import pytest
from onelake_security.models import (
    TablePermission,
    RoleDefinition,
    EntraMember,
    UserMapping,
    RoleState,
    MemberType,
)


class TestRoleState:
    """RoleState enum validates allowed values."""

    def test_present_state(self):
        assert RoleState.PRESENT == "present"

    def test_absent_state(self):
        assert RoleState.ABSENT == "absent"

    def test_invalid_state_raises(self):
        with pytest.raises(ValueError):
            RoleState("invalid")


class TestMemberType:
    """MemberType enum validates allowed values."""

    def test_user_type(self):
        assert MemberType.USER == "User"

    def test_group_type(self):
        assert MemberType.GROUP == "Group"

    def test_service_principal_type(self):
        assert MemberType.SERVICE_PRINCIPAL == "ServicePrincipal"

    def test_application_type(self):
        assert MemberType.APPLICATION == "Application"

    def test_invalid_type_raises(self):
        with pytest.raises(ValueError):
            MemberType("InvalidType")


class TestTablePermission:
    """TablePermission holds per-table access rules."""

    def test_full_table_access(self):
        tp = TablePermission(path="/Tables/doctor_table")
        assert tp.path == "/Tables/doctor_table"
        assert tp.column_names is None
        assert tp.row_filter is None

    def test_with_cls(self):
        tp = TablePermission(
            path="/Tables/patient_records",
            column_names=["patient_id", "name"],
        )
        assert tp.column_names == ["patient_id", "name"]

    def test_with_rls(self):
        tp = TablePermission(
            path="/Tables/doctor_table",
            row_filter="SELECT * FROM doctor_table WHERE dept = 'Neuro'",
        )
        assert tp.row_filter is not None

    def test_wildcard_path(self):
        tp = TablePermission(path="*")
        assert tp.path == "*"

    def test_schema_level_path(self):
        tp = TablePermission(path="/Tables/gold_reporting/*")
        assert tp.path == "/Tables/gold_reporting/*"

    def test_star_columns_treated_as_all(self):
        tp = TablePermission(path="/Tables/t1", column_names=["*"])
        assert tp.column_names == ["*"]

    def test_empty_path_raises(self):
        with pytest.raises(ValueError):
            TablePermission(path="")


class TestRoleDefinition:
    """RoleDefinition is the parsed representation of a role YAML."""

    def test_simple_role(self):
        rd = RoleDefinition(
            name="TestRole",
            state=RoleState.PRESENT,
            tables=[TablePermission(path="/Tables/t1")],
        )
        assert rd.name == "TestRole"
        assert rd.state == RoleState.PRESENT
        assert len(rd.tables) == 1

    def test_absent_role_no_tables_required(self):
        rd = RoleDefinition(
            name="OldRole",
            state=RoleState.ABSENT,
            tables=[],
        )
        assert rd.state == RoleState.ABSENT
        assert rd.tables == []

    def test_with_metadata(self):
        rd = RoleDefinition(
            name="TestRole",
            state=RoleState.PRESENT,
            tables=[TablePermission(path="/Tables/t1")],
            metadata={"description": "test", "owner": "me"},
        )
        assert rd.metadata["owner"] == "me"

    def test_empty_name_raises(self):
        with pytest.raises(ValueError):
            RoleDefinition(name="", state=RoleState.PRESENT, tables=[])

    def test_present_role_requires_at_least_one_table(self):
        with pytest.raises(ValueError):
            RoleDefinition(name="NoTables", state=RoleState.PRESENT, tables=[])


class TestEntraMember:
    """EntraMember represents a user/group/SPN in the role mapping."""

    def test_user_member(self):
        m = EntraMember(
            object_id="abc-123",
            object_type=MemberType.USER,
            display_name="Dr. Test",
        )
        assert m.object_type == MemberType.USER

    def test_group_member(self):
        m = EntraMember(
            object_id="grp-456",
            object_type=MemberType.GROUP,
        )
        assert m.display_name is None

    def test_empty_object_id_raises(self):
        with pytest.raises(ValueError):
            EntraMember(object_id="", object_type=MemberType.USER)


class TestUserMapping:
    """UserMapping is the parsed representation of a mapping YAML."""

    def test_simple_mapping(self):
        um = UserMapping(
            role_name="TestRole",
            entra_members=[
                EntraMember(object_id="u1", object_type=MemberType.USER),
                EntraMember(object_id="u2", object_type=MemberType.USER),
            ],
        )
        assert um.role_name == "TestRole"
        assert len(um.entra_members) == 2

    def test_empty_role_name_raises(self):
        with pytest.raises(ValueError):
            UserMapping(role_name="", entra_members=[])

    def test_empty_members_is_valid(self):
        """An empty member list is valid — it means 'remove all members'."""
        um = UserMapping(role_name="TestRole", entra_members=[])
        assert len(um.entra_members) == 0
