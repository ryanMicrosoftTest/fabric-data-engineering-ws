"""Tests for translating domain models to/from Fabric API payloads."""

import pytest
from onelake_security.models import (
    TablePermission,
    RoleDefinition,
    EntraMember,
    UserMapping,
    RoleState,
    MemberType,
)
from onelake_security.translators import (
    role_definition_to_api_role,
    user_mapping_to_api_members,
    api_role_to_role_name,
)


class TestRoleDefinitionToApiRole:
    """Convert a RoleDefinition model into the Fabric API role dict."""

    def test_simple_role_structure(self):
        role_def = RoleDefinition(
            name="TestRole",
            state=RoleState.PRESENT,
            tables=[
                TablePermission(
                    path="/Tables/doctor_table",
                    column_names=["*"],
                    row_filter="SELECT * FROM doctor_table WHERE dept = 'Neuro'",
                )
            ],
        )
        api_role = role_definition_to_api_role(role_def)

        assert api_role["name"] == "TestRole"
        assert len(api_role["decisionRules"]) == 1

        rule = api_role["decisionRules"][0]
        assert rule["effect"] == "Permit"

        # Check permission contains Path and Action
        path_perm = next(p for p in rule["permission"] if p["attributeName"] == "Path")
        assert "/Tables/doctor_table" in path_perm["attributeValueIncludedIn"]

        action_perm = next(p for p in rule["permission"] if p["attributeName"] == "Action")
        assert "Read" in action_perm["attributeValueIncludedIn"]

    def test_cls_columns_in_constraints(self):
        role_def = RoleDefinition(
            name="CLSRole",
            state=RoleState.PRESENT,
            tables=[
                TablePermission(
                    path="/Tables/t1",
                    column_names=["col_a", "col_b"],
                )
            ],
        )
        api_role = role_definition_to_api_role(role_def)
        constraints = api_role["decisionRules"][0]["constraints"]

        assert len(constraints["columns"]) == 1
        col = constraints["columns"][0]
        assert col["tablePath"] == "/Tables/t1"
        assert col["columnNames"] == ["col_a", "col_b"]
        assert col["columnEffect"] == "Permit"
        assert col["columnAction"] == ["Read"]

    def test_rls_rows_in_constraints(self):
        role_def = RoleDefinition(
            name="RLSRole",
            state=RoleState.PRESENT,
            tables=[
                TablePermission(
                    path="/Tables/t1",
                    row_filter="SELECT * FROM t1 WHERE region = 'West'",
                )
            ],
        )
        api_role = role_definition_to_api_role(role_def)
        constraints = api_role["decisionRules"][0]["constraints"]

        assert len(constraints["rows"]) == 1
        assert constraints["rows"][0]["tablePath"] == "/Tables/t1"
        assert "West" in constraints["rows"][0]["value"]

    def test_no_cls_no_rls_omits_constraints(self):
        role_def = RoleDefinition(
            name="NoConstraints",
            state=RoleState.PRESENT,
            tables=[TablePermission(path="/Tables/t1")],
        )
        api_role = role_definition_to_api_role(role_def)
        rule = api_role["decisionRules"][0]
        assert "constraints" not in rule or rule.get("constraints") is None

    def test_wildcard_all_columns_omits_column_constraint(self):
        """columns: ["*"] should NOT generate a column constraint — it means all columns."""
        role_def = RoleDefinition(
            name="WildcardCols",
            state=RoleState.PRESENT,
            tables=[
                TablePermission(path="/Tables/t1", column_names=["*"]),
            ],
        )
        api_role = role_definition_to_api_role(role_def)
        rule = api_role["decisionRules"][0]
        constraints = rule.get("constraints", {})
        # ["*"] means all columns — no column constraint needed
        assert not constraints.get("columns", [])

    def test_multi_table_merges_paths(self):
        role_def = RoleDefinition(
            name="MultiTable",
            state=RoleState.PRESENT,
            tables=[
                TablePermission(path="/Tables/t1"),
                TablePermission(path="/Tables/t2"),
            ],
        )
        api_role = role_definition_to_api_role(role_def)
        rule = api_role["decisionRules"][0]
        path_perm = next(p for p in rule["permission"] if p["attributeName"] == "Path")
        assert "/Tables/t1" in path_perm["attributeValueIncludedIn"]
        assert "/Tables/t2" in path_perm["attributeValueIncludedIn"]

    def test_multi_table_cls_per_table(self):
        role_def = RoleDefinition(
            name="MultiCLS",
            state=RoleState.PRESENT,
            tables=[
                TablePermission(path="/Tables/t1", column_names=["a", "b"]),
                TablePermission(path="/Tables/t2", column_names=["x", "y"]),
            ],
        )
        api_role = role_definition_to_api_role(role_def)
        cols = api_role["decisionRules"][0]["constraints"]["columns"]
        assert len(cols) == 2
        paths = {c["tablePath"] for c in cols}
        assert paths == {"/Tables/t1", "/Tables/t2"}

    def test_api_role_has_no_id_field(self):
        """New roles should not have an id — the API assigns one."""
        role_def = RoleDefinition(
            name="NewRole",
            state=RoleState.PRESENT,
            tables=[TablePermission(path="/Tables/t1")],
        )
        api_role = role_definition_to_api_role(role_def)
        assert "id" not in api_role

    def test_members_initialized_empty(self):
        """New roles start with empty members — mapping files add them."""
        role_def = RoleDefinition(
            name="NoMembers",
            state=RoleState.PRESENT,
            tables=[TablePermission(path="/Tables/t1")],
        )
        api_role = role_definition_to_api_role(role_def)
        assert api_role["members"]["microsoftEntraMembers"] == []
        assert api_role["members"]["fabricItemMembers"] == []


class TestUserMappingToApiMembers:
    """Convert a UserMapping model into the API members dict."""

    def test_simple_users(self):
        mapping = UserMapping(
            role_name="TestRole",
            entra_members=[
                EntraMember(object_id="u1", object_type=MemberType.USER, display_name="User 1"),
                EntraMember(object_id="u2", object_type=MemberType.USER),
            ],
        )
        members = user_mapping_to_api_members(mapping, tenant_id="tenant-123")

        assert len(members["microsoftEntraMembers"]) == 2
        assert members["fabricItemMembers"] == []

        m1 = members["microsoftEntraMembers"][0]
        assert m1["objectId"] == "u1"
        assert m1["objectType"] == "User"
        assert m1["tenantId"] == "tenant-123"

    def test_group_member(self):
        mapping = UserMapping(
            role_name="TestRole",
            entra_members=[
                EntraMember(object_id="g1", object_type=MemberType.GROUP),
            ],
        )
        members = user_mapping_to_api_members(mapping, tenant_id="t")
        assert members["microsoftEntraMembers"][0]["objectType"] == "Group"

    def test_empty_members(self):
        mapping = UserMapping(role_name="TestRole", entra_members=[])
        members = user_mapping_to_api_members(mapping, tenant_id="t")
        assert members["microsoftEntraMembers"] == []

    def test_display_name_not_in_api_payload(self):
        """display_name is for readability only — should NOT be sent to the API."""
        mapping = UserMapping(
            role_name="TestRole",
            entra_members=[
                EntraMember(object_id="u1", object_type=MemberType.USER, display_name="Dr. Test"),
            ],
        )
        members = user_mapping_to_api_members(mapping, tenant_id="t")
        m = members["microsoftEntraMembers"][0]
        assert "displayName" not in m
        assert "display_name" not in m


class TestApiRoleToRoleName:
    """Extract role name from an API role dict."""

    def test_extracts_name(self):
        assert api_role_to_role_name({"name": "TestRole", "id": "123"}) == "TestRole"

    def test_missing_name_raises(self):
        with pytest.raises(KeyError):
            api_role_to_role_name({"id": "123"})
