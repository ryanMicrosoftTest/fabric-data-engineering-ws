"""Tests for role reconciliation — merging YAML intent with existing API state."""

import pytest
from onelake_security.models import (
    TablePermission,
    RoleDefinition,
    EntraMember,
    UserMapping,
    RoleState,
    MemberType,
)
from onelake_security.role_definition_reconciler import reconcile_role_definitions
from onelake_security.role_membership_reconciler import reconcile_role_membership


class TestReconcileRoleDefinitions:
    """Merge role definitions into existing API role list."""

    def test_add_new_role_to_empty(self):
        """Adding a role to an empty lakehouse."""
        role_def = RoleDefinition(
            name="NewRole",
            state=RoleState.PRESENT,
            tables=[TablePermission(path="/Tables/t1")],
        )
        result = reconcile_role_definitions(existing_roles=[], role_definitions=[role_def])
        assert len(result) == 1
        assert result[0]["name"] == "NewRole"

    def test_add_new_role_preserves_existing(self, existing_api_roles):
        """Adding a new role must not disturb existing roles."""
        role_def = RoleDefinition(
            name="BrandNewRole",
            state=RoleState.PRESENT,
            tables=[TablePermission(path="/Tables/new_table")],
        )
        result = reconcile_role_definitions(existing_api_roles, [role_def])
        names = [r["name"] for r in result]
        assert "NeurologyReadRole" in names
        assert "OrthoRole" in names
        assert "BrandNewRole" in names
        assert len(result) == 3

    def test_update_existing_role(self, existing_api_roles):
        """Updating a role by name replaces its definition but preserves its id."""
        role_def = RoleDefinition(
            name="NeurologyReadRole",
            state=RoleState.PRESENT,
            tables=[
                TablePermission(
                    path="/Tables/doctor_table",
                    row_filter="SELECT * FROM doctor_table WHERE department = 'Cardiology'",
                )
            ],
        )
        result = reconcile_role_definitions(existing_api_roles, [role_def])
        neuro = next(r for r in result if r["name"] == "NeurologyReadRole")
        # Should preserve the existing server-assigned id
        assert neuro["id"] == "role-id-111"
        # But update the decision rules
        assert "Cardiology" in str(neuro["decisionRules"])

    def test_update_preserves_members(self, existing_api_roles):
        """Updating a role definition must NOT wipe its members."""
        role_def = RoleDefinition(
            name="NeurologyReadRole",
            state=RoleState.PRESENT,
            tables=[TablePermission(path="/Tables/t1")],
        )
        result = reconcile_role_definitions(existing_api_roles, [role_def])
        neuro = next(r for r in result if r["name"] == "NeurologyReadRole")
        # Members should be unchanged
        assert len(neuro["members"]["microsoftEntraMembers"]) == 1

    def test_remove_absent_role(self, existing_api_roles):
        """state=absent removes the role from the list."""
        role_def = RoleDefinition(
            name="OrthoRole",
            state=RoleState.ABSENT,
            tables=[],
        )
        result = reconcile_role_definitions(existing_api_roles, [role_def])
        names = [r["name"] for r in result]
        assert "OrthoRole" not in names
        assert "NeurologyReadRole" in names

    def test_remove_nonexistent_role_is_noop(self, existing_api_roles):
        """Removing a role that doesn't exist should not raise."""
        role_def = RoleDefinition(
            name="GhostRole",
            state=RoleState.ABSENT,
            tables=[],
        )
        result = reconcile_role_definitions(existing_api_roles, [role_def])
        assert len(result) == len(existing_api_roles)

    def test_idempotency_same_input_twice(self, existing_api_roles):
        """Applying the same role definition twice produces identical output."""
        role_def = RoleDefinition(
            name="NeurologyReadRole",
            state=RoleState.PRESENT,
            tables=[
                TablePermission(
                    path="/Tables/doctor_table",
                    column_names=["*"],
                    row_filter="SELECT * FROM doctor_table WHERE department = 'Neurology'",
                )
            ],
        )
        result1 = reconcile_role_definitions(existing_api_roles, [role_def])
        result2 = reconcile_role_definitions(result1, [role_def])
        assert len(result1) == len(result2)

    def test_multiple_definitions_at_once(self, existing_api_roles):
        """Process multiple role definitions in one reconciliation."""
        defs = [
            RoleDefinition(
                name="NeurologyReadRole",
                state=RoleState.PRESENT,
                tables=[TablePermission(path="/Tables/t1")],
            ),
            RoleDefinition(
                name="OrthoRole",
                state=RoleState.ABSENT,
                tables=[],
            ),
            RoleDefinition(
                name="NewRole",
                state=RoleState.PRESENT,
                tables=[TablePermission(path="/Tables/t2")],
            ),
        ]
        result = reconcile_role_definitions(existing_api_roles, defs)
        names = [r["name"] for r in result]
        assert "NeurologyReadRole" in names
        assert "OrthoRole" not in names
        assert "NewRole" in names

    def test_case_insensitive_name_matching(self, existing_api_roles):
        """Role name matching should be case-insensitive."""
        role_def = RoleDefinition(
            name="neurologyreadrole",
            state=RoleState.PRESENT,
            tables=[TablePermission(path="/Tables/t1")],
        )
        result = reconcile_role_definitions(existing_api_roles, [role_def])
        # Should update existing, not create duplicate
        neuro_roles = [r for r in result if r["name"].lower() == "neurologyreadrole"]
        assert len(neuro_roles) == 1


class TestReconcileRoleMembership:
    """Merge user mappings into existing API role list."""

    def test_set_members_on_existing_role(self, existing_api_roles):
        """Set members on a role that already exists."""
        mapping = UserMapping(
            role_name="NeurologyReadRole",
            entra_members=[
                EntraMember(object_id="new-user-1", object_type=MemberType.USER),
                EntraMember(object_id="new-user-2", object_type=MemberType.USER),
            ],
        )
        result = reconcile_role_membership(
            existing_api_roles, mapping, tenant_id="tenant-123"
        )
        neuro = next(r for r in result if r["name"] == "NeurologyReadRole")
        members = neuro["members"]["microsoftEntraMembers"]
        # Authoritative: should be exactly the 2 new members
        assert len(members) == 2
        object_ids = {m["objectId"] for m in members}
        assert object_ids == {"new-user-1", "new-user-2"}

    def test_preserves_other_roles(self, existing_api_roles):
        """Updating members on one role must not affect other roles."""
        mapping = UserMapping(
            role_name="NeurologyReadRole",
            entra_members=[
                EntraMember(object_id="x", object_type=MemberType.USER),
            ],
        )
        result = reconcile_role_membership(
            existing_api_roles, mapping, tenant_id="t"
        )
        ortho = next(r for r in result if r["name"] == "OrthoRole")
        # OrthoRole should be completely untouched
        assert ortho == existing_api_roles[1]

    def test_preserves_decision_rules(self, existing_api_roles):
        """Updating members must NOT change the role's decision rules."""
        mapping = UserMapping(
            role_name="NeurologyReadRole",
            entra_members=[
                EntraMember(object_id="x", object_type=MemberType.USER),
            ],
        )
        result = reconcile_role_membership(
            existing_api_roles, mapping, tenant_id="t"
        )
        neuro = next(r for r in result if r["name"] == "NeurologyReadRole")
        assert neuro["decisionRules"] == existing_api_roles[0]["decisionRules"]

    def test_empty_members_clears_role(self, existing_api_roles):
        """An empty member list should remove all members from the role."""
        mapping = UserMapping(role_name="NeurologyReadRole", entra_members=[])
        result = reconcile_role_membership(
            existing_api_roles, mapping, tenant_id="t"
        )
        neuro = next(r for r in result if r["name"] == "NeurologyReadRole")
        assert len(neuro["members"]["microsoftEntraMembers"]) == 0

    def test_mapping_to_nonexistent_role_raises(self, existing_api_roles):
        """Mapping users to a role that doesn't exist should raise."""
        mapping = UserMapping(
            role_name="GhostRole",
            entra_members=[
                EntraMember(object_id="u1", object_type=MemberType.USER),
            ],
        )
        with pytest.raises(ValueError, match="GhostRole"):
            reconcile_role_membership(existing_api_roles, mapping, tenant_id="t")

    def test_tenant_id_applied_to_all_members(self, existing_api_roles):
        """All Entra members should get the provided tenant_id."""
        mapping = UserMapping(
            role_name="NeurologyReadRole",
            entra_members=[
                EntraMember(object_id="u1", object_type=MemberType.USER),
                EntraMember(object_id="g1", object_type=MemberType.GROUP),
            ],
        )
        result = reconcile_role_membership(
            existing_api_roles, mapping, tenant_id="my-tenant"
        )
        neuro = next(r for r in result if r["name"] == "NeurologyReadRole")
        for m in neuro["members"]["microsoftEntraMembers"]:
            assert m["tenantId"] == "my-tenant"

    def test_fabric_item_members_preserved(self, existing_api_roles):
        """If a role has fabricItemMembers, they should not be wiped by membership update."""
        # Add a fabricItemMember to the existing role
        existing_api_roles[0]["members"]["fabricItemMembers"] = [
            {"itemAccess": ["ReadAll"], "sourcePath": "ws/lh"}
        ]
        mapping = UserMapping(
            role_name="NeurologyReadRole",
            entra_members=[
                EntraMember(object_id="u1", object_type=MemberType.USER),
            ],
        )
        result = reconcile_role_membership(
            existing_api_roles, mapping, tenant_id="t"
        )
        neuro = next(r for r in result if r["name"] == "NeurologyReadRole")
        assert len(neuro["members"]["fabricItemMembers"]) == 1

    def test_idempotency(self, existing_api_roles):
        """Applying the same mapping twice produces identical output."""
        mapping = UserMapping(
            role_name="NeurologyReadRole",
            entra_members=[
                EntraMember(object_id="u1", object_type=MemberType.USER),
            ],
        )
        result1 = reconcile_role_membership(
            existing_api_roles, mapping, tenant_id="t"
        )
        result2 = reconcile_role_membership(result1, mapping, tenant_id="t")
        neuro1 = next(r for r in result1 if r["name"] == "NeurologyReadRole")
        neuro2 = next(r for r in result2 if r["name"] == "NeurologyReadRole")
        assert neuro1["members"] == neuro2["members"]

    def test_case_insensitive_role_name_matching(self, existing_api_roles):
        """Role name matching in mappings should be case-insensitive."""
        mapping = UserMapping(
            role_name="neurologyreadrole",
            entra_members=[
                EntraMember(object_id="u1", object_type=MemberType.USER),
            ],
        )
        # Should not raise — should find the role despite case difference
        result = reconcile_role_membership(
            existing_api_roles, mapping, tenant_id="t"
        )
        assert len(result) == len(existing_api_roles)
