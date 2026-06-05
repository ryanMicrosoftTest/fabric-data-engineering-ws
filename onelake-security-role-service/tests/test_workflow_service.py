"""Tests for the workflow service — orchestration layer.

Tests the 412 retry loop, file processing, and coordination between
parser, reconciler, and API client. All I/O is mocked.
"""

import pytest
from unittest.mock import MagicMock, patch, call
from onelake_security.workflow_service import (
    process_role_definitions,
    process_user_mappings,
    RoleWorkflowResult,
    MappingWorkflowResult,
)
from onelake_security.api_client import PreconditionFailedError
from onelake_security.models import (
    RoleDefinition,
    RoleState,
    TablePermission,
    UserMapping,
    EntraMember,
    MemberType,
)


@pytest.fixture
def mock_client():
    client = MagicMock()
    client.list_roles.return_value = ([], '"etag-1"')
    client.put_roles.return_value = '"etag-2"'
    client.put_roles_dry_run.return_value = True
    return client


@pytest.fixture
def sample_role_def():
    return RoleDefinition(
        name="TestRole",
        state=RoleState.PRESENT,
        tables=[TablePermission(path="/Tables/t1")],
    )


@pytest.fixture
def sample_mapping():
    return UserMapping(
        role_name="TestRole",
        entra_members=[
            EntraMember(object_id="u1", object_type=MemberType.USER),
        ],
    )


class TestProcessRoleDefinitions:
    """Orchestrate: parse YAMLs → reconcile → PUT with ETag."""

    def test_applies_single_role(self, mock_client, sample_role_def):
        result = process_role_definitions(
            client=mock_client,
            workspace_id="ws",
            item_id="item",
            role_definitions=[sample_role_def],
        )

        assert result.success is True
        assert result.roles_applied == 1
        mock_client.list_roles.assert_called_once()
        mock_client.put_roles.assert_called_once()

    def test_applies_multiple_roles(self, mock_client):
        defs = [
            RoleDefinition(name="R1", state=RoleState.PRESENT,
                           tables=[TablePermission(path="/Tables/t1")]),
            RoleDefinition(name="R2", state=RoleState.PRESENT,
                           tables=[TablePermission(path="/Tables/t2")]),
        ]

        result = process_role_definitions(
            client=mock_client,
            workspace_id="ws",
            item_id="item",
            role_definitions=defs,
        )

        assert result.success is True
        assert result.roles_applied == 2

    def test_no_definitions_is_noop(self, mock_client):
        result = process_role_definitions(
            client=mock_client,
            workspace_id="ws",
            item_id="item",
            role_definitions=[],
        )

        assert result.success is True
        assert result.roles_applied == 0
        mock_client.put_roles.assert_not_called()

    def test_retries_on_412(self, mock_client, sample_role_def):
        """On PreconditionFailedError, re-fetch roles and retry."""
        mock_client.put_roles.side_effect = [
            PreconditionFailedError("stale"),
            '"etag-3"',
        ]
        # Second list_roles call returns updated state
        mock_client.list_roles.side_effect = [
            ([], '"etag-1"'),
            ([{"name": "OtherRole", "id": "x", "decisionRules": [], "members": {"microsoftEntraMembers": [], "fabricItemMembers": []}}], '"etag-2"'),
        ]

        result = process_role_definitions(
            client=mock_client,
            workspace_id="ws",
            item_id="item",
            role_definitions=[sample_role_def],
        )

        assert result.success is True
        assert mock_client.list_roles.call_count == 2
        assert mock_client.put_roles.call_count == 2

    def test_gives_up_after_max_412_retries(self, mock_client, sample_role_def):
        mock_client.put_roles.side_effect = PreconditionFailedError("stale")
        mock_client.list_roles.return_value = ([], '"etag"')

        result = process_role_definitions(
            client=mock_client,
            workspace_id="ws",
            item_id="item",
            role_definitions=[sample_role_def],
            max_concurrency_retries=2,
        )

        assert result.success is False
        assert "concurrency" in result.error.lower()

    def test_validates_with_dry_run(self, mock_client, sample_role_def):
        """Should call dry_run before actual PUT when validate=True."""
        result = process_role_definitions(
            client=mock_client,
            workspace_id="ws",
            item_id="item",
            role_definitions=[sample_role_def],
            validate_first=True,
        )

        assert result.success is True
        mock_client.put_roles_dry_run.assert_called_once()

    def test_aborts_if_dry_run_fails(self, mock_client, sample_role_def):
        mock_client.put_roles_dry_run.return_value = False

        result = process_role_definitions(
            client=mock_client,
            workspace_id="ws",
            item_id="item",
            role_definitions=[sample_role_def],
            validate_first=True,
        )

        assert result.success is False
        assert "validation" in result.error.lower()
        mock_client.put_roles.assert_not_called()

    def test_handles_api_error(self, mock_client, sample_role_def):
        mock_client.put_roles.side_effect = Exception("500 Internal Server Error")

        result = process_role_definitions(
            client=mock_client,
            workspace_id="ws",
            item_id="item",
            role_definitions=[sample_role_def],
        )

        assert result.success is False
        assert "500" in result.error


class TestProcessUserMappings:
    """Orchestrate: parse mapping YAMLs → reconcile → PUT with ETag."""

    def test_applies_single_mapping(self, mock_client, sample_mapping):
        # Need existing role for mapping to find
        mock_client.list_roles.return_value = (
            [{"name": "TestRole", "id": "r1", "decisionRules": [],
              "members": {"microsoftEntraMembers": [], "fabricItemMembers": []}}],
            '"etag-1"',
        )

        result = process_user_mappings(
            client=mock_client,
            workspace_id="ws",
            item_id="item",
            tenant_id="t",
            user_mappings=[sample_mapping],
        )

        assert result.success is True
        assert result.mappings_applied == 1

    def test_no_mappings_is_noop(self, mock_client):
        result = process_user_mappings(
            client=mock_client,
            workspace_id="ws",
            item_id="item",
            tenant_id="t",
            user_mappings=[],
        )

        assert result.success is True
        assert result.mappings_applied == 0
        mock_client.put_roles.assert_not_called()

    def test_fails_if_role_not_found(self, mock_client, sample_mapping):
        mock_client.list_roles.return_value = ([], '"etag"')

        result = process_user_mappings(
            client=mock_client,
            workspace_id="ws",
            item_id="item",
            tenant_id="t",
            user_mappings=[sample_mapping],
        )

        assert result.success is False
        assert "TestRole" in result.error

    def test_retries_on_412(self, mock_client, sample_mapping):
        existing_role = {
            "name": "TestRole", "id": "r1", "decisionRules": [],
            "members": {"microsoftEntraMembers": [], "fabricItemMembers": []},
        }
        mock_client.list_roles.side_effect = [
            ([existing_role], '"etag-1"'),
            ([existing_role], '"etag-2"'),
        ]
        mock_client.put_roles.side_effect = [
            PreconditionFailedError("stale"),
            '"etag-3"',
        ]

        result = process_user_mappings(
            client=mock_client,
            workspace_id="ws",
            item_id="item",
            tenant_id="t",
            user_mappings=[sample_mapping],
        )

        assert result.success is True
        assert mock_client.put_roles.call_count == 2


class TestWorkflowResults:
    """Result dataclasses carry success/failure details."""

    def test_role_result_success(self):
        r = RoleWorkflowResult(success=True, roles_applied=3)
        assert r.success
        assert r.error is None

    def test_role_result_failure(self):
        r = RoleWorkflowResult(success=False, roles_applied=0, error="boom")
        assert not r.success
        assert r.error == "boom"

    def test_mapping_result_success(self):
        r = MappingWorkflowResult(success=True, mappings_applied=2)
        assert r.success

    def test_mapping_result_failure(self):
        r = MappingWorkflowResult(success=False, mappings_applied=0, error="nope")
        assert not r.success
