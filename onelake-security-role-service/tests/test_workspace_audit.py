"""Tests for workspace_audit — workspace-level role assignment snapshots."""

import csv
import json
from unittest.mock import MagicMock

import pytest

from onelake_security.entra_directory import DirectoryObject
from onelake_security.workspace_audit import (
    audit_workspace_roles,
    api_assignment_to_role_assignment,
    expand_workspace_groups,
    role_rank,
    save_workspace_report,
    WorkspaceAuditReport,
)


ADMIN_GROUP = {
    "id": "ra-1",
    "principal": {
        "id": "grp-admin",
        "displayName": "APP-FABRIC-ADMINS",
        "type": "Group",
        "groupDetails": {"groupType": "SecurityGroup"},
    },
    "role": "Admin",
}

VIEWER_USER = {
    "id": "ra-2",
    "principal": {
        "id": "user-ada",
        "displayName": "Ada Lovelace",
        "type": "User",
        "userDetails": {"userPrincipalName": "ada@contoso.com"},
    },
    "role": "Viewer",
}

CONTRIBUTOR_SPN = {
    "id": "ra-3",
    "principal": {
        "id": "spn-1",
        "displayName": "fabric-prod-spn",
        "type": "ServicePrincipal",
        "servicePrincipalDetails": {"aadAppId": "app-123"},
    },
    "role": "Contributor",
}

ALL_ASSIGNMENTS = [ADMIN_GROUP, VIEWER_USER, CONTRIBUTOR_SPN]


def _client(assignments, workspace=None):
    client = MagicMock()
    client.list_workspace_role_assignments.return_value = assignments
    client.get_workspace.return_value = workspace or {
        "displayName": "healthcare_ws",
        "capacityId": "cap-1",
    }
    return client


def _directory(members):
    directory = MagicMock()
    directory.get_group_members.return_value = members
    return directory


GROUP_MEMBERS = [
    DirectoryObject(
        object_id="user-ada",
        display_name="Ada Lovelace",
        object_type="User",
        user_principal_name="ada@contoso.com",
    ),
    DirectoryObject(
        object_id="user-grace",
        display_name="Grace Hopper",
        object_type="User",
        user_principal_name="grace@contoso.com",
    ),
]


class TestParsing:
    def test_maps_user_assignment(self):
        assignment = api_assignment_to_role_assignment(VIEWER_USER)

        assert assignment.principal_id == "user-ada"
        assert assignment.role == "Viewer"
        assert assignment.principal_type == "User"
        assert assignment.user_principal_name == "ada@contoso.com"
        assert assignment.friendly_name == "Ada Lovelace"
        assert assignment.is_group is False
        assert assignment.is_elevated is False

    def test_maps_group_assignment(self):
        assignment = api_assignment_to_role_assignment(ADMIN_GROUP)

        assert assignment.is_group is True
        assert assignment.group_type == "SecurityGroup"
        assert assignment.is_elevated is True

    def test_maps_service_principal_assignment(self):
        assignment = api_assignment_to_role_assignment(CONTRIBUTOR_SPN)

        assert assignment.principal_type == "ServicePrincipal"
        assert assignment.aad_app_id == "app-123"
        assert assignment.is_elevated is False

    def test_falls_back_to_object_id_without_name(self):
        assignment = api_assignment_to_role_assignment(
            {"principal": {"id": "bare"}, "role": "Viewer"}
        )

        assert assignment.friendly_name == "bare"


class TestAuditWorkspaceRoles:
    def test_reads_every_assignment(self):
        report = audit_workspace_roles(_client(ALL_ASSIGNMENTS), "ws-1")

        assert report.assignment_count == 3
        assert report.workspace_name == "healthcare_ws"
        assert report.capacity_id == "cap-1"

    def test_requires_workspace_id(self):
        with pytest.raises(ValueError):
            audit_workspace_roles(_client([]), "")

    def test_skips_name_lookup_when_supplied(self):
        client = _client(ALL_ASSIGNMENTS)

        report = audit_workspace_roles(client, "ws-1", workspace_name="given")

        assert report.workspace_name == "given"
        client.get_workspace.assert_not_called()

    def test_counts_roles_and_flags_elevated(self):
        report = audit_workspace_roles(_client(ALL_ASSIGNMENTS), "ws-1")

        assert report.role_counts == {"Admin": 1, "Viewer": 1, "Contributor": 1}
        assert [a.friendly_name for a in report.elevated_assignments] == [
            "APP-FABRIC-ADMINS"
        ]

    def test_expands_groups_when_directory_supplied(self):
        directory = _directory(GROUP_MEMBERS)

        report = audit_workspace_roles(
            _client(ALL_ASSIGNMENTS), "ws-1", directory=directory
        )

        assert report.assignments[0].group_member_count == 2
        directory.get_group_members.assert_called_once_with(
            "grp-admin", transitive=True
        )

    def test_expansion_can_be_disabled(self):
        directory = _directory(GROUP_MEMBERS)

        report = audit_workspace_roles(
            _client(ALL_ASSIGNMENTS), "ws-1",
            directory=directory, expand_groups=False,
        )

        directory.get_group_members.assert_not_called()
        assert report.unexpanded_groups == report.group_assignments
        assert report.empty_groups == []

    def test_markdown_flags_empty_group_grants(self):
        report = audit_workspace_roles(
            _client(ALL_ASSIGNMENTS), "ws-1", directory=_directory([])
        )

        markdown = report.to_markdown()

        assert "Empty group grants: 1" in markdown
        assert "0 — empty group" in markdown
        assert "grant access to nobody" in markdown

    def test_markdown_marks_unexpanded_groups(self):
        report = audit_workspace_roles(_client(ALL_ASSIGNMENTS), "ws-1")

        assert "not expanded" in report.to_markdown()


class TestExpandWorkspaceGroups:
    def test_only_groups_are_expanded(self):
        directory = _directory(GROUP_MEMBERS)
        report = audit_workspace_roles(_client(ALL_ASSIGNMENTS), "ws-1")

        counts = expand_workspace_groups(report, directory)

        assert counts == {"grp-admin": 2}
        assert directory.get_group_members.call_count == 1

    def test_unexpanded_groups_are_reported(self):
        """A group we never looked at is unknown, not empty."""
        report = audit_workspace_roles(_client(ALL_ASSIGNMENTS), "ws-1")

        assert [a.friendly_name for a in report.unexpanded_groups] == [
            "APP-FABRIC-ADMINS"
        ]
        assert report.empty_groups == []

    def test_empty_group_is_distinguished_from_unexpanded(self):
        directory = _directory([])
        report = audit_workspace_roles(_client(ALL_ASSIGNMENTS), "ws-1")

        expand_workspace_groups(report, directory)

        assert report.unexpanded_groups == []
        assert [a.friendly_name for a in report.empty_groups] == ["APP-FABRIC-ADMINS"]

    def test_empty_group_grants_nobody_effective_access(self):
        report = audit_workspace_roles(
            _client(ALL_ASSIGNMENTS), "ws-1", directory=_directory([])
        )

        names = [r["principal_name"] for r in report.effective_principals()]

        assert "APP-FABRIC-ADMINS" not in names
        assert names == ["fabric-prod-spn", "Ada Lovelace"]


class TestEffectivePrincipals:
    def test_group_members_inherit_the_group_role(self):
        report = audit_workspace_roles(
            _client(ALL_ASSIGNMENTS), "ws-1", directory=_directory(GROUP_MEMBERS)
        )

        rows = {r["principal_name"]: r for r in report.effective_principals()}

        assert rows["Grace Hopper"]["effective_role"] == "Admin"
        assert "Admin via APP-FABRIC-ADMINS" in rows["Grace Hopper"]["granted_via"]

    def test_highest_role_wins_across_paths(self):
        """Ada is a direct Viewer and sits in the Admin group — she is an Admin."""
        report = audit_workspace_roles(
            _client(ALL_ASSIGNMENTS), "ws-1", directory=_directory(GROUP_MEMBERS)
        )

        ada = next(
            r for r in report.effective_principals()
            if r["principal_name"] == "Ada Lovelace"
        )

        assert ada["effective_role"] == "Admin"
        assert "Admin via APP-FABRIC-ADMINS" in ada["granted_via"]
        assert "Viewer via direct" in ada["granted_via"]

    def test_deduplicates_people_reached_twice(self):
        report = audit_workspace_roles(
            _client(ALL_ASSIGNMENTS), "ws-1", directory=_directory(GROUP_MEMBERS)
        )

        names = [r["principal_name"] for r in report.effective_principals()]

        assert names.count("Ada Lovelace") == 1

    def test_sorted_most_privileged_first(self):
        report = audit_workspace_roles(
            _client(ALL_ASSIGNMENTS), "ws-1", directory=_directory(GROUP_MEMBERS)
        )

        roles = [r["effective_role"] for r in report.effective_principals()]

        assert roles == sorted(roles, key=role_rank)

    def test_unexpanded_group_stays_a_single_row(self):
        report = audit_workspace_roles(_client([ADMIN_GROUP]), "ws-1")

        rows = report.effective_principals()

        assert len(rows) == 1
        assert rows[0]["principal_name"] == "APP-FABRIC-ADMINS"
        assert rows[0]["granted_via"] == "Admin via direct"


class TestRoleRank:
    def test_orders_by_privilege(self):
        assert role_rank("Admin") < role_rank("Member")
        assert role_rank("Member") < role_rank("Contributor")
        assert role_rank("Contributor") < role_rank("Viewer")

    def test_unknown_role_sorts_last(self):
        assert role_rank("Mystery") > role_rank("Viewer")
        assert role_rank(None) > role_rank("Viewer")


class TestSerialization:
    def test_to_json_round_trips(self):
        report = audit_workspace_roles(
            _client(ALL_ASSIGNMENTS), "ws-1", directory=_directory(GROUP_MEMBERS)
        )

        data = json.loads(report.to_json())

        assert data["workspace_id"] == "ws-1"
        assert data["assignment_count"] == 3
        assert data["role_counts"]["Admin"] == 1
        assert len(data["effective_principals"]) == 3

    def test_to_rows_is_one_row_per_assignment(self):
        report = audit_workspace_roles(
            _client(ALL_ASSIGNMENTS), "ws-1", directory=_directory(GROUP_MEMBERS)
        )

        rows = report.to_rows()

        assert len(rows) == 3
        group_row = next(r for r in rows if r["principal_type"] == "Group")
        assert group_row["group_member_count"] == 2
        assert "Grace Hopper" in group_row["group_members"]

    def test_effective_rows_carry_workspace_context(self):
        report = audit_workspace_roles(
            _client(ALL_ASSIGNMENTS), "ws-1", directory=_directory(GROUP_MEMBERS)
        )

        row = report.to_effective_user_rows()[0]

        assert row["workspace_id"] == "ws-1"
        assert row["workspace_name"] == "healthcare_ws"

    def test_markdown_lists_groups_and_effective_access(self):
        report = audit_workspace_roles(
            _client(ALL_ASSIGNMENTS), "ws-1", directory=_directory(GROUP_MEMBERS)
        )

        markdown = report.to_markdown()

        assert "# Workspace Role Audit — healthcare_ws" in markdown
        assert "Inside `APP-FABRIC-ADMINS` (Admin)" in markdown
        assert "Grace Hopper" in markdown
        assert "Effective access — one row per person" in markdown

    def test_markdown_handles_empty_workspace(self):
        report = audit_workspace_roles(_client([]), "ws-1")

        assert "Assignments: 0" in report.to_markdown()


class TestSaveWorkspaceReport:
    def test_writes_json(self, tmp_path):
        report = audit_workspace_roles(_client(ALL_ASSIGNMENTS), "ws-1")
        path = tmp_path / "nested" / "ws.json"

        save_workspace_report(report, str(path))

        assert json.loads(path.read_text(encoding="utf-8"))["assignment_count"] == 3

    def test_writes_assignment_csv(self, tmp_path):
        report = audit_workspace_roles(_client(ALL_ASSIGNMENTS), "ws-1")
        path = tmp_path / "ws.csv"

        save_workspace_report(report, str(path), format="csv")

        rows = list(csv.DictReader(path.read_text(encoding="utf-8").splitlines()))
        assert len(rows) == 3

    def test_writes_effective_csv(self, tmp_path):
        report = audit_workspace_roles(
            _client(ALL_ASSIGNMENTS), "ws-1", directory=_directory(GROUP_MEMBERS)
        )
        path = tmp_path / "effective.csv"

        save_workspace_report(report, str(path), format="effective_csv")

        rows = list(csv.DictReader(path.read_text(encoding="utf-8").splitlines()))
        assert {r["principal_name"] for r in rows} == {
            "Ada Lovelace", "Grace Hopper", "fabric-prod-spn"
        }

    def test_writes_markdown(self, tmp_path):
        report = audit_workspace_roles(_client(ALL_ASSIGNMENTS), "ws-1")
        path = tmp_path / "ws.md"

        save_workspace_report(report, str(path), format="markdown")

        assert "Workspace Role Audit" in path.read_text(encoding="utf-8")

    def test_empty_csv_still_has_header(self, tmp_path):
        report = WorkspaceAuditReport(workspace_id="ws-1")
        path = tmp_path / "empty.csv"

        save_workspace_report(report, str(path), format="csv")

        assert path.read_text(encoding="utf-8").startswith("workspace_id,")

    def test_rejects_unknown_format(self, tmp_path):
        report = audit_workspace_roles(_client(ALL_ASSIGNMENTS), "ws-1")

        with pytest.raises(ValueError, match="Unsupported format"):
            save_workspace_report(report, str(tmp_path / "x.txt"), format="txt")
