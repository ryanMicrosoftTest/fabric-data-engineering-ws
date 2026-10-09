"""Tests for lakehouse_audit — read-only role snapshot and disk persistence."""

import csv
import json
from unittest.mock import MagicMock

import pytest

from onelake_security.lakehouse_audit import (
    audit_lakehouse,
    api_role_to_audited_role,
    expand_group_members,
    resolve_member_names,
    save_report,
    LakehouseAuditReport,
)
from onelake_security.entra_directory import (
    DirectoryObject,
    DirectoryResolution,
    UnresolvedObject,
)


API_ROLE = {
    "id": "role-1",
    "name": "DoctorRole",
    "decisionRules": [
        {
            "effect": "Permit",
            "permission": [
                {
                    "attributeName": "Path",
                    "attributeValueIncludedIn": ["/Tables/doctor", "/Tables/patient"],
                },
                {"attributeName": "Action", "attributeValueIncludedIn": ["Read"]},
            ],
            "constraints": {
                "columns": [
                    {
                        "tablePath": "/Tables/patient",
                        "columnNames": ["id", "name"],
                        "columnEffect": "Permit",
                        "columnAction": ["Read"],
                    }
                ],
                "rows": [
                    {"tablePath": "/Tables/patient", "value": "region = 'East'"}
                ],
            },
        }
    ],
    "members": {
        "microsoftEntraMembers": [
            {"tenantId": "t1", "objectId": "u1", "objectType": "User"}
        ],
        "fabricItemMembers": [],
    },
}

ORPHAN_ROLE = {
    "id": "role-2",
    "name": "EmptyRole",
    "decisionRules": [
        {
            "effect": "Permit",
            "permission": [
                {"attributeName": "Path", "attributeValueIncludedIn": ["*"]},
                {"attributeName": "Action", "attributeValueIncludedIn": ["Read"]},
            ],
        }
    ],
    "members": {"microsoftEntraMembers": [], "fabricItemMembers": []},
}


ITEM_MEMBER_ROLE = {
    "id": "role-3",
    "name": "DefaultReader",
    "decisionRules": [
        {
            "effect": "Permit",
            "permission": [
                {"attributeName": "Path", "attributeValueIncludedIn": ["*"]},
                {"attributeName": "Action", "attributeValueIncludedIn": ["Read"]},
            ],
        }
    ],
    "members": {
        "fabricItemMembers": [
            {"sourcePath": "ws-1/item-1", "itemAccess": ["ReadAll"]}
        ]
    },
}


def _client(roles, etag='"abc"'):
    client = MagicMock()
    client.list_roles.return_value = (roles, etag)
    return client


class TestApiRoleToAuditedRole:
    def test_flattens_paths_columns_and_rows(self):
        role = api_role_to_audited_role(API_ROLE)

        assert role.name == "DoctorRole"
        assert role.role_id == "role-1"
        assert role.table_paths == ["/Tables/doctor", "/Tables/patient"]

        doctor, patient = role.permissions
        assert doctor.actions == ["Read"]
        assert doctor.column_names is None
        assert doctor.row_filter is None
        assert patient.column_names == ["id", "name"]
        assert patient.row_filter == "region = 'East'"
        assert patient.has_column_security
        assert patient.has_row_security

    def test_extracts_members(self):
        role = api_role_to_audited_role(API_ROLE)

        assert role.member_count == 1
        assert role.entra_members[0].object_id == "u1"
        assert role.entra_members[0].object_type == "User"
        assert role.entra_members[0].tenant_id == "t1"
        assert not role.is_orphaned

    def test_role_without_members_is_orphaned(self):
        role = api_role_to_audited_role(ORPHAN_ROLE)

        assert role.member_count == 0
        assert role.is_orphaned

    def test_extracts_fabric_item_members(self):
        role = api_role_to_audited_role(ITEM_MEMBER_ROLE)

        assert role.member_count == 1
        assert not role.is_orphaned
        assert role.entra_members == []
        assert role.fabric_item_members[0].source_path == "ws-1/item-1"
        assert role.fabric_item_members[0].item_access == ["ReadAll"]

    def test_handles_missing_fields(self):
        role = api_role_to_audited_role({"name": "Bare"})

        assert role.name == "Bare"
        assert role.role_id is None
        assert role.permissions == []
        assert role.entra_members == []


class TestAuditLakehouse:
    def test_returns_report_for_all_roles(self):
        client = _client([API_ROLE, ORPHAN_ROLE])

        report = audit_lakehouse(client, "ws-1", "item-1", lakehouse_name="lh")

        client.list_roles.assert_called_once_with("ws-1", "item-1")
        assert report.role_count == 2
        assert report.total_members == 1
        assert report.lakehouse_item_id == "item-1"
        assert report.lakehouse_name == "lh"
        assert report.etag == '"abc"'
        assert [r.name for r in report.orphaned_roles] == ["EmptyRole"]

    def test_empty_lakehouse(self):
        report = audit_lakehouse(_client([]), "ws-1", "item-1")

        assert report.role_count == 0
        assert report.roles == []

    def test_requires_ids(self):
        with pytest.raises(ValueError):
            audit_lakehouse(_client([]), "", "item-1")
        with pytest.raises(ValueError):
            audit_lakehouse(_client([]), "ws-1", "")


class TestReportConstruction:
    def test_constructs_without_roles(self):
        report = LakehouseAuditReport("ws-1", "item-1")

        assert report.roles == []
        assert report.role_count == 0
        assert report.total_members == 0
        assert report.lakehouse_item_id == "item-1"
        assert report.captured_at is not None

    def test_roles_are_appendable_after_construction(self):
        report = LakehouseAuditReport("ws-1", "item-1", lakehouse_name="lh")
        report.roles.append(api_role_to_audited_role(API_ROLE))

        assert report.role_count == 1
        assert report.total_members == 1


class TestEntraEnrichment:
    """Member object IDs resolved to friendly Entra names."""

    def _directory(self, mapping, unresolved=None):
        directory = MagicMock()
        directory.resolve.return_value = DirectoryResolution(
            resolved=mapping,
            unresolved=list(unresolved or []),
        )
        return directory

    def test_fills_display_name_and_type(self):
        report = audit_lakehouse(_client([API_ROLE]), "ws-1", "item-1")
        directory = self._directory({
            "u1": DirectoryObject(
                object_id="u1",
                display_name="Ada Lovelace",
                object_type="User",
                user_principal_name="ada@contoso.com",
            )
        })

        unresolved = resolve_member_names(report, directory)

        member = report.roles[0].entra_members[0]
        assert member.display_name == "Ada Lovelace"
        assert member.object_type == "User"
        assert member.user_principal_name == "ada@contoso.com"
        assert member.friendly_name == "Ada Lovelace"
        assert unresolved == []

    def test_resolves_group_display_name(self):
        report = audit_lakehouse(_client([API_ROLE]), "ws-1", "item-1")
        directory = self._directory({
            "u1": DirectoryObject(
                object_id="u1",
                display_name="AAD-FABRIC-PRIVATE-SQL-ADMINS",
                object_type="Group",
            )
        })

        resolve_member_names(report, directory)

        member = report.roles[0].entra_members[0]
        assert member.friendly_name == "AAD-FABRIC-PRIVATE-SQL-ADMINS"
        assert member.object_type == "Group"

    def test_reports_unresolved_ids(self):
        report = audit_lakehouse(_client([API_ROLE]), "ws-1", "item-1")

        unresolved = resolve_member_names(
            report,
            self._directory({}, [UnresolvedObject("u1", "Object not found")]),
        )

        assert [u.object_id for u in unresolved] == ["u1"]
        member = report.roles[0].entra_members[0]
        assert member.display_name is None
        assert member.friendly_name == "u1"
        assert member.is_resolved is False
        assert member.resolution_note == "Object not found"
        assert report.unresolved_members == [member]

    def test_skips_lookup_when_no_entra_members(self):
        report = audit_lakehouse(_client([ITEM_MEMBER_ROLE]), "ws-1", "item-1")
        directory = self._directory({})

        assert resolve_member_names(report, directory) == []
        directory.resolve.assert_not_called()

    def test_audit_lakehouse_enriches_when_directory_supplied(self):
        directory = self._directory({
            "u1": DirectoryObject(
                object_id="u1", display_name="Ada Lovelace", object_type="User"
            )
        })

        report = audit_lakehouse(
            _client([API_ROLE]), "ws-1", "item-1", directory=directory
        )

        assert report.roles[0].entra_members[0].display_name == "Ada Lovelace"

    def test_names_appear_in_markdown_and_rows(self):
        report = audit_lakehouse(_client([API_ROLE]), "ws-1", "item-1")
        resolve_member_names(report, self._directory({
            "u1": DirectoryObject(
                object_id="u1", display_name="Ada Lovelace", object_type="User"
            )
        }))

        assert "Ada Lovelace" in report.to_markdown()
        assert report.to_rows()[0]["member_display_name"] == "Ada Lovelace"
        assert report.to_rows()[0]["member_object_type"] == "User"


class TestGroupExpansion:
    """Group members exploded into the principals that actually hold access."""

    def _directory(self, group_members):
        directory = MagicMock()
        directory.resolve.return_value = DirectoryResolution(
            resolved={
                "u1": DirectoryObject(
                    object_id="u1",
                    display_name="AAD-NEUROLOGY-READERS",
                    object_type="Group",
                )
            }
        )
        directory.get_group_members.return_value = group_members
        return directory

    def _expanded_report(self):
        directory = self._directory([
            DirectoryObject(
                object_id="user-a",
                display_name="Ada Lovelace",
                object_type="User",
                user_principal_name="ada@contoso.com",
            ),
            DirectoryObject(
                object_id="user-b",
                display_name="Grace Hopper",
                object_type="User",
                user_principal_name="grace@contoso.com",
            ),
        ])
        report = audit_lakehouse(_client([API_ROLE]), "ws-1", "item-1")
        resolve_member_names(report, directory)
        counts = expand_group_members(report, directory)
        return report, directory, counts

    def test_fills_group_members(self):
        report, _, counts = self._expanded_report()

        member = report.roles[0].entra_members[0]
        assert counts == {"u1": 2}
        assert member.is_group is True
        assert member.group_member_count == 2
        assert [g.friendly_name for g in member.group_members] == [
            "Ada Lovelace",
            "Grace Hopper",
        ]

    def test_defaults_to_transitive_expansion(self):
        _, directory, _ = self._expanded_report()

        directory.get_group_members.assert_called_with("u1", transitive=True)

    def test_skips_non_group_members(self):
        directory = MagicMock()
        directory.resolve.return_value = DirectoryResolution(
            resolved={
                "u1": DirectoryObject(
                    object_id="u1", display_name="Ada Lovelace", object_type="User"
                )
            }
        )
        report = audit_lakehouse(_client([API_ROLE]), "ws-1", "item-1")
        resolve_member_names(report, directory)

        assert expand_group_members(report, directory) == {}
        directory.get_group_members.assert_not_called()

    def test_audit_lakehouse_expands_when_requested(self):
        directory = self._directory([
            DirectoryObject(object_id="user-a", display_name="Ada", object_type="User")
        ])

        report = audit_lakehouse(
            _client([API_ROLE]),
            "ws-1",
            "item-1",
            directory=directory,
            expand_groups=True,
        )

        assert report.roles[0].entra_members[0].group_member_count == 1

    def test_group_members_appear_in_markdown_and_rows(self):
        report, _, _ = self._expanded_report()

        markdown = report.to_markdown()
        assert "Inside `AAD-NEUROLOGY-READERS`" in markdown
        assert "Grace Hopper" in markdown

        row = report.to_rows()[0]
        assert row["member_group_member_count"] == 2
        assert "Ada Lovelace" in row["member_group_members"]

    def test_effective_user_rows_explode_the_group(self):
        report, _, _ = self._expanded_report()

        rows = report.to_effective_user_rows()

        assert {r["principal_name"] for r in rows} == {
            "Ada Lovelace",
            "Grace Hopper",
        }
        assert all(r["granted_via"] == "AAD-NEUROLOGY-READERS" for r in rows)
        assert rows[0]["granted_via_object_id"] == "u1"

    def test_effective_user_rows_mark_direct_grants(self):
        directory = MagicMock()
        directory.resolve.return_value = DirectoryResolution(
            resolved={
                "u1": DirectoryObject(
                    object_id="u1", display_name="Ada Lovelace", object_type="User"
                )
            }
        )
        report = audit_lakehouse(_client([API_ROLE]), "ws-1", "item-1")
        resolve_member_names(report, directory)

        rows = report.to_effective_user_rows()

        assert rows[0]["granted_via"] == "direct"
        assert rows[0]["principal_name"] == "Ada Lovelace"


class TestSerialization:
    def test_to_json_round_trips(self):
        report = audit_lakehouse(_client([API_ROLE]), "ws-1", "item-1")

        data = json.loads(report.to_json())

        assert data["role_count"] == 1
        assert data["lakehouse_item_id"] == "item-1"
        assert data["roles"][0]["name"] == "DoctorRole"
        assert "raw" not in data["roles"][0]

    def test_to_json_include_raw(self):
        report = audit_lakehouse(_client([API_ROLE]), "ws-1", "item-1")

        data = json.loads(report.to_json(include_raw=True))

        assert data["roles"][0]["raw"] == API_ROLE

    def test_to_rows_expands_role_path_member(self):
        report = audit_lakehouse(_client([API_ROLE, ORPHAN_ROLE]), "ws-1", "item-1")

        rows = report.to_rows()

        assert len(rows) == 3
        assert rows[1]["role_name"] == "DoctorRole"
        assert rows[1]["path"] == "/Tables/patient"
        assert rows[1]["column_names"] == "id;name"
        assert rows[1]["member_object_id"] == "u1"
        assert rows[1]["member_kind"] == "microsoftEntraMember"
        assert rows[2]["member_object_id"] == ""
        assert rows[2]["member_kind"] == "none"

    def test_to_rows_includes_fabric_item_members(self):
        report = audit_lakehouse(_client([ITEM_MEMBER_ROLE]), "ws-1", "item-1")

        rows = report.to_rows()

        assert len(rows) == 1
        assert rows[0]["member_kind"] == "fabricItemMember"
        assert rows[0]["member_source_path"] == "ws-1/item-1"
        assert rows[0]["member_item_access"] == "ReadAll"
        assert rows[0]["is_orphaned"] is False

    def test_markdown_renders_fabric_item_members(self):
        report = audit_lakehouse(_client([ITEM_MEMBER_ROLE]), "ws-1", "item-1")

        md = report.to_markdown()

        assert "Fabric item member" in md
        assert "ws-1/item-1" in md
        assert "ReadAll" in md
        assert "_No members assigned._" not in md

    def test_markdown_flags_truly_memberless_role(self):
        report = audit_lakehouse(_client([ORPHAN_ROLE]), "ws-1", "item-1")

        assert "_No members assigned._" in report.to_markdown()

    def test_to_markdown_contains_role_names(self):
        report = audit_lakehouse(_client([API_ROLE]), "ws-1", "item-1")

        md = report.to_markdown()

        assert "## DoctorRole" in md
        assert "/Tables/patient" in md


class TestSaveReport:
    def test_saves_json(self, tmp_path):
        report = audit_lakehouse(_client([API_ROLE]), "ws-1", "item-1")
        target = tmp_path / "nested" / "audit.json"

        save_report(report, str(target))

        data = json.loads(target.read_text(encoding="utf-8"))
        assert data["roles"][0]["name"] == "DoctorRole"

    def test_saves_csv(self, tmp_path):
        report = audit_lakehouse(_client([API_ROLE]), "ws-1", "item-1")
        target = tmp_path / "audit.csv"

        save_report(report, str(target), format="csv")

        with open(target, newline="", encoding="utf-8") as f:
            rows = list(csv.DictReader(f))
        assert len(rows) == 2
        assert rows[0]["role_name"] == "DoctorRole"

    def test_saves_empty_csv_with_header(self, tmp_path):
        report = LakehouseAuditReport(workspace_id="ws-1", lakehouse_item_id="item-1")
        target = tmp_path / "empty.csv"

        save_report(report, str(target), format="csv")

        header = target.read_text(encoding="utf-8").splitlines()[0]
        assert "role_name" in header

    def test_saves_markdown(self, tmp_path):
        report = audit_lakehouse(_client([API_ROLE]), "ws-1", "item-1")
        target = tmp_path / "audit.md"

        save_report(report, str(target), format="markdown")

        assert "## DoctorRole" in target.read_text(encoding="utf-8")

    def test_rejects_unknown_format(self, tmp_path):
        report = LakehouseAuditReport(workspace_id="ws-1", lakehouse_item_id="item-1")

        with pytest.raises(ValueError):
            save_report(report, str(tmp_path / "x.txt"), format="xml")
