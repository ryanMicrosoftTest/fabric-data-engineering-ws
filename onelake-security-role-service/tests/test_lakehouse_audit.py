"""Tests for lakehouse_audit — read-only role snapshot and disk persistence."""

import csv
import json
from unittest.mock import MagicMock

import pytest

from onelake_security.lakehouse_audit import (
    audit_lakehouse,
    api_role_to_audited_role,
    save_report,
    LakehouseAuditReport,
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


class TestSerialization:
    def test_to_json_round_trips(self):
        report = audit_lakehouse(_client([API_ROLE]), "ws-1", "item-1")

        data = json.loads(report.to_json())

        assert data["role_count"] == 1
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
        assert rows[2]["member_object_id"] == ""

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
        report = LakehouseAuditReport(workspace_id="ws-1", item_id="item-1")
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
        report = LakehouseAuditReport(workspace_id="ws-1", item_id="item-1")

        with pytest.raises(ValueError):
            save_report(report, str(tmp_path / "x.txt"), format="xml")
