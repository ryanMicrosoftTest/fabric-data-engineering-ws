from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest


def test_build_row_shape_and_timestamp():
    from activities import _common as mod

    payload = {"id": "n1", "name": "Notebook"}
    row = mod.build_row(payload, {"workspace_id": "w1", "notebook_id": "n1"}, "run-x")

    assert row["workspace_id"] == "w1"
    assert row["notebook_id"] == "n1"
    assert row["collector_run_id"] == "run-x"
    assert isinstance(row["ingested_at"], datetime)
    assert row["ingested_at"].tzinfo is timezone.utc
    assert '"id": "n1"' in row["raw_payload"]
    assert '"name": "Notebook"' in row["raw_payload"]


def test_build_row_serializes_non_json_types():
    from activities import _common as mod

    payload = {"when": datetime(2024, 1, 2, 3, 4, 5, tzinfo=timezone.utc)}
    row = mod.build_row(payload, {"workspace_id": "w"}, "r")
    assert "2024-01-02" in row["raw_payload"]


@pytest.mark.asyncio
async def test_persist_raw_empty_returns_zero_without_writer(mock_settings):
    from activities import _common as mod

    with patch.object(mod, "WarehouseWriter") as wh_cls:
        n = await mod.persist_raw(mock_settings, MagicMock(), "raw.t", ["k"], [], "run-1")
    assert n == 0
    wh_cls.assert_not_called()


@pytest.mark.asyncio
async def test_persist_raw_calls_writer_context_and_upsert(mock_settings, mock_warehouse_writer):
    from activities import _common as mod

    mock_warehouse_writer.upsert_raw.return_value = 3
    with patch.object(mod, "WarehouseWriter", return_value=mock_warehouse_writer) as wh_cls:
        n = await mod.persist_raw(
            mock_settings,
            MagicMock(),
            "raw.t",
            ["k"],
            [{"k": "v"}],
            "run-1",
        )
    assert n == 3
    wh_cls.assert_called_once()
    mock_warehouse_writer.__enter__.assert_called_once()
    mock_warehouse_writer.__exit__.assert_called_once()
    mock_warehouse_writer.upsert_raw.assert_called_once_with("raw.t", ["k"], [{"k": "v"}], "run-1")


def test_get_credential_uses_managed_identity_when_client_id(mock_settings):
    from activities import _common as mod

    mock_settings.mi_client_id = "client-xyz"
    with patch.object(mod, "ManagedIdentityCredential") as mi, patch.object(mod, "DefaultAzureCredential") as default:
        mod.get_credential(mock_settings)
    mi.assert_called_once_with(client_id="client-xyz")
    default.assert_not_called()


def test_get_credential_uses_default_otherwise(mock_settings):
    from activities import _common as mod

    mock_settings.mi_client_id = None
    with patch.object(mod, "ManagedIdentityCredential") as mi, patch.object(mod, "DefaultAzureCredential") as default:
        mod.get_credential(mock_settings)
    default.assert_called_once_with()
    mi.assert_not_called()
