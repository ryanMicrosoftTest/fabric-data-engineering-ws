from __future__ import annotations

from unittest.mock import patch

import pytest


@pytest.fixture
def patched_module(mock_settings, mock_warehouse_writer):
    from activities import merge_notebook_runs as mod

    with (
        patch.object(mod, "get_settings", return_value=mock_settings),
        patch.object(mod, "WarehouseWriter", return_value=mock_warehouse_writer) as wh_cls,
        patch.object(mod, "DefaultAzureCredential") as default_cred,
        patch.object(mod, "ManagedIdentityCredential") as mi_cred,
    ):
        yield mod, wh_cls, default_cred, mi_cred, mock_warehouse_writer


def test_happy_path(patched_module):
    mod, wh_cls, _default, _mi, writer = patched_module
    writer.execute_proc.return_value = 42

    result = mod.merge_notebook_runs({"collector_run_id": "run-1"})

    assert result["success"] is True
    assert result["rows_written"] == 42
    assert result["duration_ms"] >= 0
    writer.execute_proc.assert_called_once_with("dbo.usp_merge_notebook_runs", {"collector_run_id": "run-1"})


def test_failure_returns_error(patched_module):
    mod, _wh_cls, _default, _mi, writer = patched_module

    class FakePyodbcError(Exception):
        pass

    writer.execute_proc.side_effect = FakePyodbcError("boom")

    result = mod.merge_notebook_runs({"collector_run_id": "run-2"})

    assert result["success"] is False
    assert result["rows_written"] == 0
    assert "FakePyodbcError" in result["error"]
    assert "boom" in result["error"]
    assert result["duration_ms"] >= 0


def test_credential_uses_managed_identity_when_client_id_set(mock_settings, mock_warehouse_writer):
    from activities import merge_notebook_runs as mod

    mock_settings.mi_client_id = "client-xyz"

    with (
        patch.object(mod, "get_settings", return_value=mock_settings),
        patch.object(mod, "WarehouseWriter", return_value=mock_warehouse_writer),
        patch.object(mod, "DefaultAzureCredential") as default_cred,
        patch.object(mod, "ManagedIdentityCredential") as mi_cred,
    ):
        mod.merge_notebook_runs({"collector_run_id": "run-3"})

    mi_cred.assert_called_once_with(client_id="client-xyz")
    default_cred.assert_not_called()


def test_credential_uses_default_when_no_client_id(mock_settings, mock_warehouse_writer):
    from activities import merge_notebook_runs as mod

    mock_settings.mi_client_id = None

    with (
        patch.object(mod, "get_settings", return_value=mock_settings),
        patch.object(mod, "WarehouseWriter", return_value=mock_warehouse_writer),
        patch.object(mod, "DefaultAzureCredential") as default_cred,
        patch.object(mod, "ManagedIdentityCredential") as mi_cred,
    ):
        mod.merge_notebook_runs({"collector_run_id": "run-4"})

    default_cred.assert_called_once_with()
    mi_cred.assert_not_called()
