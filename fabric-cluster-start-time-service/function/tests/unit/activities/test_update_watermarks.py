from __future__ import annotations

from unittest.mock import patch

import pytest


@pytest.fixture
def patched_module(mock_settings, mock_warehouse_writer):
    from activities import update_watermarks as mod

    with patch.object(mod, "get_settings", return_value=mock_settings), \
         patch.object(mod, "WarehouseWriter", return_value=mock_warehouse_writer) as wh_cls, \
         patch.object(mod, "DefaultAzureCredential") as default_cred, \
         patch.object(mod, "ManagedIdentityCredential") as mi_cred, \
         patch.object(mod, "update_watermark") as upd:
        yield mod, wh_cls, default_cred, mi_cred, mock_warehouse_writer, upd


def test_happy_path_updates_all_nine_sources(patched_module):
    mod, _wh_cls, _default, _mi, _writer, upd = patched_module

    result = mod.update_watermarks({"collector_run_id": "run-1"})

    assert result["success"] is True
    assert result["rows_written"] == 9
    assert result["duration_ms"] >= 0
    assert upd.call_count == 9
    sources_called = [c.args[1] for c in upd.call_args_list]
    assert set(sources_called) == {
        "workspace",
        "notebook_item",
        "notebook_definition",
        "environment",
        "spark_pool",
        "spark_application",
        "job_instance",
        "livy_session",
        "livy_statement",
    }


def test_workspace_id_passed_through(patched_module):
    mod, _wh_cls, _default, _mi, _writer, upd = patched_module

    mod.update_watermarks({"collector_run_id": "run-2", "workspace_id": "ws-42"})

    assert upd.call_count == 9
    for call in upd.call_args_list:
        # signature: update_watermark(wh, source, workspace_id, now, cri)
        assert call.args[2] == "ws-42"
        assert call.args[4] == "run-2"


def test_workspace_id_none_when_omitted(patched_module):
    mod, _wh_cls, _default, _mi, _writer, upd = patched_module

    mod.update_watermarks({"collector_run_id": "run-3"})

    for call in upd.call_args_list:
        assert call.args[2] is None


def test_partial_failure_returns_error_with_source(patched_module):
    mod, _wh_cls, _default, _mi, _writer, upd = patched_module

    def _side_effect(_wh, source, *_args, **_kwargs):
        if source == "spark_pool":
            raise RuntimeError("kaboom")

    upd.side_effect = _side_effect

    result = mod.update_watermarks({"collector_run_id": "run-4"})

    assert result["success"] is False
    assert result["rows_written"] == 0
    assert "spark_pool" in result["error"]
    assert "RuntimeError" in result["error"]


def test_credential_uses_managed_identity_when_client_id_set(mock_settings, mock_warehouse_writer):
    from activities import update_watermarks as mod

    mock_settings.mi_client_id = "client-mi"

    with patch.object(mod, "get_settings", return_value=mock_settings), \
         patch.object(mod, "WarehouseWriter", return_value=mock_warehouse_writer), \
         patch.object(mod, "DefaultAzureCredential") as default_cred, \
         patch.object(mod, "ManagedIdentityCredential") as mi_cred, \
         patch.object(mod, "update_watermark"):
        mod.update_watermarks({"collector_run_id": "run-5"})

    mi_cred.assert_called_once_with(client_id="client-mi")
    default_cred.assert_not_called()


def test_credential_uses_default_when_no_client_id(mock_settings, mock_warehouse_writer):
    from activities import update_watermarks as mod

    mock_settings.mi_client_id = None

    with patch.object(mod, "get_settings", return_value=mock_settings), \
         patch.object(mod, "WarehouseWriter", return_value=mock_warehouse_writer), \
         patch.object(mod, "DefaultAzureCredential") as default_cred, \
         patch.object(mod, "ManagedIdentityCredential") as mi_cred, \
         patch.object(mod, "update_watermark"):
        mod.update_watermarks({"collector_run_id": "run-6"})

    default_cred.assert_called_once_with()
    mi_cred.assert_not_called()
