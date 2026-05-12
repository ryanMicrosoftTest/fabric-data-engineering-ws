from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest

from shared.fabric_client import FabricApiError

from ._helpers import fabric_client_factory, make_fabric_client_mock  # type: ignore[import-not-found]


def _payload(**extra):
    base = {
        "collector_run_id": "run-1",
        "workspace_id": "ws-A",
        "to_watermark": "2024-01-15T10:00:00+00:00",
        "notebook_ids": ["n1", "n2", "n3"],
    }
    base.update(extra)
    return base


@pytest.fixture
def patched(mock_settings):
    from activities import collect_notebook_definitions as mod

    with patch.object(mod, "get_settings", return_value=mock_settings), patch.object(
        mod, "get_credential", return_value=object()
    ), patch.object(mod, "persist_raw", new=AsyncMock(return_value=3)) as persist:
        yield mod, persist


@pytest.mark.asyncio
async def test_happy_path_calls_lro_for_each_notebook(patched):
    mod, persist = patched
    responses = {
        f"/v1/workspaces/ws-A/notebooks/{n}/getDefinition?format=ipynb": {"definition": n}
        for n in ("n1", "n2", "n3")
    }
    client = make_fabric_client_mock(post_lro_responses=responses)
    with patch.object(mod, "FabricClient", side_effect=fabric_client_factory(client)):
        result = await mod.collect_notebook_definitions(_payload())

    assert result["success"] is True
    assert result["rows_written"] == 3
    assert client.post_lro.await_count == 3
    args, _ = persist.call_args
    assert args[2] == "raw.notebook_definition"
    assert args[3] == ["workspace_id", "notebook_id"]
    rows = args[4]
    assert {r["notebook_id"] for r in rows} == {"n1", "n2", "n3"}


@pytest.mark.asyncio
async def test_no_notebook_ids_skips_calls(patched):
    mod, persist = patched
    persist.return_value = 0
    client = make_fabric_client_mock()
    with patch.object(mod, "FabricClient", side_effect=fabric_client_factory(client)):
        result = await mod.collect_notebook_definitions(_payload(notebook_ids=[]))

    assert result["success"] is True
    assert result["rows_written"] == 0
    client.post_lro.assert_not_awaited()


@pytest.mark.asyncio
async def test_fabric_api_error_is_caught(patched):
    mod, persist = patched
    client = make_fabric_client_mock()
    client.post_lro = AsyncMock(side_effect=FabricApiError(500, "lro fail", "/v1/x"))
    with patch.object(mod, "FabricClient", side_effect=fabric_client_factory(client)):
        result = await mod.collect_notebook_definitions(_payload())

    assert result["success"] is False
    assert "lro fail" in result["error"]
    persist.assert_not_awaited()
