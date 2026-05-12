from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest

from shared.fabric_client import FabricApiError

from ._helpers import fabric_client_factory, make_fabric_client_mock  # type: ignore[import-not-found]

PAYLOAD = {
    "collector_run_id": "run-1",
    "workspace_id": "ws-A",
    "to_watermark": "2024-01-15T10:00:00+00:00",
}


@pytest.fixture
def patched(mock_settings):
    from activities import collect_workspace as mod

    with (
        patch.object(mod, "get_settings", return_value=mock_settings),
        patch.object(mod, "get_credential", return_value=object()),
        patch.object(mod, "persist_raw", new=AsyncMock(return_value=1)) as persist,
    ):
        yield mod, persist


@pytest.mark.asyncio
async def test_happy_path(patched):
    mod, persist = patched
    client = make_fabric_client_mock(get_default={"id": "ws-A", "displayName": "WS"})
    with patch.object(mod, "FabricClient", side_effect=fabric_client_factory(client)):
        result = await mod.collect_workspace(dict(PAYLOAD))

    assert result["success"] is True
    assert result["rows_written"] == 1
    assert result["workspace_id"] == "ws-A"
    persist.assert_awaited_once()
    args, _ = persist.call_args
    assert args[2] == "raw.workspace"
    assert args[3] == ["workspace_id"]
    rows = args[4]
    assert len(rows) == 1
    assert rows[0]["workspace_id"] == "ws-A"
    assert "displayName" in rows[0]["raw_payload"]


@pytest.mark.asyncio
async def test_fabric_api_error(patched):
    mod, persist = patched
    client = make_fabric_client_mock()
    client.get = AsyncMock(side_effect=FabricApiError(500, "boom", "/v1/workspaces/ws-A"))
    with patch.object(mod, "FabricClient", side_effect=fabric_client_factory(client)):
        result = await mod.collect_workspace(dict(PAYLOAD))

    assert result["success"] is False
    assert result["rows_written"] == 0
    assert "boom" in result["error"]
    persist.assert_not_awaited()
