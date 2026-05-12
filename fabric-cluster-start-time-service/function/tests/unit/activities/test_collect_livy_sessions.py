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
        "livy_session_ids": ["livy-1", "livy-2", "livy-3"],
    }
    base.update(extra)
    return base


@pytest.fixture
def patched(mock_settings):
    from activities import collect_livy_sessions as mod

    with (
        patch.object(mod, "get_settings", return_value=mock_settings),
        patch.object(mod, "get_credential", return_value=object()),
        patch.object(mod, "persist_raw", new=AsyncMock(return_value=3)) as persist,
    ):
        yield mod, persist


@pytest.mark.asyncio
async def test_happy_path(patched):
    mod, persist = patched
    responses = {
        f"/v1/workspaces/ws-A/spark/livySessions/{lid}": {"id": lid, "state": "idle"}
        for lid in ("livy-1", "livy-2", "livy-3")
    }
    client = make_fabric_client_mock(get_responses=responses)
    with patch.object(mod, "FabricClient", side_effect=fabric_client_factory(client)):
        result = await mod.collect_livy_sessions(_payload())

    assert result["success"] is True
    assert result["rows_written"] == 3
    args, _ = persist.call_args
    assert args[2] == "raw.livy_session"
    assert args[3] == ["workspace_id", "livy_session_id"]
    rows = args[4]
    assert {r["livy_session_id"] for r in rows} == {"livy-1", "livy-2", "livy-3"}


@pytest.mark.asyncio
async def test_404_skipped(patched):
    mod, persist = patched

    async def _get(path, params=None):
        if "livy-2" in path:
            raise FabricApiError(404, "gone", path)
        return {"id": "x"}

    client = make_fabric_client_mock()
    client.get = AsyncMock(side_effect=_get)
    with patch.object(mod, "FabricClient", side_effect=fabric_client_factory(client)):
        await mod.collect_livy_sessions(_payload())

    args, _ = persist.call_args
    rows = args[4]
    assert {r["livy_session_id"] for r in rows} == {"livy-1", "livy-3"}


@pytest.mark.asyncio
async def test_no_ids(patched):
    mod, persist = patched
    persist.return_value = 0
    client = make_fabric_client_mock()
    with patch.object(mod, "FabricClient", side_effect=fabric_client_factory(client)):
        result = await mod.collect_livy_sessions(_payload(livy_session_ids=[]))
    assert result["success"] is True
    assert result["rows_written"] == 0


@pytest.mark.asyncio
async def test_non_404_fabric_error(patched):
    mod, persist = patched

    async def _get(path, params=None):
        raise FabricApiError(500, "boom", path)

    client = make_fabric_client_mock()
    client.get = AsyncMock(side_effect=_get)
    with patch.object(mod, "FabricClient", side_effect=fabric_client_factory(client)):
        result = await mod.collect_livy_sessions(_payload())

    assert result["success"] is False
    assert "boom" in result["error"]
    persist.assert_not_awaited()
