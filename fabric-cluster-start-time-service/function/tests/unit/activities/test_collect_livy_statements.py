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
        "livy_session_ids": ["livy-1", "livy-2"],
    }
    base.update(extra)
    return base


@pytest.fixture
def patched(mock_settings):
    from activities import collect_livy_statements as mod

    with (
        patch.object(mod, "get_settings", return_value=mock_settings),
        patch.object(mod, "get_credential", return_value=object()),
        patch.object(mod, "persist_raw", new=AsyncMock(return_value=3)) as persist,
    ):
        yield mod, persist


@pytest.mark.asyncio
async def test_happy_path_one_row_per_statement(patched):
    mod, persist = patched
    responses = {
        "/v1/workspaces/ws-A/spark/livySessions/livy-1/statements": {
            "statements": [{"id": 0, "code": "1"}, {"id": 1, "code": "2"}]
        },
        "/v1/workspaces/ws-A/spark/livySessions/livy-2/statements": {"statements": [{"id": 0, "code": "x"}]},
    }
    client = make_fabric_client_mock(get_responses=responses)
    with patch.object(mod, "FabricClient", side_effect=fabric_client_factory(client)):
        result = await mod.collect_livy_statements(_payload())

    assert result["success"] is True
    assert result["rows_written"] == 3
    args, _ = persist.call_args
    assert args[2] == "raw.livy_statement"
    assert args[3] == ["workspace_id", "livy_session_id", "statement_id"]
    rows = args[4]
    assert len(rows) == 3
    keys = {(r["livy_session_id"], r["statement_id"]) for r in rows}
    assert keys == {("livy-1", "0"), ("livy-1", "1"), ("livy-2", "0")}


@pytest.mark.asyncio
async def test_404_skipped(patched):
    mod, persist = patched

    async def _get(path, params=None):
        if "livy-2" in path:
            raise FabricApiError(404, "gone", path)
        return {"statements": [{"id": 0}]}

    client = make_fabric_client_mock()
    client.get = AsyncMock(side_effect=_get)
    with patch.object(mod, "FabricClient", side_effect=fabric_client_factory(client)):
        await mod.collect_livy_statements(_payload())

    args, _ = persist.call_args
    rows = args[4]
    assert {r["livy_session_id"] for r in rows} == {"livy-1"}


@pytest.mark.asyncio
async def test_no_ids(patched):
    mod, persist = patched
    persist.return_value = 0
    client = make_fabric_client_mock()
    with patch.object(mod, "FabricClient", side_effect=fabric_client_factory(client)):
        result = await mod.collect_livy_statements(_payload(livy_session_ids=[]))
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
        result = await mod.collect_livy_statements(_payload())

    assert result["success"] is False
    assert "boom" in result["error"]
    persist.assert_not_awaited()
