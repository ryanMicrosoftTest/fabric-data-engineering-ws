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
    from activities import collect_spark_pools as mod

    with (
        patch.object(mod, "get_settings", return_value=mock_settings),
        patch.object(mod, "get_credential", return_value=object()),
        patch.object(mod, "persist_raw", new=AsyncMock(return_value=3)) as persist,
    ):
        yield mod, persist


@pytest.mark.asyncio
async def test_happy_path_pools_and_settings(patched):
    mod, persist = patched
    pools = [
        {"id": "p1", "name": "Pool1"},
        {"id": "p2", "name": "Pool2"},
    ]
    client = make_fabric_client_mock(
        paged={"/v1/workspaces/ws-A/spark/pools": pools},
        get_responses={"/v1/workspaces/ws-A/spark/settings": {"defaultPoolName": "auto"}},
    )
    with patch.object(mod, "FabricClient", side_effect=fabric_client_factory(client)):
        result = await mod.collect_spark_pools(dict(PAYLOAD))

    assert result["success"] is True
    assert result["rows_written"] == 3
    args, _ = persist.call_args
    assert args[2] == "raw.spark_pool"
    assert args[3] == ["workspace_id", "pool_id"]
    rows = args[4]
    pool_ids = [r["pool_id"] for r in rows]
    assert "p1" in pool_ids and "p2" in pool_ids and "workspace_default" in pool_ids
    # raw_payload for default contains pool_type=workspace_default
    default_row = [r for r in rows if r["pool_id"] == "workspace_default"][0]
    assert "workspace_default" in default_row["raw_payload"]


@pytest.mark.asyncio
async def test_no_custom_pools_still_writes_default(patched):
    mod, persist = patched
    persist.return_value = 1
    client = make_fabric_client_mock(
        paged={"/v1/workspaces/ws-A/spark/pools": []},
        get_responses={"/v1/workspaces/ws-A/spark/settings": {}},
    )
    with patch.object(mod, "FabricClient", side_effect=fabric_client_factory(client)):
        result = await mod.collect_spark_pools(dict(PAYLOAD))

    assert result["success"] is True
    args, _ = persist.call_args
    rows = args[4]
    assert len(rows) == 1
    assert rows[0]["pool_id"] == "workspace_default"


@pytest.mark.asyncio
async def test_fabric_api_error(patched):
    mod, persist = patched

    def _raises(*_a, **_kw):
        async def _gen():
            raise FabricApiError(500, "pools down", "/v1/workspaces/ws-A/spark/pools")
            yield  # pragma: no cover

        return _gen()

    client = make_fabric_client_mock()
    client.get_paged.side_effect = _raises
    with patch.object(mod, "FabricClient", side_effect=fabric_client_factory(client)):
        result = await mod.collect_spark_pools(dict(PAYLOAD))

    assert result["success"] is False
    assert "pools down" in result["error"]
    persist.assert_not_awaited()
