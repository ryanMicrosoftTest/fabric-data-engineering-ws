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
    from activities import collect_notebook_items as mod

    with (
        patch.object(mod, "get_settings", return_value=mock_settings),
        patch.object(mod, "get_credential", return_value=object()),
        patch.object(mod, "persist_raw", new=AsyncMock(return_value=2)) as persist,
    ):
        yield mod, persist


@pytest.mark.asyncio
async def test_happy_path_collects_ids(patched):
    mod, persist = patched
    items = [
        {"id": "n1", "displayName": "N1"},
        {"id": "n2", "displayName": "N2"},
    ]
    client = make_fabric_client_mock(paged={"/v1/workspaces/ws-A/notebooks": items})
    with patch.object(mod, "FabricClient", side_effect=fabric_client_factory(client)):
        result = await mod.collect_notebook_items(dict(PAYLOAD))

    assert result["success"] is True
    assert result["rows_written"] == 2
    assert result["notebook_ids"] == ["n1", "n2"]
    args, _ = persist.call_args
    assert args[2] == "raw.notebook_item"
    assert args[3] == ["workspace_id", "notebook_id"]
    rows = args[4]
    assert {r["notebook_id"] for r in rows} == {"n1", "n2"}
    assert all(r["workspace_id"] == "ws-A" for r in rows)


@pytest.mark.asyncio
async def test_empty_response(patched):
    mod, persist = patched
    persist.return_value = 0
    client = make_fabric_client_mock(paged={"/v1/workspaces/ws-A/notebooks": []})
    with patch.object(mod, "FabricClient", side_effect=fabric_client_factory(client)):
        result = await mod.collect_notebook_items(dict(PAYLOAD))

    assert result["success"] is True
    assert result["rows_written"] == 0
    assert result["notebook_ids"] == []


@pytest.mark.asyncio
async def test_fabric_api_error(patched):
    mod, persist = patched

    def _raises(*_a, **_kw):
        async def _gen():
            raise FabricApiError(500, "down", "/v1/workspaces/ws-A/notebooks")
            yield  # pragma: no cover

        return _gen()

    client = make_fabric_client_mock()
    client.get_paged.side_effect = _raises
    with patch.object(mod, "FabricClient", side_effect=fabric_client_factory(client)):
        result = await mod.collect_notebook_items(dict(PAYLOAD))

    assert result["success"] is False
    assert "down" in result["error"]
    assert result["notebook_ids"] == []
    persist.assert_not_awaited()
