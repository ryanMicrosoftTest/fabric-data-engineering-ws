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
        "notebook_ids": ["n1", "n2"],
    }
    base.update(extra)
    return base


@pytest.fixture
def patched(mock_settings):
    from activities import collect_job_instances as mod

    with patch.object(mod, "get_settings", return_value=mock_settings), patch.object(
        mod, "get_credential", return_value=object()
    ), patch.object(mod, "persist_raw", new=AsyncMock(return_value=3)) as persist:
        yield mod, persist


@pytest.mark.asyncio
async def test_happy_path_extracts_livy_ids(patched):
    mod, persist = patched
    paged = {
        "/v1/workspaces/ws-A/items/n1/jobs/instances": [
            {"id": "j1", "livyId": "livy-1"},
            {"id": "j2", "parameters": {"livyId": "livy-2"}},
        ],
        "/v1/workspaces/ws-A/items/n2/jobs/instances": [
            {"id": "j3"},  # no livy
        ],
    }
    client = make_fabric_client_mock(paged=paged)
    with patch.object(mod, "FabricClient", side_effect=fabric_client_factory(client)):
        result = await mod.collect_job_instances(_payload())

    assert result["success"] is True
    assert result["rows_written"] == 3
    assert sorted(result["livy_session_ids"]) == ["livy-1", "livy-2"]
    args, _ = persist.call_args
    assert args[2] == "raw.job_instance"
    assert args[3] == ["workspace_id", "item_id", "job_instance_id"]
    rows = args[4]
    assert {r["job_instance_id"] for r in rows} == {"j1", "j2", "j3"}
    # item_id matches the notebook
    assert all(r["item_id"] in ("n1", "n2") for r in rows)


@pytest.mark.asyncio
async def test_filter_applied_when_watermark_provided(patched):
    mod, _persist = patched
    captured: list[dict] = []

    def _paged(path, params=None):
        captured.append({"path": path, "params": params})

        async def _gen():
            for it in []:
                yield it

        return _gen()

    client = make_fabric_client_mock()
    client.get_paged.side_effect = _paged
    with patch.object(mod, "FabricClient", side_effect=fabric_client_factory(client)):
        await mod.collect_job_instances(_payload(from_watermark="2024-01-01T00:00:00+00:00"))

    assert all(c["params"] == {"$filter": "startTimeUtc ge 2024-01-01T00:00:00+00:00"} for c in captured)


@pytest.mark.asyncio
async def test_no_notebook_ids(patched):
    mod, persist = patched
    persist.return_value = 0
    client = make_fabric_client_mock()
    with patch.object(mod, "FabricClient", side_effect=fabric_client_factory(client)):
        result = await mod.collect_job_instances(_payload(notebook_ids=[]))
    assert result["success"] is True
    assert result["rows_written"] == 0
    assert result["livy_session_ids"] == []


@pytest.mark.asyncio
async def test_fabric_api_error(patched):
    mod, persist = patched

    def _raises(*_a, **_kw):
        async def _gen():
            raise FabricApiError(500, "jobs down", "/v1/x")
            yield  # pragma: no cover

        return _gen()

    client = make_fabric_client_mock()
    client.get_paged.side_effect = _raises
    with patch.object(mod, "FabricClient", side_effect=fabric_client_factory(client)):
        result = await mod.collect_job_instances(_payload())

    assert result["success"] is False
    assert "jobs down" in result["error"]
    assert result["livy_session_ids"] == []
    persist.assert_not_awaited()
