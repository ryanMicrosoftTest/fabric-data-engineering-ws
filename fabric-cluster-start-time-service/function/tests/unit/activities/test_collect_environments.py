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
    from activities import collect_environments as mod

    with (
        patch.object(mod, "get_settings", return_value=mock_settings),
        patch.object(mod, "get_credential", return_value=object()),
        patch.object(mod, "persist_raw", new=AsyncMock(return_value=2)) as persist,
    ):
        yield mod, persist


@pytest.mark.asyncio
async def test_happy_path_persists_envs_and_libs(patched):
    mod, persist = patched
    envs = [{"id": "e1", "displayName": "E1"}, {"id": "e2", "displayName": "E2"}]
    libs = {
        "/v1/workspaces/ws-A/environments/e1/staging/libraries": {"customLibraries": []},
        "/v1/workspaces/ws-A/environments/e2/staging/libraries": {"customLibraries": [{"name": "x"}]},
    }
    client = make_fabric_client_mock(
        paged={"/v1/workspaces/ws-A/environments": envs},
        get_responses=libs,
    )
    with patch.object(mod, "FabricClient", side_effect=fabric_client_factory(client)):
        result = await mod.collect_environments(dict(PAYLOAD))

    assert result["success"] is True
    assert result["rows_written"] == 4  # 2 from each persist call
    # persist called twice — for envs and libs
    assert persist.await_count == 2
    tables = [c.args[2] for c in persist.await_args_list]
    assert "raw.environment" in tables
    assert "raw.environment_library" in tables


@pytest.mark.asyncio
async def test_lib_404_treated_as_empty(patched):
    mod, persist = patched
    envs = [{"id": "e1"}]

    async def _get(path, params=None):
        raise FabricApiError(404, "missing", path)

    client = make_fabric_client_mock(paged={"/v1/workspaces/ws-A/environments": envs})
    client.get = AsyncMock(side_effect=_get)
    with patch.object(mod, "FabricClient", side_effect=fabric_client_factory(client)):
        result = await mod.collect_environments(dict(PAYLOAD))

    assert result["success"] is True
    # both persist calls happened (env list + lib list with empty payload row)
    assert persist.await_count == 2
    lib_call = [c for c in persist.await_args_list if c.args[2] == "raw.environment_library"][0]
    rows = lib_call.args[4]
    assert len(rows) == 1
    assert rows[0]["environment_id"] == "e1"


@pytest.mark.asyncio
async def test_empty_environments(patched):
    mod, persist = patched
    persist.return_value = 0
    client = make_fabric_client_mock(paged={"/v1/workspaces/ws-A/environments": []})
    with patch.object(mod, "FabricClient", side_effect=fabric_client_factory(client)):
        result = await mod.collect_environments(dict(PAYLOAD))

    assert result["success"] is True
    assert result["rows_written"] == 0


@pytest.mark.asyncio
async def test_fabric_api_error_envs(patched):
    mod, persist = patched

    def _raises(*_a, **_kw):
        async def _gen():
            raise FabricApiError(500, "envs down", "/v1/workspaces/ws-A/environments")
            yield  # pragma: no cover

        return _gen()

    client = make_fabric_client_mock()
    client.get_paged.side_effect = _raises
    with patch.object(mod, "FabricClient", side_effect=fabric_client_factory(client)):
        result = await mod.collect_environments(dict(PAYLOAD))

    assert result["success"] is False
    assert "envs down" in result["error"]
    persist.assert_not_awaited()
