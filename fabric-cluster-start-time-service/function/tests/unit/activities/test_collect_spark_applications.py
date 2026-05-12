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
    }
    base.update(extra)
    return base


@pytest.fixture
def patched(mock_settings):
    from activities import collect_spark_applications as mod

    with (
        patch.object(mod, "get_settings", return_value=mock_settings),
        patch.object(mod, "get_credential", return_value=object()),
        patch.object(mod, "persist_raw", new=AsyncMock(return_value=2)) as persist,
    ):
        yield mod, persist


@pytest.mark.asyncio
async def test_happy_path(patched):
    mod, persist = patched
    apps = [{"id": "a1"}, {"id": "a2"}]
    client = make_fabric_client_mock(paged={"/v1/workspaces/ws-A/spark/applications": apps})
    with patch.object(mod, "FabricClient", side_effect=fabric_client_factory(client)):
        result = await mod.collect_spark_applications(_payload())

    assert result["success"] is True
    assert result["rows_written"] == 2
    args, _ = persist.call_args
    assert args[2] == "raw.spark_application"
    assert args[3] == ["workspace_id", "application_id"]
    rows = args[4]
    assert {r["application_id"] for r in rows} == {"a1", "a2"}


@pytest.mark.asyncio
async def test_filter_applied_when_watermark_provided(patched):
    mod, _persist = patched
    captured: dict = {}

    def _paged(path, params=None):
        captured["path"] = path
        captured["params"] = params

        async def _gen():
            for it in []:
                yield it

        return _gen()

    client = make_fabric_client_mock()
    client.get_paged.side_effect = _paged
    with patch.object(mod, "FabricClient", side_effect=fabric_client_factory(client)):
        await mod.collect_spark_applications(_payload(from_watermark="2024-01-01T00:00:00+00:00"))

    assert captured["params"] == {"$filter": "submittedDateTime ge 2024-01-01T00:00:00+00:00"}


@pytest.mark.asyncio
async def test_empty_response(patched):
    mod, persist = patched
    persist.return_value = 0
    client = make_fabric_client_mock(paged={"/v1/workspaces/ws-A/spark/applications": []})
    with patch.object(mod, "FabricClient", side_effect=fabric_client_factory(client)):
        result = await mod.collect_spark_applications(_payload())
    assert result["success"] is True
    assert result["rows_written"] == 0


@pytest.mark.asyncio
async def test_fabric_api_error(patched):
    mod, persist = patched

    def _raises(*_a, **_kw):
        async def _gen():
            raise FabricApiError(503, "throttled", "/v1/workspaces/ws-A/spark/applications")
            yield  # pragma: no cover

        return _gen()

    client = make_fabric_client_mock()
    client.get_paged.side_effect = _raises
    with patch.object(mod, "FabricClient", side_effect=fabric_client_factory(client)):
        result = await mod.collect_spark_applications(_payload())

    assert result["success"] is False
    assert "throttled" in result["error"]
    persist.assert_not_awaited()
