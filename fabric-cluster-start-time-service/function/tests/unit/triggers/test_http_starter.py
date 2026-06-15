from __future__ import annotations

import json
from unittest.mock import AsyncMock, MagicMock

import azure.durable_functions as df
import azure.functions as func
import pytest

from triggers import http_starter as http_starter_mod
from triggers.http_starter import http_starter


def _req(body):
    r = MagicMock(spec=func.HttpRequest)
    if isinstance(body, ValueError):
        r.get_json = MagicMock(side_effect=body)
    else:
        r.get_json = MagicMock(return_value=body)
    return r


def _status(runtime_status):
    s = MagicMock()
    s.runtime_status = runtime_status
    return s


def _client(get_status_value=None):
    client = MagicMock()
    client.get_status = AsyncMock(return_value=get_status_value)
    client.start_new = AsyncMock(return_value="instance")
    client.create_check_status_response = MagicMock(
        return_value=func.HttpResponse('{"id":"x"}', status_code=202, mimetype="application/json")
    )
    return client


@pytest.mark.asyncio
async def test_http_starter_happy_path(monkeypatch):
    monkeypatch.setattr(http_starter_mod.secrets, "token_hex", lambda n: "abcdef")
    client = _client(get_status_value=None)
    body = {"from_date": "2024-01-01", "to_date": "2024-01-05"}
    resp = await http_starter(_req(body), client)

    assert resp.status_code == 202
    client.start_new.assert_awaited_once()
    args, _ = client.start_new.call_args
    assert args[0] == "collector_orchestrator"
    instance_id = args[1]
    assert instance_id == "backfill-2024-01-01-2024-01-05-abcdef"
    payload = args[2]
    assert payload["trigger"] == "http"
    assert payload["collector_run_id"] == instance_id
    assert payload["from_date"] == "2024-01-01"
    assert payload["to_date"] == "2024-01-05"
    client.create_check_status_response.assert_called_once()


@pytest.mark.asyncio
async def test_http_starter_invalid_json():
    client = _client()
    resp = await http_starter(_req(ValueError("not json")), client)
    assert resp.status_code == 400
    assert json.loads(resp.get_body()) == {"error": "body must be JSON"}
    client.start_new.assert_not_called()


@pytest.mark.asyncio
async def test_http_starter_validation_error():
    client = _client()
    body = {"from_date": "2024-01-10", "to_date": "2024-01-01"}
    resp = await http_starter(_req(body), client)
    assert resp.status_code == 400
    parsed = json.loads(resp.get_body())
    assert isinstance(parsed, list)
    assert any("from_date" in str(item).lower() or "to_date" in str(item).lower() for item in parsed)
    client.start_new.assert_not_called()


@pytest.mark.asyncio
async def test_http_starter_duplicate_instance(monkeypatch):
    monkeypatch.setattr(http_starter_mod.secrets, "token_hex", lambda n: "abcdef")
    client = _client(get_status_value=_status(df.OrchestrationRuntimeStatus.Running))
    body = {"from_date": "2024-01-01", "to_date": "2024-01-05"}
    resp = await http_starter(_req(body), client)
    assert resp.status_code == 409
    parsed = json.loads(resp.get_body())
    assert "already running" in parsed["error"]
    client.start_new.assert_not_called()
