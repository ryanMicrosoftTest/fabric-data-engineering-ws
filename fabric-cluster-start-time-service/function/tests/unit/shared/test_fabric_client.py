import time
from datetime import datetime
from unittest.mock import MagicMock

import httpx
import pytest

from shared.config import Settings
from shared.fabric_client import FabricApiError, FabricClient


class _FakeTokenResult:
    def __init__(self, token: str, expires_on: float):
        self.token = token
        self.expires_on = expires_on


class _FakeCredential:
    def __init__(self, token: str = "fake-token"):
        self._token = token
        self.calls = 0

    def get_token(self, scope: str):
        self.calls += 1
        return _FakeTokenResult(self._token, time.time() + 3600)


def _settings() -> Settings:
    return Settings(
        fabric_api_base_url="https://api.fabric.microsoft.com",
        warehouse_sql_endpoint="x",
        warehouse_database="y",
        target_workspace_ids="ws-1",
    )


def _mock_transport(handler):
    return httpx.MockTransport(handler)


@pytest.mark.asyncio
async def test_get_sends_auth_header():
    seen = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["auth"] = request.headers.get("Authorization")
        seen["url"] = str(request.url)
        return httpx.Response(200, json={"value": [{"id": 1}]})

    fc = FabricClient(_settings(), _FakeCredential("tok-abc"))
    async with fc:
        fc._client = httpx.AsyncClient(  # override with mock transport
            base_url=fc._settings.fabric_api_base_url,
            transport=_mock_transport(handler),
        )
        data = await fc.get("/v1/workspaces")
    assert seen["auth"] == "Bearer tok-abc"
    assert data == {"value": [{"id": 1}]}


@pytest.mark.asyncio
async def test_get_raises_on_4xx():
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(404, text="not found")

    fc = FabricClient(_settings(), _FakeCredential())
    async with fc:
        fc._client = httpx.AsyncClient(
            base_url=fc._settings.fabric_api_base_url,
            transport=_mock_transport(handler),
        )
        with pytest.raises(FabricApiError) as ei:
            await fc.get("/v1/missing")
    assert ei.value.status_code == 404


@pytest.mark.asyncio
async def test_get_paged_follows_continuation_token():
    pages = [
        {"value": [{"id": 1}, {"id": 2}], "continuationToken": "ct1"},
        {"value": [{"id": 3}]},
    ]
    counter = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        idx = counter["n"]
        counter["n"] += 1
        if idx == 1:
            assert request.url.params.get("continuationToken") == "ct1"
        return httpx.Response(200, json=pages[idx])

    fc = FabricClient(_settings(), _FakeCredential())
    async with fc:
        fc._client = httpx.AsyncClient(
            base_url=fc._settings.fabric_api_base_url,
            transport=_mock_transport(handler),
        )
        items = []
        async for item in fc.get_paged("/v1/items"):
            items.append(item)
    assert [i["id"] for i in items] == [1, 2, 3]


@pytest.mark.asyncio
async def test_post_lro_polls_until_200():
    state = {"polls": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "POST":
            return httpx.Response(202, headers={"Location": "https://api.fabric.microsoft.com/v1/op/abc"})
        state["polls"] += 1
        if state["polls"] < 2:
            return httpx.Response(202)
        return httpx.Response(200, json={"status": "Succeeded"})

    fc = FabricClient(_settings(), _FakeCredential())
    async with fc:
        fc._client = httpx.AsyncClient(
            base_url=fc._settings.fabric_api_base_url,
            transport=_mock_transport(handler),
        )
        result = await fc.post_lro("/v1/start", json={}, poll_interval=0.0, timeout=5.0)
    assert result == {"status": "Succeeded"}
    assert state["polls"] == 2


@pytest.mark.asyncio
async def test_token_cached_across_calls():
    cred = _FakeCredential()

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"value": []})

    fc = FabricClient(_settings(), cred)
    async with fc:
        fc._client = httpx.AsyncClient(
            base_url=fc._settings.fabric_api_base_url,
            transport=_mock_transport(handler),
        )
        await fc.get("/v1/a")
        await fc.get("/v1/b")
    assert cred.calls == 1
