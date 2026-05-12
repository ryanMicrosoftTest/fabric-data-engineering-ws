import asyncio
import time
from typing import Any, AsyncIterator

import httpx

from .retry import fabric_retry

_TOKEN_SCOPE = "https://api.fabric.microsoft.com/.default"
_TOKEN_REFRESH_BUFFER_SECONDS = 300


class FabricApiError(Exception):
    def __init__(self, status_code: int, body: str, request_url: str):
        super().__init__(f"Fabric API error {status_code} for {request_url}: {body[:500]}")
        self.status_code = status_code
        self.body = body
        self.request_url = request_url


class FabricClient:
    def __init__(self, settings, credential):
        self._settings = settings
        self._credential = credential
        self._client: httpx.AsyncClient | None = None
        self._token: str | None = None
        self._token_expires_at: float = 0.0

    async def __aenter__(self) -> "FabricClient":
        self._client = httpx.AsyncClient(
            base_url=self._settings.fabric_api_base_url,
            timeout=httpx.Timeout(60.0, connect=10.0),
        )
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        if self._client is not None:
            await self._client.aclose()
            self._client = None

    async def _get_token(self) -> str:
        now = time.time()
        if self._token and now < (self._token_expires_at - _TOKEN_REFRESH_BUFFER_SECONDS):
            return self._token
        get_token = getattr(self._credential, "get_token", None)
        if get_token is None:
            raise RuntimeError("credential does not implement get_token")
        result = get_token(_TOKEN_SCOPE)
        if asyncio.iscoroutine(result):
            result = await result
        self._token = result.token
        self._token_expires_at = float(getattr(result, "expires_on", now + 3600))
        return self._token

    async def _auth_headers(self) -> dict[str, str]:
        token = await self._get_token()
        return {"Authorization": f"Bearer {token}", "Accept": "application/json"}

    def _ensure_client(self) -> httpx.AsyncClient:
        if self._client is None:
            raise RuntimeError("FabricClient must be used as an async context manager")
        return self._client

    @fabric_retry()
    async def _request(self, method: str, url: str, **kwargs) -> httpx.Response:
        client = self._ensure_client()
        headers = kwargs.pop("headers", {}) or {}
        headers.update(await self._auth_headers())
        return await client.request(method, url, headers=headers, **kwargs)

    async def get(self, path: str, params: dict | None = None) -> dict:
        resp = await self._request("GET", path, params=params)
        if resp.status_code >= 400:
            raise FabricApiError(resp.status_code, resp.text, str(resp.request.url))
        return resp.json()

    async def get_paged(self, path: str, params: dict | None = None) -> AsyncIterator[dict]:
        next_url: str | None = path
        next_params = params
        while next_url is not None:
            resp = await self._request("GET", next_url, params=next_params)
            if resp.status_code >= 400:
                raise FabricApiError(resp.status_code, resp.text, str(resp.request.url))
            payload = resp.json()
            for item in payload.get("value", []) or []:
                yield item
            cont = payload.get("continuationToken")
            nxt = payload.get("nextLink") or payload.get("continuationUri")
            if nxt:
                next_url = nxt
                next_params = None
            elif cont:
                next_url = path
                merged = dict(params or {})
                merged["continuationToken"] = cont
                next_params = merged
            else:
                next_url = None

    async def post_lro(
        self,
        path: str,
        json: dict | None = None,
        poll_interval: float = 2.0,
        timeout: float = 300.0,
    ) -> dict:
        resp = await self._request("POST", path, json=json)
        if resp.status_code in (200, 201):
            return resp.json() if resp.content else {}
        if resp.status_code != 202:
            raise FabricApiError(resp.status_code, resp.text, str(resp.request.url))
        location = resp.headers.get("Location") or resp.headers.get("location")
        if not location:
            raise FabricApiError(resp.status_code, "202 without Location header", str(resp.request.url))
        deadline = time.time() + timeout
        while time.time() < deadline:
            await asyncio.sleep(poll_interval)
            poll = await self._request("GET", location)
            if poll.status_code == 200:
                return poll.json() if poll.content else {}
            if poll.status_code in (202,):
                continue
            if poll.status_code >= 400:
                raise FabricApiError(poll.status_code, poll.text, str(poll.request.url))
        raise FabricApiError(408, "LRO polling timeout", location)
