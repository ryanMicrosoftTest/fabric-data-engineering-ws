"""Test helpers for activity unit tests."""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any
from unittest.mock import AsyncMock, MagicMock


def make_async_iter(items: Iterable[Any]):
    """Return a function that, when called, returns an async iterator over `items`.

    Suitable for patching `client.get_paged` since it's a sync function returning
    an async iterator (async generator).
    """
    items_list = list(items)

    def _factory(*_args, **_kwargs):
        async def _gen():
            for it in items_list:
                yield it

        return _gen()

    return _factory


def make_fabric_client_mock(
    *,
    paged: dict[str, list[Any]] | None = None,
    paged_default: list[Any] | None = None,
    get_responses: dict[str, Any] | None = None,
    get_default: Any = None,
    get_side_effect: Any = None,
    post_lro_responses: dict[str, Any] | None = None,
    post_lro_default: Any = None,
):
    """Build a mock object that mimics FabricClient as an async context manager."""
    client = MagicMock(name="FabricClient")
    client.__aenter__ = AsyncMock(return_value=client)
    client.__aexit__ = AsyncMock(return_value=None)

    paged = paged or {}
    paged_default = paged_default if paged_default is not None else []

    def _get_paged(path, params=None):
        items = paged.get(path, paged_default)

        async def _gen():
            for it in items:
                yield it

        return _gen()

    client.get_paged = MagicMock(side_effect=_get_paged)

    get_responses = get_responses or {}

    async def _get(path, params=None):
        if get_side_effect is not None:
            res = get_side_effect(path, params)
            if hasattr(res, "__await__"):
                return await res
            return res
        if path in get_responses:
            return get_responses[path]
        return get_default if get_default is not None else {}

    client.get = AsyncMock(side_effect=_get)

    post_lro_responses = post_lro_responses or {}

    async def _post_lro(path, json=None, poll_interval=2.0, timeout=300.0):
        if path in post_lro_responses:
            return post_lro_responses[path]
        return post_lro_default if post_lro_default is not None else {}

    client.post_lro = AsyncMock(side_effect=_post_lro)
    return client


def fabric_client_factory(client_mock):
    """Return a callable that ignores positional args and returns `client_mock`."""

    def _factory(*_args, **_kwargs):
        return client_mock

    return _factory
