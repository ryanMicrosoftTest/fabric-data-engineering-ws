import httpx
import pytest
import tenacity

from shared.retry import fabric_retry, warehouse_retry


def test_fabric_retry_retries_on_429_then_succeeds():
    calls = {"n": 0}

    @fabric_retry()
    def call() -> httpx.Response:
        calls["n"] += 1
        if calls["n"] < 3:
            return httpx.Response(429)
        return httpx.Response(200)

    resp = call()
    assert resp.status_code == 200
    assert calls["n"] == 3


def test_fabric_retry_gives_up_after_5():
    calls = {"n": 0}

    @fabric_retry()
    def call() -> httpx.Response:
        calls["n"] += 1
        return httpx.Response(503)

    with pytest.raises(tenacity.RetryError):
        call()
    assert calls["n"] == 5


def test_fabric_retry_does_not_retry_on_400():
    calls = {"n": 0}

    @fabric_retry()
    def call() -> httpx.Response:
        calls["n"] += 1
        return httpx.Response(400)

    resp = call()
    assert resp.status_code == 400
    assert calls["n"] == 1


def test_fabric_retry_retries_on_network_error():
    calls = {"n": 0}

    @fabric_retry()
    def call() -> httpx.Response:
        calls["n"] += 1
        if calls["n"] < 2:
            raise httpx.ConnectError("boom")
        return httpx.Response(200)

    resp = call()
    assert resp.status_code == 200
    assert calls["n"] == 2


def test_warehouse_retry_does_not_retry_on_429():
    calls = {"n": 0}

    @warehouse_retry()
    def call() -> httpx.Response:
        calls["n"] += 1
        return httpx.Response(429)

    resp = call()
    assert resp.status_code == 429
    assert calls["n"] == 1


def test_warehouse_retry_caps_at_3():
    calls = {"n": 0}

    @warehouse_retry()
    def call() -> httpx.Response:
        calls["n"] += 1
        return httpx.Response(503)

    with pytest.raises(tenacity.RetryError):
        call()
    assert calls["n"] == 3
