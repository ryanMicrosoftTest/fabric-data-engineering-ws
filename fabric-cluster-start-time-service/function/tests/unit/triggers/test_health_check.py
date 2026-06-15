from __future__ import annotations

import json
from unittest.mock import MagicMock

import azure.functions as func

from triggers import health_check as health_check_mod
from triggers.health_check import health_check


def _req(deep: str | None = None):
    r = MagicMock(spec=func.HttpRequest)
    r.params = {"deep": deep} if deep is not None else {}
    return r


def _patch_warehouse(monkeypatch, ping_value=None, raise_exc=None):
    monkeypatch.setattr(health_check_mod, "DefaultAzureCredential", MagicMock())
    wh_instance = MagicMock()
    if raise_exc is not None:
        wh_instance.__enter__ = MagicMock(side_effect=raise_exc)
    else:
        wh_instance.__enter__ = MagicMock(return_value=wh_instance)
        wh_instance.ping = MagicMock(return_value=ping_value)
    wh_instance.__exit__ = MagicMock(return_value=None)
    wh_cls = MagicMock(return_value=wh_instance)
    monkeypatch.setattr(health_check_mod, "WarehouseWriter", wh_cls)
    return wh_cls, wh_instance


def test_health_check_shallow(monkeypatch):
    wh_cls, _ = _patch_warehouse(monkeypatch, ping_value=True)
    resp = health_check(_req())
    assert resp.status_code == 200
    body = json.loads(resp.get_body())
    assert body == {"status": "ok", "version": "0.1.0"}
    wh_cls.assert_not_called()


def test_health_check_deep_ok(monkeypatch):
    _patch_warehouse(monkeypatch, ping_value=True)
    resp = health_check(_req(deep="true"))
    assert resp.status_code == 200
    body = json.loads(resp.get_body())
    assert body["status"] == "ok"
    assert body["warehouse"] == "ok"


def test_health_check_deep_ping_false(monkeypatch):
    _patch_warehouse(monkeypatch, ping_value=False)
    resp = health_check(_req(deep="true"))
    assert resp.status_code == 200
    body = json.loads(resp.get_body())
    assert body["warehouse"] == "fail"


def test_health_check_deep_raises(monkeypatch):
    _patch_warehouse(monkeypatch, raise_exc=RuntimeError("boom"))
    resp = health_check(_req(deep="true"))
    assert resp.status_code == 503
    body = json.loads(resp.get_body())
    assert body["warehouse"] == "error: RuntimeError"
