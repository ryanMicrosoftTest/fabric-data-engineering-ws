"""Shared pytest fixtures.

NOTE: We patch sys.modules for pyodbc BEFORE any import of warehouse_writer so
unit tests can run on machines without ODBC drivers installed.
"""
from __future__ import annotations

import sys
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest

# Ensure pyodbc is importable in environments without ODBC drivers.
sys.modules.setdefault("pyodbc", MagicMock())


# Stub azure.functions / azure.durable_functions BEFORE function_app is imported
# so decorators are pass-throughs and decorated symbols stay directly callable
# from tests. Applied globally so test ordering doesn't matter.
class _FakeApp:
    def function_name(self, *_a, **_kw):
        def deco(fn):
            return fn
        return deco

    def __getattr__(self, _name):
        def deco_factory(*_args, **_kwargs):
            def deco(fn):
                return fn
            return deco
        return deco_factory


if "azure.functions" not in sys.modules:
    _af = MagicMock()
    _af.AuthLevel = MagicMock(FUNCTION="function", ANONYMOUS="anonymous", ADMIN="admin")

    class _HttpRequest:  # plain class so MagicMock(spec=...) works
        pass

    class _HttpResponse:
        def __init__(self, body=None, status_code=200, mimetype=None, headers=None):
            self.body = body
            self.status_code = status_code
            self.mimetype = mimetype
            self.headers = headers or {}

        def get_body(self):
            return self.body if isinstance(self.body, (bytes, bytearray)) else (self.body or "").encode()

    _af.HttpRequest = _HttpRequest
    _af.HttpResponse = _HttpResponse
    sys.modules["azure.functions"] = _af

if "azure.durable_functions" not in sys.modules:
    _df = MagicMock()
    _df.DFApp = lambda *a, **kw: _FakeApp()
    sys.modules["azure.durable_functions"] = _df

# Make `function/` (the parent of the tests dir) importable as the project root.
_FUNCTION_DIR = Path(__file__).resolve().parent.parent
if str(_FUNCTION_DIR) not in sys.path:
    sys.path.insert(0, str(_FUNCTION_DIR))


@pytest.fixture
def mock_settings():
    from shared.config import Settings

    return Settings(
        fabric_tenant_id="00000000-0000-0000-0000-000000000001",
        fabric_api_base_url="https://api.fabric.microsoft.com",
        warehouse_sql_endpoint="example.datawarehouse.fabric.microsoft.com",
        warehouse_database="fabric_telemetry_wh",
        mi_client_id=None,
        target_workspace_ids="ws-1,ws-2",
        applicationinsights_connection_string=None,
    )


@pytest.fixture
def mock_fabric_client():
    client = AsyncMock()
    client.get = AsyncMock()
    client.get_paged = AsyncMock()
    client.post_lro = AsyncMock()
    client.__aenter__ = AsyncMock(return_value=client)
    client.__aexit__ = AsyncMock(return_value=None)
    return client


@pytest.fixture
def mock_warehouse_writer():
    writer = MagicMock()
    writer.upsert_raw = MagicMock(return_value=0)
    writer.execute_proc = MagicMock(return_value=0)
    writer.ping = MagicMock(return_value=True)
    writer.__enter__ = MagicMock(return_value=writer)
    writer.__exit__ = MagicMock(return_value=None)
    return writer


@pytest.fixture
def mock_durable_context():
    ctx = MagicMock()
    ctx.call_activity = MagicMock()
    ctx.call_sub_orchestrator = MagicMock()
    ctx.task_all = MagicMock(side_effect=lambda tasks: list(tasks))
    ctx.current_utc_datetime = datetime(2024, 1, 15, 10, 0, 0, tzinfo=timezone.utc)
    return ctx


@pytest.fixture
def frozen_now(monkeypatch):
    fixed = datetime(2024, 1, 15, 10, 0, 0, tzinfo=timezone.utc)

    class _FixedDT(datetime):
        @classmethod
        def utcnow(cls):
            return fixed.replace(tzinfo=None)

        @classmethod
        def now(cls, tz=None):
            return fixed if tz else fixed.replace(tzinfo=None)

    return fixed
