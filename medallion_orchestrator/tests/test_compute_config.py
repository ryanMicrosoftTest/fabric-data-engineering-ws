from __future__ import annotations

from itertools import product
from types import SimpleNamespace

import pytest
from airflow.exceptions import AirflowException

from medallion_fabric.compute_config import (
    ALLOWED_CORES,
    ALLOWED_MEMORY,
    FABRIC_SQL_SCOPE,
    ComputeConfiguration,
    FabricSqlConfigHook,
)


class Cursor:
    def __init__(self, row):
        self.row = row
        self.executed = None
        self.closed = False
        self.timeout = None

    def execute(self, query, parameter):
        self.executed = (query, parameter)

    def fetchone(self):
        return self.row

    def close(self):
        self.closed = True


class Connection:
    def __init__(self, row):
        self._cursor = Cursor(row)
        self.closed = False

    def cursor(self):
        return self._cursor

    def close(self):
        self.closed = True


@pytest.mark.parametrize("memory,cores", list(product(ALLOWED_MEMORY, ALLOWED_CORES)))
def test_every_allowed_memory_and_core_value(memory, cores):
    config = ComputeConfiguration(memory, cores, memory, cores, 1)
    assert config.driver_memory == memory


@pytest.mark.parametrize("memory", ["", "16g", "28G", "512g", None])
def test_rejects_invalid_memory(memory):
    with pytest.raises(AirflowException):
        ComputeConfiguration(memory, 4, "28g", 4, 1)


@pytest.mark.parametrize("cores", [0, 1, 2, 12, 128, None, "4"])
def test_rejects_invalid_cores(cores):
    with pytest.raises(AirflowException):
        ComputeConfiguration("28g", cores, "28g", 4, 1)


@pytest.mark.parametrize("count", [0, -1, None, 1.5, True])
def test_rejects_invalid_executor_count(count):
    with pytest.raises(AirflowException):
        ComputeConfiguration("28g", 4, "28g", 4, count)


@pytest.mark.parametrize(
    "row,warning",
    [(None, "No compute configuration"), ((False, None, None, None, None, None), "disabled")],
)
def test_missing_or_disabled_rows_use_defaults(monkeypatch, caplog, row, warning):
    connection = Connection(row)
    hook = FabricSqlConfigHook()
    monkeypatch.setattr(hook, "get_conn", lambda: connection)
    assert hook.get_compute_configuration("notebook-uuid") is None
    assert warning in caplog.text
    assert connection.closed


def test_enabled_row_is_parameterized_and_returned(monkeypatch):
    connection = Connection((True, "56g", 8, "56g", 8, 2))
    hook = FabricSqlConfigHook(command_timeout=12)
    monkeypatch.setattr(hook, "get_conn", lambda: connection)
    config = hook.get_compute_configuration("abc")
    assert config.to_api_dict()["numExecutors"] == 2
    query, parameter = connection._cursor.executed
    assert "?" in query
    assert parameter == "abc"
    assert connection._cursor.timeout == 12
    assert connection._cursor.closed


def test_incomplete_enabled_row_fails(monkeypatch):
    hook = FabricSqlConfigHook()
    monkeypatch.setattr(
        hook, "get_conn", lambda: Connection((True, "56g", 8, None, 8, 2))
    )
    with pytest.raises(AirflowException, match="incomplete"):
        hook.get_compute_configuration("abc")


def test_sql_token_audience_is_separate(monkeypatch):
    captured = {}
    connection = SimpleNamespace(
        host="server.database.fabric.microsoft.com",
        schema="airflow_config",
        login="client",
        password="secret",
        extra_dejson={"tenantId": "tenant"},
    )
    hook = FabricSqlConfigHook()
    monkeypatch.setattr(hook, "get_connection", lambda _: connection)

    def acquire(tenant, client, secret, scope):
        captured["scope"] = scope
        return "token"

    monkeypatch.setattr(hook, "_acquire_token", acquire)
    monkeypatch.setattr(
        "medallion_fabric.compute_config.pyodbc.connect",
        lambda *args, **kwargs: (args, kwargs),
    )
    hook.get_conn()
    assert captured["scope"] == FABRIC_SQL_SCOPE
