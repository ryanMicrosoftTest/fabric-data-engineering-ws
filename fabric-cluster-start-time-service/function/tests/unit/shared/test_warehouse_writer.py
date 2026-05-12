import sys
from unittest.mock import MagicMock

import pytest

# pyodbc is patched in conftest; grab the mock
pyodbc_mock = sys.modules["pyodbc"]


class _FakeTokenResult:
    def __init__(self, token: str):
        self.token = token


class _FakeCredential:
    def get_token(self, scope: str):
        return _FakeTokenResult("warehouse-token")


def _make_settings():
    from shared.config import Settings

    return Settings(
        warehouse_sql_endpoint="example.datawarehouse.fabric.microsoft.com",
        warehouse_database="fabric_telemetry_wh",
        target_workspace_ids="ws-1",
    )


def test_connection_string_built_correctly():
    from shared.warehouse_writer import WarehouseWriter

    w = WarehouseWriter(_make_settings(), _FakeCredential())
    cs = w._build_connection_string()
    assert "Driver={ODBC Driver 18 for SQL Server}" in cs
    assert "Server=tcp:example.datawarehouse.fabric.microsoft.com,1433" in cs
    assert "Database=fabric_telemetry_wh" in cs
    assert "Encrypt=yes" in cs


def test_identifier_validation_rejects_injection():
    from shared.warehouse_writer import _validate_identifier

    _validate_identifier("raw.workspace")
    _validate_identifier("good_table")
    with pytest.raises(ValueError):
        _validate_identifier("bad; DROP TABLE users")
    with pytest.raises(ValueError):
        _validate_identifier("bad-name")
    with pytest.raises(ValueError):
        _validate_identifier("1numfirst")


def test_token_encoding():
    from shared.warehouse_writer import _encode_token

    token = "abc"
    enc = _encode_token(token)
    assert isinstance(enc, bytes)
    assert len(enc) == 4 + len(token.encode("utf-16-le"))


def test_upsert_raw_composes_merge_sql(monkeypatch):
    from shared import warehouse_writer
    from shared.warehouse_writer import WarehouseWriter

    fake_conn = MagicMock()
    fake_cursor = MagicMock()
    fake_cursor.rowcount = 2
    fake_conn.cursor.return_value = fake_cursor
    monkeypatch.setattr(warehouse_writer.pyodbc, "connect", MagicMock(return_value=fake_conn))

    w = WarehouseWriter(_make_settings(), _FakeCredential())
    with w:
        rows = [{"workspace_id": "w1", "raw_payload": {"name": "x"}}]
        n = w.upsert_raw("raw.workspace", ["workspace_id"], rows, "cr-1")

    # Check that CREATE TABLE, INSERT (executemany), and MERGE were issued
    sqls = [c.args[0] for c in fake_cursor.execute.call_args_list]
    joined = "\n".join(sqls)
    assert any("CREATE TABLE #stage_" in s for s in sqls)
    assert "MERGE raw.workspace AS tgt USING" in joined
    assert "WHEN MATCHED" in joined
    assert "WHEN NOT MATCHED" in joined
    fake_cursor.executemany.assert_called_once()
    fake_conn.commit.assert_called_once()
    assert n == 2


def test_upsert_raw_empty_returns_zero():
    from shared.warehouse_writer import WarehouseWriter

    w = WarehouseWriter(_make_settings(), _FakeCredential())
    assert w.upsert_raw("raw.workspace", ["workspace_id"], [], "cr-1") == 0


def test_upsert_raw_rejects_bad_identifier():
    from shared.warehouse_writer import WarehouseWriter

    w = WarehouseWriter(_make_settings(), _FakeCredential())
    with pytest.raises(ValueError):
        w.upsert_raw("raw; DROP", ["workspace_id"], [{"workspace_id": "x"}], "cr-1")


def test_execute_proc_no_params(monkeypatch):
    from shared import warehouse_writer
    from shared.warehouse_writer import WarehouseWriter

    fake_conn = MagicMock()
    fake_cursor = MagicMock()
    fake_cursor.rowcount = 5
    fake_conn.cursor.return_value = fake_cursor
    monkeypatch.setattr(warehouse_writer.pyodbc, "connect", MagicMock(return_value=fake_conn))

    w = WarehouseWriter(_make_settings(), _FakeCredential())
    with w:
        rc = w.execute_proc("dbo.merge_runs")
    assert rc == 5
    sql = fake_cursor.execute.call_args.args[0]
    assert sql == "EXEC dbo.merge_runs"


def test_execute_proc_with_params(monkeypatch):
    from shared import warehouse_writer
    from shared.warehouse_writer import WarehouseWriter

    fake_conn = MagicMock()
    fake_cursor = MagicMock()
    fake_cursor.rowcount = 1
    fake_conn.cursor.return_value = fake_cursor
    monkeypatch.setattr(warehouse_writer.pyodbc, "connect", MagicMock(return_value=fake_conn))

    w = WarehouseWriter(_make_settings(), _FakeCredential())
    with w:
        w.execute_proc("dbo.proc", {"workspace_id": "w1", "run_id": "r1"})
    sql, params = fake_cursor.execute.call_args.args
    assert sql == "EXEC dbo.proc @workspace_id = ?, @run_id = ?"
    assert params == ["w1", "r1"]


def test_ping(monkeypatch):
    from shared import warehouse_writer
    from shared.warehouse_writer import WarehouseWriter

    fake_conn = MagicMock()
    fake_cursor = MagicMock()
    fake_cursor.fetchone.return_value = (1,)
    fake_conn.cursor.return_value = fake_cursor
    monkeypatch.setattr(warehouse_writer.pyodbc, "connect", MagicMock(return_value=fake_conn))

    w = WarehouseWriter(_make_settings(), _FakeCredential())
    with w:
        assert w.ping() is True
