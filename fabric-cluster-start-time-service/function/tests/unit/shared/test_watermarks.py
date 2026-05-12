from datetime import datetime, timezone
from unittest.mock import MagicMock

from shared.watermarks import read_watermark, update_watermark


def _writer_with_cursor():
    writer = MagicMock()
    cursor = MagicMock()
    writer._cursor.return_value = cursor
    return writer, cursor


def test_read_watermark_returns_none_when_missing():
    writer, cursor = _writer_with_cursor()
    cursor.fetchone.return_value = None
    assert read_watermark(writer, "spark_apps", "ws-1") is None
    sql, params = cursor.execute.call_args.args
    assert "workspace_id = ?" in sql
    assert params == ["spark_apps", "ws-1"]


def test_read_watermark_returns_value():
    writer, cursor = _writer_with_cursor()
    dt = datetime(2024, 1, 1, tzinfo=timezone.utc)
    cursor.fetchone.return_value = (dt,)
    result = read_watermark(writer, "spark_apps", "ws-1")
    assert result == dt


def test_read_watermark_workspace_none_uses_is_null():
    writer, cursor = _writer_with_cursor()
    cursor.fetchone.return_value = None
    read_watermark(writer, "spark_apps")
    sql = cursor.execute.call_args.args[0]
    assert "workspace_id IS NULL" in sql


def test_update_watermark_updates_when_existing():
    writer, cursor = _writer_with_cursor()
    cursor.rowcount = 1
    update_watermark(
        writer, "spark_apps", "ws-1", datetime(2024, 1, 2, tzinfo=timezone.utc), "cr-1"
    )
    # Only one execute call (UPDATE succeeded)
    assert cursor.execute.call_count == 1
    sql = cursor.execute.call_args_list[0].args[0]
    assert sql.startswith("UPDATE")


def test_update_watermark_inserts_when_missing():
    writer, cursor = _writer_with_cursor()
    cursor.rowcount = 0
    update_watermark(
        writer, "spark_apps", "ws-1", datetime(2024, 1, 2, tzinfo=timezone.utc), "cr-1"
    )
    assert cursor.execute.call_count == 2
    update_sql = cursor.execute.call_args_list[0].args[0]
    insert_sql = cursor.execute.call_args_list[1].args[0]
    assert update_sql.startswith("UPDATE")
    assert insert_sql.startswith("INSERT INTO")


def test_update_watermark_workspace_none():
    writer, cursor = _writer_with_cursor()
    cursor.rowcount = 0
    update_watermark(
        writer, "global", None, datetime(2024, 1, 2, tzinfo=timezone.utc), "cr-1"
    )
    update_sql = cursor.execute.call_args_list[0].args[0]
    assert "workspace_id IS NULL" in update_sql
    insert_sql = cursor.execute.call_args_list[1].args[0]
    assert "VALUES (?, NULL, ?, ?)" in insert_sql
