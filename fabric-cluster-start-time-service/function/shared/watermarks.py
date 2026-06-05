from datetime import datetime
from typing import Any

from .warehouse_writer import WarehouseWriter

_TABLE = "meta.collection_watermark"


def read_watermark(
    writer: WarehouseWriter,
    source: str,
    workspace_id: str | None = None,
) -> datetime | None:
    cur = writer._cursor()  # noqa: SLF001
    if workspace_id is None:
        sql = f"SELECT last_run_at FROM {_TABLE} WHERE source = ? AND workspace_id IS NULL"
        cur.execute(sql, [source])
    else:
        sql = f"SELECT last_run_at FROM {_TABLE} WHERE source = ? AND workspace_id = ?"
        cur.execute(sql, [source, workspace_id])
    row = cur.fetchone()
    if not row:
        return None
    val: Any = row[0]
    if isinstance(val, datetime) or val is None:
        return val
    return datetime.fromisoformat(str(val))


def update_watermark(
    writer: WarehouseWriter,
    source: str,
    workspace_id: str | None,
    last_run_at: datetime,
    collector_run_id: str,
) -> None:
    cur = writer._cursor()  # noqa: SLF001
    if workspace_id is None:
        cur.execute(
            f"UPDATE {_TABLE} SET last_run_at = ?, collector_run_id = ? " f"WHERE source = ? AND workspace_id IS NULL",
            [last_run_at, collector_run_id, source],
        )
        if (cur.rowcount or 0) == 0:
            cur.execute(
                f"INSERT INTO {_TABLE} (source, workspace_id, last_run_at, collector_run_id) "
                f"VALUES (?, NULL, ?, ?)",
                [source, last_run_at, collector_run_id],
            )
    else:
        cur.execute(
            f"UPDATE {_TABLE} SET last_run_at = ?, collector_run_id = ? " f"WHERE source = ? AND workspace_id = ?",
            [last_run_at, collector_run_id, source, workspace_id],
        )
        if (cur.rowcount or 0) == 0:
            cur.execute(
                f"INSERT INTO {_TABLE} (source, workspace_id, last_run_at, collector_run_id) " f"VALUES (?, ?, ?, ?)",
                [source, workspace_id, last_run_at, collector_run_id],
            )
