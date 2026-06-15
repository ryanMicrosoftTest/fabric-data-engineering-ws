import json
import re
import struct
from datetime import datetime, timezone
from typing import Any

import pyodbc  # type: ignore

from .retry import warehouse_retry

SQL_COPT_SS_ACCESS_TOKEN = 1256
_IDENT_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")
_DB_SCOPE = "https://database.windows.net/.default"


def _validate_identifier(name: str) -> str:
    parts = name.split(".")
    for p in parts:
        if not _IDENT_RE.match(p):
            raise ValueError(f"invalid SQL identifier: {name}")
    return name


def _encode_token(token: str) -> bytes:
    raw = token.encode("utf-16-le")
    return struct.pack(f"<I{len(raw)}s", len(raw), raw)


class WarehouseWriter:
    def __init__(self, settings, credential):
        self._settings = settings
        self._credential = credential
        self._conn: pyodbc.Connection | None = None

    def _build_connection_string(self) -> str:
        endpoint = self._settings.warehouse_sql_endpoint
        database = self._settings.warehouse_database
        return (
            "Driver={ODBC Driver 18 for SQL Server};"
            f"Server=tcp:{endpoint},1433;"
            f"Database={database};"
            "Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30"
        )

    def _get_token_bytes(self) -> bytes:
        result = self._credential.get_token(_DB_SCOPE)
        return _encode_token(result.token)

    def __enter__(self) -> "WarehouseWriter":
        token_bytes = self._get_token_bytes()
        self._conn = pyodbc.connect(
            self._build_connection_string(),
            attrs_before={SQL_COPT_SS_ACCESS_TOKEN: token_bytes},
        )
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        if self._conn is not None:
            try:
                self._conn.close()
            finally:
                self._conn = None

    def _cursor(self):
        if self._conn is None:
            raise RuntimeError("WarehouseWriter must be used as a context manager")
        return self._conn.cursor()

    @warehouse_retry()
    def ping(self) -> bool:
        cur = self._cursor()
        cur.execute("SELECT 1")
        row = cur.fetchone()
        return bool(row and row[0] == 1)

    @warehouse_retry()
    def upsert_raw(
        self,
        table: str,
        key_columns: list[str],
        rows: list[dict],
        collector_run_id: str,
    ) -> int:
        if not rows:
            return 0
        _validate_identifier(table)
        for k in key_columns:
            _validate_identifier(k)
        if self._conn is None:
            raise RuntimeError("WarehouseWriter must be used as a context manager")

        ingested_at = datetime.now(timezone.utc)
        all_cols = list(key_columns) + ["raw_payload", "collector_run_id", "ingested_at"]
        col_list = ", ".join(all_cols)
        placeholders = ", ".join(["?"] * len(all_cols))

        params = []
        for r in rows:
            row_vals = [r.get(k) for k in key_columns]
            payload = r.get("raw_payload")
            if payload is None:
                payload = json.dumps(r, default=str)
            elif not isinstance(payload, str):
                payload = json.dumps(payload, default=str)
            row_vals.extend([payload, collector_run_id, ingested_at])
            params.append(tuple(row_vals))

        stage = f"#stage_{re.sub(r'[^a-zA-Z0-9_]', '_', table)}"
        col_defs = ", ".join([f"{c} NVARCHAR(400)" for c in key_columns]) + (
            ", raw_payload NVARCHAR(MAX), collector_run_id NVARCHAR(50), ingested_at DATETIME2"
        )
        on_clause = " AND ".join([f"tgt.{c} = src.{c}" for c in key_columns])
        update_set = ", ".join(
            [
                "tgt.raw_payload = src.raw_payload",
                "tgt.collector_run_id = src.collector_run_id",
                "tgt.ingested_at = src.ingested_at",
            ]
        )
        insert_cols = col_list
        insert_vals = ", ".join([f"src.{c}" for c in all_cols])

        cur = self._conn.cursor()
        try:
            self._conn.autocommit = False
            cur.execute(f"CREATE TABLE {stage} ({col_defs})")
            cur.fast_executemany = True
            cur.executemany(
                f"INSERT INTO {stage} ({col_list}) VALUES ({placeholders})",
                params,
            )
            merge_sql = (
                f"MERGE {table} AS tgt USING {stage} AS src ON {on_clause} "
                f"WHEN MATCHED THEN UPDATE SET {update_set} "
                f"WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals});"
            )
            cur.execute(merge_sql)
            written = cur.rowcount
            self._conn.commit()
            return int(written) if written is not None and written >= 0 else len(rows)
        except Exception:
            try:
                self._conn.rollback()
            except Exception:  # noqa: BLE001
                pass
            raise
        finally:
            try:
                self._conn.autocommit = True
            except Exception:  # noqa: BLE001
                pass

    @warehouse_retry()
    def execute_proc(self, proc_name: str, params: dict[str, Any] | None = None) -> int:
        _validate_identifier(proc_name)
        cur = self._cursor()
        if not params:
            cur.execute(f"EXEC {proc_name}")
            return int(cur.rowcount or 0)
        names = list(params.keys())
        for n in names:
            _validate_identifier(n)
        placeholders = ", ".join([f"@{n} = ?" for n in names])
        cur.execute(f"EXEC {proc_name} {placeholders}", [params[n] for n in names])
        return int(cur.rowcount or 0)
