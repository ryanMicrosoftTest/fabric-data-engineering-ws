"""Activity: collect livy statements for each session id."""
from __future__ import annotations

import asyncio
import time

from function_app import app
from shared.config import get_settings
from shared.fabric_client import FabricApiError, FabricClient
from shared.logging_setup import configure_logging

from activities._common import build_row, get_credential, persist_raw

TABLE = "raw.livy_statement"
KEY_COLUMNS = ["workspace_id", "livy_session_id", "statement_id"]
_CONCURRENCY = 4


async def _fetch_one(
    client: FabricClient, wid: str, livy_id: str, sem: asyncio.Semaphore
) -> list[dict]:
    async with sem:
        try:
            resp = await client.get(
                f"/v1/workspaces/{wid}/spark/livySessions/{livy_id}/statements"
            )
        except FabricApiError as e:
            if e.status_code == 404:
                return []
            raise
        if isinstance(resp, dict):
            return list(resp.get("statements") or resp.get("value") or [])
        if isinstance(resp, list):
            return resp
        return []


@app.function_name(name="collect_livy_statements")
@app.activity_trigger(input_name="payload")
async def collect_livy_statements(payload: dict) -> dict:
    started = time.monotonic()
    cri = payload["collector_run_id"]
    wid = payload["workspace_id"]
    livy_ids: list[str] = list(payload.get("livy_session_ids") or [])
    log = configure_logging(cri)
    try:
        settings = get_settings()
        credential = get_credential(settings)
        rows: list[dict] = []
        if livy_ids:
            sem = asyncio.Semaphore(_CONCURRENCY)
            async with FabricClient(settings, credential) as client:
                results = await asyncio.gather(
                    *[_fetch_one(client, wid, lid, sem) for lid in livy_ids]
                )
            for lid, statements in zip(livy_ids, results):
                for stmt in statements:
                    sid = stmt.get("id")
                    if sid is None:
                        sid = stmt.get("statementId")
                    if sid is None:
                        continue
                    rows.append(
                        build_row(
                            stmt,
                            {
                                "workspace_id": wid,
                                "livy_session_id": lid,
                                "statement_id": str(sid),
                            },
                            cri,
                        )
                    )
        rows_written = await persist_raw(settings, credential, TABLE, KEY_COLUMNS, rows, cri)
        duration_ms = int((time.monotonic() - started) * 1000)
        log.info("collect_livy_statements ok wid=%s rows=%s", wid, rows_written)
        return {"success": True, "rows_written": rows_written, "duration_ms": duration_ms}
    except FabricApiError as e:
        duration_ms = int((time.monotonic() - started) * 1000)
        log.error("collect_livy_statements fabric error wid=%s: %s", wid, e)
        return {"success": False, "rows_written": 0, "error": str(e), "duration_ms": duration_ms}
    except Exception as e:  # noqa: BLE001
        duration_ms = int((time.monotonic() - started) * 1000)
        log.exception("collect_livy_statements failed wid=%s", wid)
        return {"success": False, "rows_written": 0, "error": str(e), "duration_ms": duration_ms}