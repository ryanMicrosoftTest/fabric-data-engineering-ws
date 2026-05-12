"""Activity: fetch notebook definitions via LRO."""
from __future__ import annotations

import asyncio
import time

from function_app import app
from shared.config import get_settings
from shared.fabric_client import FabricApiError, FabricClient
from shared.logging_setup import configure_logging

from activities._common import build_row, get_credential, persist_raw

TABLE = "raw.notebook_definition"
KEY_COLUMNS = ["workspace_id", "notebook_id"]
_CONCURRENCY = 4


async def _fetch_one(client: FabricClient, wid: str, nid: str, sem: asyncio.Semaphore) -> dict | None:
    async with sem:
        try:
            return await client.post_lro(
                f"/v1/workspaces/{wid}/notebooks/{nid}/getDefinition?format=ipynb",
                json={},
            )
        except FabricApiError:
            raise


@app.function_name(name="collect_notebook_definitions")
@app.activity_trigger(input_name="payload")
async def collect_notebook_definitions(payload: dict) -> dict:
    started = time.monotonic()
    cri = payload["collector_run_id"]
    wid = payload["workspace_id"]
    notebook_ids: list[str] = list(payload.get("notebook_ids") or [])
    log = configure_logging(cri)
    try:
        settings = get_settings()
        credential = get_credential(settings)
        rows: list[dict] = []
        if notebook_ids:
            sem = asyncio.Semaphore(_CONCURRENCY)
            async with FabricClient(settings, credential) as client:
                results = await asyncio.gather(
                    *[_fetch_one(client, wid, nid, sem) for nid in notebook_ids],
                    return_exceptions=False,
                )
            for nid, definition in zip(notebook_ids, results):
                if definition is None:
                    continue
                rows.append(build_row(definition, {"workspace_id": wid, "notebook_id": nid}, cri))
        rows_written = await persist_raw(settings, credential, TABLE, KEY_COLUMNS, rows, cri)
        duration_ms = int((time.monotonic() - started) * 1000)
        log.info("collect_notebook_definitions ok wid=%s rows=%s", wid, rows_written)
        return {"success": True, "rows_written": rows_written, "duration_ms": duration_ms}
    except FabricApiError as e:
        duration_ms = int((time.monotonic() - started) * 1000)
        log.error("collect_notebook_definitions fabric error wid=%s: %s", wid, e)
        return {"success": False, "rows_written": 0, "error": str(e), "duration_ms": duration_ms}
    except Exception as e:  # noqa: BLE001
        duration_ms = int((time.monotonic() - started) * 1000)
        log.exception("collect_notebook_definitions failed wid=%s", wid)
        return {"success": False, "rows_written": 0, "error": str(e), "duration_ms": duration_ms}