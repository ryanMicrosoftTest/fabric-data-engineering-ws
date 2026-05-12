"""Activity: collect a single workspace's metadata."""
from __future__ import annotations

import time

from function_app import app
from shared.config import get_settings
from shared.fabric_client import FabricApiError, FabricClient
from shared.logging_setup import configure_logging

from activities._common import build_row, get_credential, persist_raw

TABLE = "raw.workspace"
KEY_COLUMNS = ["workspace_id"]


@app.function_name(name="collect_workspace")
@app.activity_trigger(input_name="payload")
async def collect_workspace(payload: dict) -> dict:
    started = time.monotonic()
    cri = payload["collector_run_id"]
    wid = payload["workspace_id"]
    log = configure_logging(cri)
    try:
        settings = get_settings()
        credential = get_credential(settings)
        async with FabricClient(settings, credential) as client:
            item = await client.get(f"/v1/workspaces/{wid}")
        rows = [build_row(item, {"workspace_id": wid}, cri)]
        rows_written = await persist_raw(settings, credential, TABLE, KEY_COLUMNS, rows, cri)
        duration_ms = int((time.monotonic() - started) * 1000)
        log.info("collect_workspace ok wid=%s rows=%s", wid, rows_written)
        return {
            "success": True,
            "rows_written": rows_written,
            "workspace_id": wid,
            "duration_ms": duration_ms,
        }
    except FabricApiError as e:
        duration_ms = int((time.monotonic() - started) * 1000)
        log.error("collect_workspace fabric error wid=%s: %s", wid, e)
        return {"success": False, "rows_written": 0, "error": str(e), "duration_ms": duration_ms}
    except Exception as e:  # noqa: BLE001
        duration_ms = int((time.monotonic() - started) * 1000)
        log.exception("collect_workspace failed wid=%s", wid)
        return {"success": False, "rows_written": 0, "error": str(e), "duration_ms": duration_ms}