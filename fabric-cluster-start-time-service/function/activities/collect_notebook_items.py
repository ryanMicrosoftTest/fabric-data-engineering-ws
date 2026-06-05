"""Activity: list notebook items in a workspace."""

from __future__ import annotations

import time

from activities._common import build_row, get_credential, persist_raw
from function_app import app
from shared.config import get_settings
from shared.fabric_client import FabricApiError, FabricClient
from shared.logging_setup import configure_logging

TABLE = "raw.notebook_item"
KEY_COLUMNS = ["workspace_id", "notebook_id"]


@app.function_name(name="collect_notebook_items")
@app.activity_trigger(input_name="payload")
async def collect_notebook_items(payload: dict) -> dict:
    started = time.monotonic()
    cri = payload["collector_run_id"]
    wid = payload["workspace_id"]
    log = configure_logging(cri)
    try:
        settings = get_settings()
        credential = get_credential(settings)
        notebook_ids: list[str] = []
        rows: list[dict] = []
        async with FabricClient(settings, credential) as client:
            async for item in client.get_paged(f"/v1/workspaces/{wid}/notebooks"):
                nid = item.get("id") or item.get("notebookId")
                if not nid:
                    continue
                notebook_ids.append(nid)
                rows.append(build_row(item, {"workspace_id": wid, "notebook_id": nid}, cri))
        rows_written = await persist_raw(settings, credential, TABLE, KEY_COLUMNS, rows, cri)
        duration_ms = int((time.monotonic() - started) * 1000)
        log.info("collect_notebook_items ok wid=%s rows=%s", wid, rows_written)
        return {
            "success": True,
            "rows_written": rows_written,
            "notebook_ids": notebook_ids,
            "duration_ms": duration_ms,
        }
    except FabricApiError as e:
        duration_ms = int((time.monotonic() - started) * 1000)
        log.error("collect_notebook_items fabric error wid=%s: %s", wid, e)
        return {
            "success": False,
            "rows_written": 0,
            "notebook_ids": [],
            "error": str(e),
            "duration_ms": duration_ms,
        }
    except Exception as e:  # noqa: BLE001
        duration_ms = int((time.monotonic() - started) * 1000)
        log.exception("collect_notebook_items failed wid=%s", wid)
        return {
            "success": False,
            "rows_written": 0,
            "notebook_ids": [],
            "error": str(e),
            "duration_ms": duration_ms,
        }
