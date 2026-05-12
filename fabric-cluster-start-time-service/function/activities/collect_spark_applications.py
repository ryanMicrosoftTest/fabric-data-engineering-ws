"""Activity: collect spark applications for a workspace."""

from __future__ import annotations

import time

from activities._common import build_row, get_credential, persist_raw
from function_app import app
from shared.config import get_settings
from shared.fabric_client import FabricApiError, FabricClient
from shared.logging_setup import configure_logging

TABLE = "raw.spark_application"
KEY_COLUMNS = ["workspace_id", "application_id"]


@app.function_name(name="collect_spark_applications")
@app.activity_trigger(input_name="payload")
async def collect_spark_applications(payload: dict) -> dict:
    started = time.monotonic()
    cri = payload["collector_run_id"]
    wid = payload["workspace_id"]
    from_wm = payload.get("from_watermark")
    log = configure_logging(cri)
    try:
        settings = get_settings()
        credential = get_credential(settings)
        params: dict | None = None
        if from_wm:
            params = {"$filter": f"submittedDateTime ge {from_wm}"}
        rows: list[dict] = []
        async with FabricClient(settings, credential) as client:
            async for item in client.get_paged(f"/v1/workspaces/{wid}/spark/applications", params=params):
                aid = item.get("id") or item.get("applicationId") or item.get("livyId")
                if not aid:
                    continue
                rows.append(build_row(item, {"workspace_id": wid, "application_id": aid}, cri))
        rows_written = await persist_raw(settings, credential, TABLE, KEY_COLUMNS, rows, cri)
        duration_ms = int((time.monotonic() - started) * 1000)
        log.info("collect_spark_applications ok wid=%s rows=%s", wid, rows_written)
        return {"success": True, "rows_written": rows_written, "duration_ms": duration_ms}
    except FabricApiError as e:
        duration_ms = int((time.monotonic() - started) * 1000)
        log.error("collect_spark_applications fabric error wid=%s: %s", wid, e)
        return {"success": False, "rows_written": 0, "error": str(e), "duration_ms": duration_ms}
    except Exception as e:  # noqa: BLE001
        duration_ms = int((time.monotonic() - started) * 1000)
        log.exception("collect_spark_applications failed wid=%s", wid)
        return {"success": False, "rows_written": 0, "error": str(e), "duration_ms": duration_ms}
