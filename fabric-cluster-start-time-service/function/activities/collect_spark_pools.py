"""Activity: collect spark pools and workspace default settings."""

from __future__ import annotations

import time

from activities._common import build_row, get_credential, persist_raw
from function_app import app
from shared.config import get_settings
from shared.fabric_client import FabricApiError, FabricClient
from shared.logging_setup import configure_logging

TABLE = "raw.spark_pool"
KEY_COLUMNS = ["workspace_id", "pool_id"]


@app.function_name(name="collect_spark_pools")
@app.activity_trigger(input_name="payload")
async def collect_spark_pools(payload: dict) -> dict:
    started = time.monotonic()
    cri = payload["collector_run_id"]
    wid = payload["workspace_id"]
    log = configure_logging(cri)
    try:
        settings = get_settings()
        credential = get_credential(settings)
        rows: list[dict] = []
        async with FabricClient(settings, credential) as client:
            async for item in client.get_paged(f"/v1/workspaces/{wid}/spark/pools"):
                pid = item.get("id") or item.get("poolId") or item.get("name")
                if not pid:
                    continue
                payload_with_type = {**item, "pool_type": "custom"}
                rows.append(build_row(payload_with_type, {"workspace_id": wid, "pool_id": pid}, cri))
            settings_obj = await client.get(f"/v1/workspaces/{wid}/spark/settings")
            ws_payload = {**settings_obj, "pool_type": "workspace_default"}
            rows.append(build_row(ws_payload, {"workspace_id": wid, "pool_id": "workspace_default"}, cri))
        rows_written = await persist_raw(settings, credential, TABLE, KEY_COLUMNS, rows, cri)
        duration_ms = int((time.monotonic() - started) * 1000)
        log.info("collect_spark_pools ok wid=%s rows=%s", wid, rows_written)
        return {"success": True, "rows_written": rows_written, "duration_ms": duration_ms}
    except FabricApiError as e:
        duration_ms = int((time.monotonic() - started) * 1000)
        log.error("collect_spark_pools fabric error wid=%s: %s", wid, e)
        return {"success": False, "rows_written": 0, "error": str(e), "duration_ms": duration_ms}
    except Exception as e:  # noqa: BLE001
        duration_ms = int((time.monotonic() - started) * 1000)
        log.exception("collect_spark_pools failed wid=%s", wid)
        return {"success": False, "rows_written": 0, "error": str(e), "duration_ms": duration_ms}
