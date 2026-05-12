"""Activity: collect environments and their staging libraries."""
from __future__ import annotations

import time

from function_app import app
from shared.config import get_settings
from shared.fabric_client import FabricApiError, FabricClient
from shared.logging_setup import configure_logging

from activities._common import build_row, get_credential, persist_raw

ENV_TABLE = "raw.environment"
ENV_KEYS = ["workspace_id", "environment_id"]
LIB_TABLE = "raw.environment_library"
LIB_KEYS = ["workspace_id", "environment_id"]


@app.function_name(name="collect_environments")
@app.activity_trigger(input_name="payload")
async def collect_environments(payload: dict) -> dict:
    started = time.monotonic()
    cri = payload["collector_run_id"]
    wid = payload["workspace_id"]
    log = configure_logging(cri)
    try:
        settings = get_settings()
        credential = get_credential(settings)
        env_rows: list[dict] = []
        lib_rows: list[dict] = []
        env_ids: list[str] = []
        async with FabricClient(settings, credential) as client:
            async for item in client.get_paged(f"/v1/workspaces/{wid}/environments"):
                eid = item.get("id") or item.get("environmentId")
                if not eid:
                    continue
                env_ids.append(eid)
                env_rows.append(build_row(item, {"workspace_id": wid, "environment_id": eid}, cri))
            for eid in env_ids:
                try:
                    libs = await client.get(
                        f"/v1/workspaces/{wid}/environments/{eid}/staging/libraries"
                    )
                except FabricApiError as e:
                    if e.status_code == 404:
                        libs = {}
                    else:
                        raise
                lib_rows.append(build_row(libs, {"workspace_id": wid, "environment_id": eid}, cri))

        rows_written = 0
        rows_written += await persist_raw(settings, credential, ENV_TABLE, ENV_KEYS, env_rows, cri)
        rows_written += await persist_raw(settings, credential, LIB_TABLE, LIB_KEYS, lib_rows, cri)
        duration_ms = int((time.monotonic() - started) * 1000)
        log.info("collect_environments ok wid=%s envs=%s rows=%s", wid, len(env_ids), rows_written)
        return {"success": True, "rows_written": rows_written, "duration_ms": duration_ms}
    except FabricApiError as e:
        duration_ms = int((time.monotonic() - started) * 1000)
        log.error("collect_environments fabric error wid=%s: %s", wid, e)
        return {"success": False, "rows_written": 0, "error": str(e), "duration_ms": duration_ms}
    except Exception as e:  # noqa: BLE001
        duration_ms = int((time.monotonic() - started) * 1000)
        log.exception("collect_environments failed wid=%s", wid)
        return {"success": False, "rows_written": 0, "error": str(e), "duration_ms": duration_ms}