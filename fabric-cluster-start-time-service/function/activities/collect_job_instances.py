"""Activity: collect job instances for each notebook."""
from __future__ import annotations

import asyncio
import time

from function_app import app
from shared.config import get_settings
from shared.fabric_client import FabricApiError, FabricClient
from shared.logging_setup import configure_logging

from activities._common import build_row, get_credential, persist_raw

TABLE = "raw.job_instance"
KEY_COLUMNS = ["workspace_id", "item_id", "job_instance_id"]
_CONCURRENCY = 4


def _extract_livy_id(instance: dict) -> str | None:
    livy = instance.get("livyId")
    if livy:
        return livy
    params = instance.get("parameters")
    if isinstance(params, dict):
        livy = params.get("livyId")
        if livy:
            return livy
    return None


async def _fetch_for_notebook(
    client: FabricClient,
    wid: str,
    nid: str,
    params: dict | None,
    sem: asyncio.Semaphore,
) -> list[dict]:
    async with sem:
        items: list[dict] = []
        async for inst in client.get_paged(
            f"/v1/workspaces/{wid}/items/{nid}/jobs/instances", params=params
        ):
            items.append(inst)
        return items


@app.function_name(name="collect_job_instances")
@app.activity_trigger(input_name="payload")
async def collect_job_instances(payload: dict) -> dict:
    started = time.monotonic()
    cri = payload["collector_run_id"]
    wid = payload["workspace_id"]
    notebook_ids: list[str] = list(payload.get("notebook_ids") or [])
    from_wm = payload.get("from_watermark")
    log = configure_logging(cri)
    try:
        settings = get_settings()
        credential = get_credential(settings)
        params: dict | None = None
        if from_wm:
            params = {"$filter": f"startTimeUtc ge {from_wm}"}
        rows: list[dict] = []
        livy_session_ids: list[str] = []
        if notebook_ids:
            sem = asyncio.Semaphore(_CONCURRENCY)
            async with FabricClient(settings, credential) as client:
                results = await asyncio.gather(
                    *[_fetch_for_notebook(client, wid, nid, params, sem) for nid in notebook_ids]
                )
            for nid, instances in zip(notebook_ids, results):
                for inst in instances:
                    jid = inst.get("id") or inst.get("jobInstanceId")
                    if not jid:
                        continue
                    rows.append(
                        build_row(
                            inst,
                            {"workspace_id": wid, "item_id": nid, "job_instance_id": jid},
                            cri,
                        )
                    )
                    livy = _extract_livy_id(inst)
                    if livy:
                        livy_session_ids.append(livy)
        rows_written = await persist_raw(settings, credential, TABLE, KEY_COLUMNS, rows, cri)
        duration_ms = int((time.monotonic() - started) * 1000)
        log.info(
            "collect_job_instances ok wid=%s rows=%s livy=%s",
            wid,
            rows_written,
            len(livy_session_ids),
        )
        return {
            "success": True,
            "rows_written": rows_written,
            "livy_session_ids": livy_session_ids,
            "duration_ms": duration_ms,
        }
    except FabricApiError as e:
        duration_ms = int((time.monotonic() - started) * 1000)
        log.error("collect_job_instances fabric error wid=%s: %s", wid, e)
        return {
            "success": False,
            "rows_written": 0,
            "livy_session_ids": [],
            "error": str(e),
            "duration_ms": duration_ms,
        }
    except Exception as e:  # noqa: BLE001
        duration_ms = int((time.monotonic() - started) * 1000)
        log.exception("collect_job_instances failed wid=%s", wid)
        return {
            "success": False,
            "rows_written": 0,
            "livy_session_ids": [],
            "error": str(e),
            "duration_ms": duration_ms,
        }