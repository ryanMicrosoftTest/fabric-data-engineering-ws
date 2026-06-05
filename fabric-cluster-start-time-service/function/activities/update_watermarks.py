from datetime import datetime, timezone

from azure.identity import DefaultAzureCredential, ManagedIdentityCredential

from function_app import app
from shared.config import get_settings
from shared.logging_setup import configure_logging
from shared.warehouse_writer import WarehouseWriter
from shared.watermarks import update_watermark

_SOURCES = [
    "workspace",
    "notebook_item",
    "notebook_definition",
    "environment",
    "spark_pool",
    "spark_application",
    "job_instance",
    "livy_session",
    "livy_statement",
]


def _get_credential(settings):
    if settings.mi_client_id:
        return ManagedIdentityCredential(client_id=settings.mi_client_id)
    return DefaultAzureCredential()


@app.function_name(name="update_watermarks")
@app.activity_trigger(input_name="payload")
def update_watermarks(payload: dict) -> dict:
    cri = payload["collector_run_id"]
    workspace_id = payload.get("workspace_id")
    logger = configure_logging(collector_run_id=cri)
    started = datetime.now(timezone.utc)
    now = started
    settings = get_settings()
    credential = _get_credential(settings)
    updated = 0
    current_source: str | None = None
    try:
        with WarehouseWriter(settings, credential) as wh:
            for src in _SOURCES:
                current_source = src
                update_watermark(wh, src, workspace_id, now, cri)
                updated += 1
        duration_ms = int((datetime.now(timezone.utc) - started).total_seconds() * 1000)
        logger.info(
            "update_watermarks complete",
            extra={"rows_written": updated, "duration_ms": duration_ms},
        )
        return {"success": True, "rows_written": updated, "duration_ms": duration_ms}
    except Exception as e:  # noqa: BLE001
        duration_ms = int((datetime.now(timezone.utc) - started).total_seconds() * 1000)
        logger.exception("update_watermarks failed")
        err = f"{type(e).__name__}: {e}"
        if current_source:
            err = f"source={current_source}: {err}"
        return {
            "success": False,
            "rows_written": 0,
            "error": err,
            "duration_ms": duration_ms,
        }
