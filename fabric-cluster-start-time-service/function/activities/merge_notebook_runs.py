from datetime import datetime, timezone

from azure.identity import DefaultAzureCredential, ManagedIdentityCredential

from function_app import app
from shared.config import get_settings
from shared.logging_setup import configure_logging
from shared.warehouse_writer import WarehouseWriter


def _get_credential(settings):
    if settings.mi_client_id:
        return ManagedIdentityCredential(client_id=settings.mi_client_id)
    return DefaultAzureCredential()


@app.function_name(name="merge_notebook_runs")
@app.activity_trigger(input_name="payload")
def merge_notebook_runs(payload: dict) -> dict:
    cri = payload["collector_run_id"]
    logger = configure_logging(collector_run_id=cri)
    started = datetime.now(timezone.utc)
    settings = get_settings()
    credential = _get_credential(settings)
    try:
        with WarehouseWriter(settings, credential) as wh:
            rowcount = wh.execute_proc("dbo.usp_merge_notebook_runs", {"collector_run_id": cri})
        duration_ms = int((datetime.now(timezone.utc) - started).total_seconds() * 1000)
        logger.info(
            "merge_notebook_runs complete",
            extra={"rows_written": rowcount, "duration_ms": duration_ms},
        )
        return {"success": True, "rows_written": rowcount, "duration_ms": duration_ms}
    except Exception as e:  # noqa: BLE001
        duration_ms = int((datetime.now(timezone.utc) - started).total_seconds() * 1000)
        logger.exception("merge_notebook_runs failed")
        return {
            "success": False,
            "rows_written": 0,
            "error": f"{type(e).__name__}: {e}",
            "duration_ms": duration_ms,
        }
