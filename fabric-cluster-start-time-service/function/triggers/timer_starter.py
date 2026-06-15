from datetime import datetime

import azure.durable_functions as df
import azure.functions as func

from function_app import app
from shared.logging_setup import configure_logging


@app.function_name(name="timer_starter")
@app.timer_trigger(schedule="0 0 10 * * *", arg_name="myTimer", run_on_startup=False, use_monitor=True)
@app.durable_client_input(client_name="client")
async def timer_starter(myTimer: func.TimerRequest, client: df.DurableOrchestrationClient) -> None:  # noqa: N803
    today = datetime.utcnow().strftime("%Y%m%d")
    instance_id = f"daily-collector-{today}"
    logger = configure_logging(collector_run_id=instance_id)

    if myTimer.past_due:
        logger.warning("Timer trigger is past due", extra={"instance_id": instance_id})

    existing = await client.get_status(instance_id)
    if existing is not None and existing.runtime_status in (
        df.OrchestrationRuntimeStatus.Running,
        df.OrchestrationRuntimeStatus.Pending,
        df.OrchestrationRuntimeStatus.ContinuedAsNew,
    ):
        logger.info(
            "Skipping daily collector start; already running",
            extra={"instance_id": instance_id, "runtime_status": str(existing.runtime_status)},
        )
        return

    payload = {"collector_run_id": instance_id, "trigger": "timer", "from_watermark": None}
    try:
        await client.start_new("collector_orchestrator", instance_id, payload)
    except Exception as e:  # noqa: BLE001
        fallback_id = f"{instance_id}-{datetime.utcnow().strftime('%H%M%S')}"
        logger.warning(
            "start_new failed for instance_id; retrying with fallback",
            extra={"instance_id": instance_id, "fallback_id": fallback_id, "error": str(e)},
        )
        payload["collector_run_id"] = fallback_id
        await client.start_new("collector_orchestrator", fallback_id, payload)
        instance_id = fallback_id

    logger.info("Started daily collector", extra={"instance_id": instance_id})
