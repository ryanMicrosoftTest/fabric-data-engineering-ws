import secrets

import azure.durable_functions as df
import azure.functions as func
from pydantic import ValidationError

from function_app import app
from shared.logging_setup import configure_logging
from shared.models import BackfillRequest


@app.function_name(name="http_starter")
@app.route(route="start", methods=["POST"], auth_level=func.AuthLevel.FUNCTION)
@app.durable_client_input(client_name="client")
async def http_starter(req: func.HttpRequest, client: df.DurableOrchestrationClient) -> func.HttpResponse:
    try:
        body = req.get_json()
    except ValueError:
        return func.HttpResponse(
            '{"error":"body must be JSON"}',
            status_code=400,
            mimetype="application/json",
        )
    try:
        request_model = BackfillRequest.model_validate(body)
    except ValidationError as e:
        return func.HttpResponse(e.json(), status_code=400, mimetype="application/json")

    short = secrets.token_hex(3)
    instance_id = f"backfill-{request_model.from_date.isoformat()}-{request_model.to_date.isoformat()}-{short}"
    logger = configure_logging(collector_run_id=instance_id)

    existing = await client.get_status(instance_id)
    if existing is not None and existing.runtime_status in (
        df.OrchestrationRuntimeStatus.Running,
        df.OrchestrationRuntimeStatus.Pending,
    ):
        return func.HttpResponse(
            f'{{"error":"instance {instance_id} already running"}}',
            status_code=409,
            mimetype="application/json",
        )

    payload = request_model.model_dump(mode="json") | {"collector_run_id": instance_id, "trigger": "http"}
    await client.start_new("collector_orchestrator", instance_id, payload)
    logger.info("Started backfill", extra={"instance_id": instance_id})
    return client.create_check_status_response(req, instance_id)
