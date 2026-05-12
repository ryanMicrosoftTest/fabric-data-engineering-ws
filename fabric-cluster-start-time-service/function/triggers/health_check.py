import json

import azure.functions as func
from azure.identity import DefaultAzureCredential

from function_app import app
from shared.config import get_settings
from shared.warehouse_writer import WarehouseWriter


@app.function_name(name="health_check")
@app.route(route="health", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def health_check(req: func.HttpRequest) -> func.HttpResponse:
    settings = get_settings()
    body: dict = {"status": "ok", "version": "0.1.0"}
    deep = req.params.get("deep", "false").lower() == "true"
    if deep:
        try:
            credential = DefaultAzureCredential()
            with WarehouseWriter(settings, credential) as wh:
                body["warehouse"] = "ok" if wh.ping() else "fail"
        except Exception as e:  # noqa: BLE001
            body["warehouse"] = f"error: {type(e).__name__}"
            return func.HttpResponse(
                json.dumps(body), status_code=503, mimetype="application/json"
            )
    return func.HttpResponse(json.dumps(body), status_code=200, mimetype="application/json")
