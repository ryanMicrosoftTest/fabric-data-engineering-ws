from __future__ import annotations

import azure.durable_functions as df

from function_app import app
from shared.config import get_settings
from shared.models import WorkspaceCollectInput


def collector_orchestrator(context: df.DurableOrchestrationContext):
    raw_input = context.get_input() or {}
    collector_run_id: str = raw_input["collector_run_id"]
    from_watermark = raw_input.get("from_watermark")
    to_watermark = raw_input.get("to_watermark") or context.current_utc_datetime.isoformat()

    if "workspace_ids" in raw_input:
        workspace_ids = raw_input.get("workspace_ids") or []
    else:
        workspace_ids = get_settings().target_workspace_id_list

    if not workspace_ids:
        return {"workspaces_processed": 0, "merge_complete": False}

    inputs = [
        WorkspaceCollectInput(
            collector_run_id=collector_run_id,
            workspace_id=wid,
            from_watermark=from_watermark,
            to_watermark=to_watermark,
        )
        for wid in workspace_ids
    ]

    tasks = [
        context.call_sub_orchestrator(
            "workspace_sub_orchestrator",
            w.model_dump(mode="json"),
            instance_id=f"{collector_run_id}-ws-{w.workspace_id}",
        )
        for w in inputs
    ]
    results = yield context.task_all(tasks)

    yield context.call_sub_orchestrator("merge_orchestrator", {"collector_run_id": collector_run_id})

    return {"workspaces_processed": len(results), "merge_complete": True}


app.function_name(name="collector_orchestrator")(
    app.orchestration_trigger(context_name="context")(collector_orchestrator)
)
