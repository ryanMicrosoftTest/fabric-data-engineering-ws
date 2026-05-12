from __future__ import annotations

import azure.durable_functions as df

from function_app import app


def merge_orchestrator(context: df.DurableOrchestrationContext):
    raw_input = context.get_input() or {}
    cri = raw_input["collector_run_id"]
    payload = {"collector_run_id": cri}

    r1 = yield context.call_activity("merge_notebook_runs", payload)
    r2 = yield context.call_activity("merge_environment_versions", payload)
    r3 = yield context.call_activity("update_watermarks", payload)

    return {
        "merge_notebook_runs": r1,
        "merge_environment_versions": r2,
        "update_watermarks": r3,
    }


app.function_name(name="merge_orchestrator")(
    app.orchestration_trigger(context_name="context")(merge_orchestrator)
)
