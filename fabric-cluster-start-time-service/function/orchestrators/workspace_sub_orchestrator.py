from __future__ import annotations

import azure.durable_functions as df

from function_app import app

LEVEL0_ACTIVITIES = [
    "collect_workspace",
    "collect_notebook_items",
    "collect_environments",
    "collect_spark_pools",
    "collect_spark_applications",
]


def _sum_rows(results) -> int:
    total = 0
    for r in results:
        if isinstance(r, dict):
            total += int(r.get("rows_written", 0) or 0)
    return total


def workspace_sub_orchestrator(context: df.DurableOrchestrationContext):
    raw_input = context.get_input() or {}
    cri = raw_input["collector_run_id"]
    wid = raw_input["workspace_id"]
    fw = raw_input.get("from_watermark")
    tw = raw_input.get("to_watermark")

    base = {
        "collector_run_id": cri,
        "workspace_id": wid,
        "from_watermark": fw,
        "to_watermark": tw,
    }

    level0_tasks = [context.call_activity(name, base) for name in LEVEL0_ACTIVITIES]
    level0_results = yield context.task_all(level0_tasks)

    notebook_items_result = level0_results[1] if len(level0_results) > 1 else {}
    notebook_ids = []
    if isinstance(notebook_items_result, dict):
        notebook_ids = notebook_items_result.get("notebook_ids") or []

    level1_input = {**base, "notebook_ids": notebook_ids}
    level1_tasks = [
        context.call_activity("collect_notebook_definitions", level1_input),
        context.call_activity("collect_job_instances", level1_input),
    ]
    level1_results = yield context.task_all(level1_tasks)

    job_instances_result = level1_results[1] if len(level1_results) > 1 else {}
    livy_ids = []
    if isinstance(job_instances_result, dict):
        livy_ids = job_instances_result.get("livy_session_ids") or []

    level2_input = {**base, "livy_session_ids": livy_ids}
    level2_tasks = [
        context.call_activity("collect_livy_sessions", level2_input),
        context.call_activity("collect_livy_statements", level2_input),
    ]
    level2_results = yield context.task_all(level2_tasks)

    return {
        "workspace_id": wid,
        "level0_count": _sum_rows(level0_results),
        "level1_count": _sum_rows(level1_results),
        "level2_count": _sum_rows(level2_results),
    }


app.function_name(name="workspace_sub_orchestrator")(
    app.orchestration_trigger(context_name="context")(workspace_sub_orchestrator)
)
