"""Tests for merge_orchestrator."""
from __future__ import annotations

from orchestrators.merge_orchestrator import merge_orchestrator


def _drive(gen):
    sent = None
    try:
        while True:
            sent = gen.send(sent)
    except StopIteration as e:
        return e.value


def test_sequential_three_activities(mock_durable_context):
    ctx = mock_durable_context
    ctx.call_activity.side_effect = [
        {"success": True, "rows_written": 10},
        {"success": True, "rows_written": 5},
        {"success": True, "rows_written": 0},
    ]
    ctx.get_input.return_value = {"collector_run_id": "run-merge"}

    _drive(merge_orchestrator(ctx))

    activity_calls = ctx.call_activity.call_args_list
    assert len(activity_calls) == 3
    assert [c.args[0] for c in activity_calls] == [
        "merge_notebook_runs",
        "merge_environment_versions",
        "update_watermarks",
    ]
    for c in activity_calls:
        assert c.args[1] == {"collector_run_id": "run-merge"}

    assert ctx.task_all.call_count == 0


def test_returns_aggregate(mock_durable_context):
    ctx = mock_durable_context
    r1 = {"success": True, "rows_written": 10}
    r2 = {"success": True, "rows_written": 5}
    r3 = {"success": True, "rows_written": 0}
    ctx.call_activity.side_effect = [r1, r2, r3]
    ctx.get_input.return_value = {"collector_run_id": "run-merge"}

    result = _drive(merge_orchestrator(ctx))

    assert result == {
        "merge_notebook_runs": r1,
        "merge_environment_versions": r2,
        "update_watermarks": r3,
    }
