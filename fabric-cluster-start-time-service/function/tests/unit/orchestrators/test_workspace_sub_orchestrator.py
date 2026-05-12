"""Tests for workspace_sub_orchestrator."""

from __future__ import annotations

from orchestrators.workspace_sub_orchestrator import (
    LEVEL0_ACTIVITIES,
    workspace_sub_orchestrator,
)


def _drive(gen):
    sent = None
    try:
        while True:
            sent = gen.send(sent)
    except StopIteration as e:
        return e.value


def _base_input():
    return {
        "collector_run_id": "run-1",
        "workspace_id": "ws-1",
        "from_watermark": None,
        "to_watermark": "2024-01-15T10:00:00+00:00",
    }


def test_dag_three_levels_in_order(mock_durable_context):
    ctx = mock_durable_context
    ctx.call_activity.side_effect = [
        {"rows_written": 1},
        {"rows_written": 2, "notebook_ids": ["n1", "n2"]},
        {"rows_written": 3},
        {"rows_written": 4},
        {"rows_written": 5},
        {"rows_written": 6},
        {"rows_written": 7, "livy_session_ids": ["l1"]},
        {"rows_written": 8},
        {"rows_written": 9},
    ]
    ctx.get_input.return_value = _base_input()

    result = _drive(workspace_sub_orchestrator(ctx))

    assert ctx.task_all.call_count == 3

    activity_calls = ctx.call_activity.call_args_list
    assert len(activity_calls) == 9

    level0_names = [c.args[0] for c in activity_calls[:5]]
    assert level0_names == LEVEL0_ACTIVITIES

    level1_names = [c.args[0] for c in activity_calls[5:7]]
    assert level1_names == ["collect_notebook_definitions", "collect_job_instances"]

    level2_names = [c.args[0] for c in activity_calls[7:9]]
    assert level2_names == ["collect_livy_sessions", "collect_livy_statements"]

    assert result["workspace_id"] == "ws-1"
    assert result["level0_count"] == 1 + 2 + 3 + 4 + 5
    assert result["level1_count"] == 6 + 7
    assert result["level2_count"] == 8 + 9


def test_notebook_ids_propagate_to_level1(mock_durable_context):
    ctx = mock_durable_context
    ctx.call_activity.side_effect = [
        {"rows_written": 0},
        {"rows_written": 0, "notebook_ids": ["n1", "n2"]},
        {"rows_written": 0},
        {"rows_written": 0},
        {"rows_written": 0},
        {"rows_written": 0},
        {"rows_written": 0, "livy_session_ids": []},
        {"rows_written": 0},
        {"rows_written": 0},
    ]
    ctx.get_input.return_value = _base_input()

    _drive(workspace_sub_orchestrator(ctx))

    activity_calls = ctx.call_activity.call_args_list
    level1_input_defs = activity_calls[5].args[1]
    level1_input_jobs = activity_calls[6].args[1]
    assert level1_input_defs["notebook_ids"] == ["n1", "n2"]
    assert level1_input_jobs["notebook_ids"] == ["n1", "n2"]


def test_livy_session_ids_propagate_to_level2(mock_durable_context):
    ctx = mock_durable_context
    ctx.call_activity.side_effect = [
        {"rows_written": 0},
        {"rows_written": 0, "notebook_ids": ["n1"]},
        {"rows_written": 0},
        {"rows_written": 0},
        {"rows_written": 0},
        {"rows_written": 0},
        {"rows_written": 0, "livy_session_ids": ["l1", "l2", "l3"]},
        {"rows_written": 0},
        {"rows_written": 0},
    ]
    ctx.get_input.return_value = _base_input()

    _drive(workspace_sub_orchestrator(ctx))

    activity_calls = ctx.call_activity.call_args_list
    level2_input_sessions = activity_calls[7].args[1]
    level2_input_statements = activity_calls[8].args[1]
    assert level2_input_sessions["livy_session_ids"] == ["l1", "l2", "l3"]
    assert level2_input_statements["livy_session_ids"] == ["l1", "l2", "l3"]


def test_defensive_missing_notebook_ids_defaults_empty(mock_durable_context):
    ctx = mock_durable_context
    ctx.call_activity.side_effect = [
        {"rows_written": 0},
        {"rows_written": 0},
        {"rows_written": 0},
        {"rows_written": 0},
        {"rows_written": 0},
        {"rows_written": 0},
        {"rows_written": 0},
        {"rows_written": 0},
        {"rows_written": 0},
    ]
    ctx.get_input.return_value = _base_input()

    result = _drive(workspace_sub_orchestrator(ctx))

    activity_calls = ctx.call_activity.call_args_list
    assert activity_calls[5].args[1]["notebook_ids"] == []
    assert activity_calls[6].args[1]["notebook_ids"] == []
    assert activity_calls[7].args[1]["livy_session_ids"] == []
    assert activity_calls[8].args[1]["livy_session_ids"] == []
    assert result["workspace_id"] == "ws-1"
