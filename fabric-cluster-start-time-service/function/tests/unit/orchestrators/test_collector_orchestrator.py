"""Tests for collector_orchestrator."""

from __future__ import annotations

import pytest

from orchestrators.collector_orchestrator import collector_orchestrator


def _drive(gen):
    """Drive a Durable orchestrator generator: send each yielded value back as the result."""
    sent = None
    try:
        while True:
            sent = gen.send(sent)
    except StopIteration as e:
        return e.value


@pytest.fixture
def patched_settings(monkeypatch, mock_settings):
    from shared import config as config_mod

    monkeypatch.setattr(config_mod, "get_settings", lambda: mock_settings)
    import orchestrators.collector_orchestrator as co_mod

    monkeypatch.setattr(co_mod, "get_settings", lambda: mock_settings)
    return mock_settings


def test_happy_path_two_workspaces_from_settings(mock_durable_context, patched_settings):
    ctx = mock_durable_context
    sub_orch_returns = ["sub-result-1", "sub-result-2", "merge-result"]
    ctx.call_sub_orchestrator.side_effect = sub_orch_returns

    ctx.get_input.return_value = {
        "collector_run_id": "run-123",
        "trigger": "timer",
        "from_watermark": None,
        "to_watermark": None,
    }
    gen = collector_orchestrator(ctx)

    result = _drive(gen)

    assert result == {"workspaces_processed": 2, "merge_complete": True}

    sub_calls = ctx.call_sub_orchestrator.call_args_list
    assert len(sub_calls) == 3
    assert sub_calls[0].args[0] == "workspace_sub_orchestrator"
    assert sub_calls[1].args[0] == "workspace_sub_orchestrator"
    assert sub_calls[2].args[0] == "merge_orchestrator"
    assert sub_calls[2].args[1] == {"collector_run_id": "run-123"}


def test_workspace_ids_in_input_overrides_settings(mock_durable_context, patched_settings):
    ctx = mock_durable_context
    ctx.call_sub_orchestrator.side_effect = ["a", "b", "c", "merge"]

    ctx.get_input.return_value = {
        "collector_run_id": "run-xyz",
        "trigger": "http",
        "from_watermark": None,
        "to_watermark": None,
        "workspace_ids": ["explicit-1", "explicit-2", "explicit-3"],
    }
    gen = collector_orchestrator(ctx)

    result = _drive(gen)

    assert result == {"workspaces_processed": 3, "merge_complete": True}

    sub_calls = ctx.call_sub_orchestrator.call_args_list
    workspace_calls = [c for c in sub_calls if c.args[0] == "workspace_sub_orchestrator"]
    assert len(workspace_calls) == 3
    sent_ids = [c.args[1]["workspace_id"] for c in workspace_calls]
    assert sent_ids == ["explicit-1", "explicit-2", "explicit-3"]


def test_empty_workspace_ids_in_input_returns_empty(mock_durable_context, patched_settings):
    ctx = mock_durable_context

    ctx.get_input.return_value = {
        "collector_run_id": "run-empty",
        "trigger": "http",
        "from_watermark": None,
        "to_watermark": None,
        "workspace_ids": [],
    }
    gen = collector_orchestrator(ctx)

    result = _drive(gen)

    assert result == {"workspaces_processed": 0, "merge_complete": False}
    assert ctx.call_sub_orchestrator.call_count == 0


def test_to_watermark_defaults_to_current_utc_datetime(mock_durable_context, patched_settings):
    ctx = mock_durable_context
    ctx.call_sub_orchestrator.side_effect = ["s1", "s2", "merge"]

    ctx.get_input.return_value = {
        "collector_run_id": "run-tw",
        "trigger": "timer",
        "from_watermark": None,
        "to_watermark": None,
    }
    gen = collector_orchestrator(ctx)

    _drive(gen)

    workspace_calls = [c for c in ctx.call_sub_orchestrator.call_args_list if c.args[0] == "workspace_sub_orchestrator"]
    expected_iso = ctx.current_utc_datetime.isoformat()
    for c in workspace_calls:
        payload = c.args[1]
        assert payload["to_watermark"].startswith(expected_iso[:19])


def test_instance_id_includes_workspace(mock_durable_context, patched_settings):
    ctx = mock_durable_context
    ctx.call_sub_orchestrator.side_effect = ["s1", "s2", "merge"]

    ctx.get_input.return_value = {
        "collector_run_id": "run-iid",
        "workspace_ids": ["ws-A", "ws-B"],
    }
    gen = collector_orchestrator(ctx)
    _drive(gen)

    workspace_calls = [c for c in ctx.call_sub_orchestrator.call_args_list if c.args[0] == "workspace_sub_orchestrator"]
    assert workspace_calls[0].kwargs.get("instance_id") == "run-iid-ws-ws-A"
    assert workspace_calls[1].kwargs.get("instance_id") == "run-iid-ws-ws-B"
