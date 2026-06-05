from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import azure.durable_functions as df
import pytest

from triggers.timer_starter import timer_starter


def _timer(past_due: bool = False):
    t = MagicMock()
    t.past_due = past_due
    return t


def _status(runtime_status):
    s = MagicMock()
    s.runtime_status = runtime_status
    return s


@pytest.mark.asyncio
async def test_timer_starter_happy_path():
    client = MagicMock()
    client.get_status = AsyncMock(return_value=None)
    client.start_new = AsyncMock(return_value="daily-collector-20240115")

    await timer_starter(_timer(past_due=False), client)

    client.start_new.assert_awaited_once()
    args, _ = client.start_new.call_args
    assert args[0] == "collector_orchestrator"
    instance_id = args[1]
    payload = args[2]
    assert instance_id.startswith("daily-collector-")
    assert payload["trigger"] == "timer"
    assert payload["from_watermark"] is None
    assert payload["collector_run_id"] == instance_id


@pytest.mark.asyncio
async def test_timer_starter_skips_when_running(caplog):
    client = MagicMock()
    client.get_status = AsyncMock(return_value=_status(df.OrchestrationRuntimeStatus.Running))
    client.start_new = AsyncMock()

    with caplog.at_level("INFO", logger="fcsps"):
        await timer_starter(_timer(past_due=False), client)

    client.start_new.assert_not_called()
    assert any("Skipping" in r.message for r in caplog.records)


@pytest.mark.asyncio
async def test_timer_starter_past_due_logs_warning_and_proceeds(caplog):
    client = MagicMock()
    client.get_status = AsyncMock(return_value=None)
    client.start_new = AsyncMock(return_value="daily-collector-20240115")

    with caplog.at_level("WARNING", logger="fcsps"):
        await timer_starter(_timer(past_due=True), client)

    client.start_new.assert_awaited_once()
    assert any("past due" in r.message.lower() for r in caplog.records)


@pytest.mark.asyncio
async def test_timer_starter_fallback_when_start_new_raises(caplog):
    client = MagicMock()
    client.get_status = AsyncMock(return_value=None)
    client.start_new = AsyncMock(side_effect=[Exception("instance exists"), "fallback-id"])

    with caplog.at_level("WARNING", logger="fcsps"):
        await timer_starter(_timer(past_due=False), client)

    assert client.start_new.await_count == 2
    first_id = client.start_new.await_args_list[0].args[1]
    fallback_id = client.start_new.await_args_list[1].args[1]
    assert fallback_id != first_id
    assert fallback_id.startswith(first_id)
