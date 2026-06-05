from datetime import date, datetime, timezone

import pytest
from pydantic import ValidationError

from shared.models import (
    ActivityResult,
    BackfillRequest,
    CollectorRunContext,
    WorkspaceCollectInput,
)


def test_backfill_request_happy_path():
    r = BackfillRequest(from_date=date(2024, 1, 1), to_date=date(2024, 1, 5))
    assert r.workspace_ids is None


def test_backfill_request_inverted_dates():
    with pytest.raises(ValidationError):
        BackfillRequest(from_date=date(2024, 2, 1), to_date=date(2024, 1, 1))


def test_backfill_request_too_long():
    with pytest.raises(ValidationError):
        BackfillRequest(from_date=date(2024, 1, 1), to_date=date(2024, 6, 1))


def test_backfill_request_at_max_span():
    r = BackfillRequest(from_date=date(2024, 1, 1), to_date=date(2024, 3, 31))
    assert (r.to_date - r.from_date).days == 90


def test_backfill_request_with_workspaces():
    r = BackfillRequest(
        from_date=date(2024, 1, 1),
        to_date=date(2024, 1, 2),
        workspace_ids=["ws-1", "ws-2"],
    )
    assert r.workspace_ids == ["ws-1", "ws-2"]


def test_collector_run_context():
    ctx = CollectorRunContext(
        collector_run_id="cr-1",
        run_started_at=datetime(2024, 1, 1, tzinfo=timezone.utc),
        target_workspace_ids=["ws-1"],
        from_watermark=None,
        to_watermark=datetime(2024, 1, 2, tzinfo=timezone.utc),
    )
    assert ctx.collector_run_id == "cr-1"


def test_workspace_collect_input():
    inp = WorkspaceCollectInput(
        collector_run_id="cr-1",
        workspace_id="ws-1",
        to_watermark=datetime(2024, 1, 2, tzinfo=timezone.utc),
    )
    assert inp.from_watermark is None


def test_activity_result_defaults():
    r = ActivityResult(success=True)
    assert r.rows_written == 0
    assert r.error is None
    assert r.duration_ms == 0


def test_activity_result_negative_duration_rejected():
    with pytest.raises(ValidationError):
        ActivityResult(success=False, duration_ms=-1)
