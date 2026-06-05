from datetime import date, datetime

from pydantic import BaseModel, Field, model_validator


class BackfillRequest(BaseModel):
    from_date: date
    to_date: date
    workspace_ids: list[str] | None = None

    @model_validator(mode="after")
    def _check_date_range(self) -> "BackfillRequest":
        if self.from_date > self.to_date:
            raise ValueError("from_date must be <= to_date")
        if (self.to_date - self.from_date).days > 90:
            raise ValueError("date span must be <= 90 days")
        return self


class CollectorRunContext(BaseModel):
    collector_run_id: str
    run_started_at: datetime
    target_workspace_ids: list[str]
    from_watermark: datetime | None = None
    to_watermark: datetime


class WorkspaceCollectInput(BaseModel):
    collector_run_id: str
    workspace_id: str
    from_watermark: datetime | None = None
    to_watermark: datetime


class ActivityResult(BaseModel):
    success: bool
    rows_written: int = 0
    error: str | None = None
    duration_ms: int = Field(default=0, ge=0)
