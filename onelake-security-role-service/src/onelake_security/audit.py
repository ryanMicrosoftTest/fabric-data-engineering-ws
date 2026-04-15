"""Audit logging — records all role and mapping operations.

Collects AuditRecords during a pipeline run and flushes them to a
pluggable writer (e.g., SQL Database, lakehouse table, or stdout).
The writer is injected — this module has no I/O dependencies itself.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Callable, Optional


@dataclass
class AuditRecord:
    """A single auditable operation."""

    operation: str                          # ROLE_CREATED, ROLE_DELETED, MAPPING_APPLIED, etc.
    role_name: str
    workspace_id: str
    item_id: str
    source_file: Optional[str] = None
    content_hash: Optional[str] = None
    success: bool = True
    error: Optional[str] = None
    correlation_id: Optional[str] = None
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def to_dict(self) -> dict:
        """Serialize to a dict for SQL insertion or JSON output."""
        return {
            "operation": self.operation,
            "role_name": self.role_name,
            "workspace_id": self.workspace_id,
            "item_id": self.item_id,
            "source_file": self.source_file,
            "content_hash": self.content_hash,
            "success": self.success,
            "error": self.error,
            "correlation_id": self.correlation_id,
            "timestamp": self.timestamp.isoformat(),
        }


# Type alias for the writer function
AuditWriter = Callable[[list[AuditRecord]], None]


class AuditLogger:
    """Collects audit records and flushes them to a pluggable backend.

    Args:
        writer: Callable that receives a list of AuditRecords.
                If None, flush() just clears the buffer (useful for testing).
        correlation_id: Pipeline run ID stamped on every record.
    """

    def __init__(
        self,
        writer: Optional[AuditWriter] = None,
        correlation_id: Optional[str] = None,
    ):
        self._writer = writer
        self._correlation_id = correlation_id
        self.records: list[AuditRecord] = []

    def log(
        self,
        operation: str,
        role_name: str,
        workspace_id: str,
        item_id: str,
        success: bool = True,
        source_file: Optional[str] = None,
        content_hash: Optional[str] = None,
        error: Optional[str] = None,
    ) -> None:
        """Record an operation."""
        self.records.append(
            AuditRecord(
                operation=operation,
                role_name=role_name,
                workspace_id=workspace_id,
                item_id=item_id,
                source_file=source_file,
                content_hash=content_hash,
                success=success,
                error=error,
                correlation_id=self._correlation_id,
            )
        )

    def flush(self) -> None:
        """Send all accumulated records to the writer, then clear the buffer."""
        if self.records and self._writer:
            self._writer(self.records)
        self.records = []

    def to_dicts(self) -> list[dict]:
        """Serialize all records for inspection or manual persistence."""
        return [r.to_dict() for r in self.records]
