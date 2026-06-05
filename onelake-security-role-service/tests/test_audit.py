"""Tests for audit logging — records operations to a SQL-compatible backend."""

import pytest
from unittest.mock import MagicMock
from datetime import datetime, timezone
from onelake_security.audit import AuditLogger, AuditRecord


class TestAuditRecord:
    """AuditRecord captures a single operation."""

    def test_role_operation_record(self):
        record = AuditRecord(
            operation="ROLE_CREATED",
            role_name="NeurologyReadRole",
            workspace_id="ws-123",
            item_id="item-456",
            source_file="neurology-read.yml",
            content_hash="abc123",
            success=True,
        )
        assert record.operation == "ROLE_CREATED"
        assert record.role_name == "NeurologyReadRole"
        assert record.success is True
        assert record.error is None

    def test_failed_operation_record(self):
        record = AuditRecord(
            operation="ROLE_CREATION_FAILED",
            role_name="BadRole",
            workspace_id="ws",
            item_id="item",
            source_file="bad.yml",
            success=False,
            error="Validation error: missing table path",
        )
        assert record.success is False
        assert "Validation" in record.error

    def test_timestamp_auto_set(self):
        record = AuditRecord(
            operation="ROLE_CREATED",
            role_name="R",
            workspace_id="ws",
            item_id="item",
        )
        assert record.timestamp is not None
        assert isinstance(record.timestamp, datetime)

    def test_correlation_id_accepted(self):
        record = AuditRecord(
            operation="MAPPING_APPLIED",
            role_name="R",
            workspace_id="ws",
            item_id="item",
            correlation_id="run-abc-123",
        )
        assert record.correlation_id == "run-abc-123"


class TestAuditLogger:
    """AuditLogger collects records and writes them via a pluggable backend."""

    def test_log_adds_record(self):
        logger = AuditLogger()
        logger.log(
            operation="ROLE_CREATED",
            role_name="TestRole",
            workspace_id="ws",
            item_id="item",
            success=True,
        )
        assert len(logger.records) == 1
        assert logger.records[0].role_name == "TestRole"

    def test_log_multiple_records(self):
        logger = AuditLogger()
        logger.log(operation="ROLE_CREATED", role_name="R1",
                    workspace_id="ws", item_id="item", success=True)
        logger.log(operation="MAPPING_APPLIED", role_name="R1",
                    workspace_id="ws", item_id="item", success=True)
        assert len(logger.records) == 2

    def test_flush_calls_writer(self):
        """flush() sends all records to the provided writer function."""
        writer = MagicMock()
        logger = AuditLogger(writer=writer)
        logger.log(operation="ROLE_CREATED", role_name="R1",
                    workspace_id="ws", item_id="item", success=True)
        logger.log(operation="ROLE_DELETED", role_name="R2",
                    workspace_id="ws", item_id="item", success=True)

        logger.flush()

        writer.assert_called_once()
        records_arg = writer.call_args[0][0]
        assert len(records_arg) == 2

    def test_flush_clears_records(self):
        logger = AuditLogger(writer=MagicMock())
        logger.log(operation="ROLE_CREATED", role_name="R1",
                    workspace_id="ws", item_id="item", success=True)
        logger.flush()
        assert len(logger.records) == 0

    def test_flush_noop_when_empty(self):
        writer = MagicMock()
        logger = AuditLogger(writer=writer)
        logger.flush()
        writer.assert_not_called()

    def test_no_writer_stores_records_only(self):
        """Without a writer, logger just accumulates records in memory."""
        logger = AuditLogger()
        logger.log(operation="ROLE_CREATED", role_name="R1",
                    workspace_id="ws", item_id="item", success=True)
        logger.flush()  # should not raise
        assert len(logger.records) == 0  # still clears

    def test_correlation_id_propagated(self):
        logger = AuditLogger(correlation_id="run-xyz")
        logger.log(operation="ROLE_CREATED", role_name="R1",
                    workspace_id="ws", item_id="item", success=True)
        assert logger.records[0].correlation_id == "run-xyz"

    def test_to_dicts_returns_serializable(self):
        logger = AuditLogger()
        logger.log(operation="ROLE_CREATED", role_name="R1",
                    workspace_id="ws", item_id="item", success=True,
                    source_file="r1.yml", content_hash="h1")

        dicts = logger.to_dicts()
        assert len(dicts) == 1
        d = dicts[0]
        assert d["operation"] == "ROLE_CREATED"
        assert d["role_name"] == "R1"
        assert "timestamp" in d
