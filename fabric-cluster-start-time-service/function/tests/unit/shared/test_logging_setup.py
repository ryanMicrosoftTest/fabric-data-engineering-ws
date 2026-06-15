import logging

from shared.logging_setup import configure_logging


def test_configure_logging_returns_logger():
    log = configure_logging("cr-123")
    assert log is not None
    # LoggerAdapter has a `logger` attribute
    assert hasattr(log, "logger")


def test_configure_logging_idempotent():
    a = configure_logging("cr-1")
    b = configure_logging("cr-2")
    assert a is not b  # different adapters
    assert a.logger is b.logger  # same underlying logger


def test_collector_run_id_in_extra(caplog):
    log = configure_logging("cr-xyz")
    base_logger = log.logger
    base_logger.setLevel(logging.INFO)
    with caplog.at_level(logging.INFO, logger=base_logger.name):
        log.info("hello")
    assert any(getattr(rec, "collector_run_id", None) == "cr-xyz" for rec in caplog.records)


def test_no_collector_run_id_when_omitted(caplog):
    log = configure_logging(None)
    base_logger = log.logger
    base_logger.setLevel(logging.INFO)
    with caplog.at_level(logging.INFO, logger=base_logger.name):
        log.info("hi")
    # When no run id given, the extra dict is empty -> attribute should be absent
    for rec in caplog.records:
        assert getattr(rec, "collector_run_id", None) in (None,)
