import logging
import os
from typing import Any, MutableMapping

_AZURE_MONITOR_CONFIGURED = False


def _configure_azure_monitor_once() -> None:
    global _AZURE_MONITOR_CONFIGURED
    if _AZURE_MONITOR_CONFIGURED:
        return
    conn = os.environ.get("APPLICATIONINSIGHTS_CONNECTION_STRING")
    if not conn:
        _AZURE_MONITOR_CONFIGURED = True
        return
    try:
        from azure.monitor.opentelemetry import configure_azure_monitor

        configure_azure_monitor(connection_string=conn)
    except Exception:  # noqa: BLE001
        logging.getLogger(__name__).warning("azure-monitor-opentelemetry not configured", exc_info=True)
    finally:
        _AZURE_MONITOR_CONFIGURED = True


class _CollectorRunIdAdapter(logging.LoggerAdapter):
    def process(self, msg: str, kwargs: MutableMapping[str, Any]) -> tuple[str, MutableMapping[str, Any]]:
        extra = kwargs.get("extra") or {}
        merged = {**(self.extra or {}), **extra}
        kwargs["extra"] = merged
        return msg, kwargs


def configure_logging(collector_run_id: str | None = None) -> logging.Logger:
    _configure_azure_monitor_once()
    base = logging.getLogger("fcsps")
    if not base.handlers:
        base.setLevel(logging.INFO)
    extra = {"collector_run_id": collector_run_id} if collector_run_id else {}
    return _CollectorRunIdAdapter(base, extra)  # type: ignore[return-value]
