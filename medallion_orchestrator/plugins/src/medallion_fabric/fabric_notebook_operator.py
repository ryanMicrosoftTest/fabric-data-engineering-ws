from __future__ import annotations

from typing import Any

from airflow.exceptions import AirflowException
from airflow.models.baseoperator import BaseOperator

from medallion_fabric.compute_config import FabricSqlConfigHook
from medallion_fabric.fabric_notebook_hook import (
    FabricJobSubmission,
    FabricJobTimeout,
    FabricNotebookHook,
)


class SqlConfiguredFabricNotebookOperator(BaseOperator):
    template_fields = ("workspace_id", "notebook_id", "parameters")

    def __init__(
        self,
        *,
        workspace_id: str,
        notebook_id: str,
        fabric_conn_id: str = "fabric_conn",
        sql_conn_id: str = "airflow_config_sql",
        parameters: list[dict[str, Any]] | None = None,
        timeout_seconds: float | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.workspace_id = workspace_id
        self.notebook_id = notebook_id
        self.fabric_conn_id = fabric_conn_id
        self.sql_conn_id = sql_conn_id
        self.parameters = parameters
        self.timeout_seconds = timeout_seconds
        self._fabric_hook: FabricNotebookHook | None = None
        self._submission: FabricJobSubmission | None = None

    def _effective_timeout(self) -> float:
        candidates = [
            float(value)
            for value in (
                self.timeout_seconds,
                self.execution_timeout.total_seconds()
                if self.execution_timeout is not None
                else None,
            )
            if value is not None
        ]
        timeout = min(candidates) if candidates else 3600.0
        if timeout <= 0:
            raise AirflowException("timeout_seconds and execution_timeout must be positive")
        return timeout

    def execute(self, context: dict[str, Any]) -> dict[str, Any]:
        configuration = FabricSqlConfigHook(
            sql_conn_id=self.sql_conn_id
        ).get_compute_configuration(self.notebook_id)
        self._fabric_hook = FabricNotebookHook(fabric_conn_id=self.fabric_conn_id)
        self._submission = self._fabric_hook.submit_notebook(
            self.workspace_id,
            self.notebook_id,
            configuration,
            self.parameters,
        )
        safe_metadata: dict[str, Any] = {
            "job_instance_id": self._submission.job_instance_id,
            "request_id": self._submission.request_id,
            "requested_compute_configuration": (
                configuration.to_api_dict() if configuration else None
            ),
        }
        task_instance = context.get("ti")
        if task_instance is not None:
            task_instance.xcom_push(key="fabric_notebook_submission", value=safe_metadata)

        try:
            result = self._fabric_hook.wait_for_completion(
                self._submission,
                self._effective_timeout(),
            )
        except FabricJobTimeout as timeout_error:
            try:
                self._fabric_hook.cancel(
                    self.workspace_id,
                    self.notebook_id,
                    self._submission.job_instance_id,
                )
            except Exception as cancellation_error:
                cancellation_message = (
                    "Cancellation after timeout also failed: "
                    f"{type(cancellation_error).__name__}: {cancellation_error}"
                )
                timeout_error.add_note(cancellation_message)
                self.log.error(cancellation_message)
                raise timeout_error from cancellation_error
            raise
        safe_metadata["status"] = result["status"]
        safe_metadata["monitoring"] = result.get("monitoring", {})
        if task_instance is not None:
            task_instance.xcom_push(key="fabric_notebook_result", value=safe_metadata)
        return safe_metadata

    def on_kill(self) -> None:
        if self._fabric_hook is None or self._submission is None:
            return
        self.log.warning(
            "Cancelling Fabric notebook job %s after Airflow task termination",
            self._submission.job_instance_id,
        )
        self._fabric_hook.cancel(
            self.workspace_id,
            self.notebook_id,
            self._submission.job_instance_id,
        )
