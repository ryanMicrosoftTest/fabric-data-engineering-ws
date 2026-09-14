"""Self-contained Fabric notebook operator for Fabric Airflow recovery.

This module intentionally depends only on Airflow and runtime-provided msal, pyodbc,
requests, and Python standard-library modules.
"""

from __future__ import annotations

import struct
from dataclasses import dataclass
from typing import Any, Sequence

import msal
import pyodbc
from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook

FABRIC_SQL_SCOPE = "https://database.windows.net/.default"
SQL_COPT_SS_ACCESS_TOKEN = 1256
ALLOWED_MEMORY = frozenset({"28g", "56g", "112g", "224g", "400g"})
ALLOWED_CORES = frozenset({4, 8, 16, 32, 64})


@dataclass(frozen=True, slots=True)
class ComputeConfiguration:
    driver_memory: str
    driver_cores: int
    executor_memory: str
    executor_cores: int
    num_executors: int

    def __post_init__(self) -> None:
        if self.driver_memory not in ALLOWED_MEMORY:
            raise AirflowException(f"Invalid driver memory value: {self.driver_memory!r}")
        if self.executor_memory not in ALLOWED_MEMORY:
            raise AirflowException(f"Invalid executor memory value: {self.executor_memory!r}")
        if self.driver_cores not in ALLOWED_CORES:
            raise AirflowException(f"Invalid driver core value: {self.driver_cores!r}")
        if self.executor_cores not in ALLOWED_CORES:
            raise AirflowException(f"Invalid executor core value: {self.executor_cores!r}")
        if not isinstance(self.num_executors, int) or isinstance(self.num_executors, bool):
            raise AirflowException("num_executors must be an integer")
        if self.num_executors < 1:
            raise AirflowException("num_executors must be at least 1")

    def to_api_dict(self) -> dict[str, str | int]:
        return {
            "driverMemory": self.driver_memory,
            "driverCores": self.driver_cores,
            "executorMemory": self.executor_memory,
            "executorCores": self.executor_cores,
            "numExecutors": self.num_executors,
        }


class FabricSqlConfigHook(BaseHook):
    conn_name_attr = "sql_conn_id"
    default_conn_name = "airflow_config_sql"
    conn_type = "fabric_sql_config"
    hook_name = "Fabric SQL notebook compute configuration"

    _QUERY = """
        SELECT is_enabled, driver_memory, driver_cores,
               executor_memory, executor_cores, num_executors
        FROM dbo.notebook_compute_configuration
        WHERE notebook_id = CAST(? AS uniqueidentifier)
    """

    def __init__(
        self,
        sql_conn_id: str = default_conn_name,
        *,
        connection_timeout: int = 15,
        command_timeout: int = 30,
    ) -> None:
        super().__init__()
        self.sql_conn_id = sql_conn_id
        self.connection_timeout = connection_timeout
        self.command_timeout = command_timeout

    @staticmethod
    def _credential_parts(connection: Any) -> tuple[str, str, str]:
        extra = connection.extra_dejson
        tenant_id = extra.get("tenantId") or extra.get("tenant_id")
        client_id = connection.login or extra.get("clientId") or extra.get("client_id")
        client_secret = connection.password or extra.get("clientSecret") or extra.get("client_secret")
        if not all((tenant_id, client_id, client_secret)):
            raise AirflowException(
                "The SQL connection must provide tenant ID, client ID, and client secret"
            )
        return str(tenant_id), str(client_id), str(client_secret)

    @staticmethod
    def _acquire_token(tenant_id: str, client_id: str, client_secret: str, scope: str) -> str:
        app = msal.ConfidentialClientApplication(
            client_id,
            authority=f"https://login.microsoftonline.com/{tenant_id}",
            client_credential=client_secret,
        )
        result = app.acquire_token_for_client(scopes=[scope])
        token = result.get("access_token")
        if not token:
            correlation_id = result.get("correlation_id", "unavailable")
            raise AirflowException(
                f"Unable to acquire SQL access token (correlation ID: {correlation_id})"
            )
        return str(token)

    def get_conn(self) -> pyodbc.Connection:
        connection = self.get_connection(self.sql_conn_id)
        if not connection.host or not connection.schema:
            raise AirflowException("The SQL connection must provide host and database (schema)")
        tenant_id, client_id, client_secret = self._credential_parts(connection)
        token_scope = connection.extra_dejson.get("token_scope", FABRIC_SQL_SCOPE)
        if token_scope != FABRIC_SQL_SCOPE:
            raise AirflowException(
                f"SQL token_scope must use the SQL audience {FABRIC_SQL_SCOPE}"
            )
        token = self._acquire_token(tenant_id, client_id, client_secret, token_scope)
        token_bytes = token.encode("utf-16-le")
        token_struct = struct.pack(f"<I{len(token_bytes)}s", len(token_bytes), token_bytes)
        driver = connection.extra_dejson.get("driver", "ODBC Driver 18 for SQL Server")
        connection_string = (
            f"DRIVER={{{driver}}};SERVER={connection.host};DATABASE={connection.schema};"
            f"Encrypt=yes;TrustServerCertificate=no;Connection Timeout={self.connection_timeout}"
        )
        return pyodbc.connect(
            connection_string,
            attrs_before={SQL_COPT_SS_ACCESS_TOKEN: token_struct},
            timeout=self.connection_timeout,
        )

    def get_compute_configuration(
        self, notebook_id: str
    ) -> ComputeConfiguration | None:
        connection = self.get_conn()
        try:
            cursor = connection.cursor()
            try:
                cursor.timeout = self.command_timeout
                cursor.execute(self._QUERY, notebook_id)
                row: Sequence[Any] | None = cursor.fetchone()
            finally:
                cursor.close()
        finally:
            connection.close()

        if row is None:
            self.log.warning(
                "No compute configuration exists for notebook %s; using Fabric defaults",
                notebook_id,
            )
            return None
        if not bool(row[0]):
            self.log.warning(
                "Compute configuration is disabled for notebook %s; using Fabric defaults",
                notebook_id,
            )
            return None
        if any(value is None for value in row[1:6]):
            raise AirflowException(
                f"Enabled compute configuration for notebook {notebook_id} is incomplete"
            )
        return ComputeConfiguration(
            driver_memory=row[1],
            driver_cores=row[2],
            executor_memory=row[3],
            executor_cores=row[4],
            num_executors=row[5],
        )


import json
import re
import time
from dataclasses import dataclass
from typing import Any
from urllib.parse import unquote, urlparse
from uuid import UUID

import msal
import requests
from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook


FABRIC_API_SCOPE = "https://api.fabric.microsoft.com/.default"
DEFAULT_FABRIC_API = "https://api.fabric.microsoft.com"
SUCCESS_STATES = frozenset({"COMPLETED", "SUCCEEDED", "SUCCESS"})
FAILURE_STATES = frozenset({"FAILED", "CANCELLED", "CANCELED", "DEDUPED"})
_CONTROL_CHARACTERS = re.compile(r"[\x00-\x1f\x7f-\x9f]")
_SENSITIVE_VALUE = re.compile(
    r"(?i)\b(authorization|bearer|token|secret|password|api[_ -]?key)"
    r"(\s*[:=]\s*|\s+)([^\s,;}\]]+)"
)
_SENSITIVE_KEY = re.compile(
    r"(?i)(authorization|token|secret|password|api[_ -]?key)"
)


def _redact_sensitive_values(value: Any) -> Any:
    if isinstance(value, dict):
        return {
            key: (
                "[REDACTED]"
                if _SENSITIVE_KEY.search(str(key))
                else _redact_sensitive_values(item)
            )
            for key, item in value.items()
        }
    if isinstance(value, (list, tuple)):
        return [_redact_sensitive_values(item) for item in value]
    return value


@dataclass(frozen=True, slots=True)
class FabricJobSubmission:
    job_instance_id: str
    location: str
    request_id: str | None
    retry_after: float


class FabricJobTimeout(AirflowException):
    """Raised when a submitted Fabric job does not finish before its deadline."""


class FabricNotebookHook(BaseHook):
    conn_name_attr = "fabric_conn_id"
    default_conn_name = "fabric_conn"
    conn_type = "fabric"
    hook_name = "Microsoft Fabric notebook"

    def __init__(
        self,
        fabric_conn_id: str = default_conn_name,
        *,
        request_timeout: int = 30,
        min_poll_interval: float = 2,
        max_poll_interval: float = 60,
        session: requests.Session | None = None,
    ) -> None:
        super().__init__()
        self.fabric_conn_id = fabric_conn_id
        self.request_timeout = request_timeout
        self.min_poll_interval = min_poll_interval
        self.max_poll_interval = max_poll_interval
        self._session = session or requests.Session()
        self._access_token: str | None = None

    def _connection_settings(self) -> tuple[str, str, str, str]:
        connection = self.get_connection(self.fabric_conn_id)
        extra = connection.extra_dejson
        endpoint = (connection.host or extra.get("endpoint") or DEFAULT_FABRIC_API).rstrip("/")
        tenant_id = extra.get("tenantId") or extra.get("tenant_id")
        client_id = connection.login or extra.get("clientId") or extra.get("client_id")
        client_secret = connection.password or extra.get("clientSecret") or extra.get("client_secret")
        if not all((tenant_id, client_id, client_secret)):
            raise AirflowException(
                "The Fabric connection must provide tenant ID, client ID, and client secret"
            )
        if urlparse(endpoint).scheme != "https":
            raise AirflowException("The Fabric API endpoint must use HTTPS")
        return endpoint, str(tenant_id), str(client_id), str(client_secret)

    def _token(self) -> str:
        if self._access_token:
            return self._access_token
        _, tenant_id, client_id, client_secret = self._connection_settings()
        app = msal.ConfidentialClientApplication(
            client_id,
            authority=f"https://login.microsoftonline.com/{tenant_id}",
            client_credential=client_secret,
        )
        result = app.acquire_token_for_client(scopes=[FABRIC_API_SCOPE])
        token = result.get("access_token")
        if not token:
            correlation_id = result.get("correlation_id", "unavailable")
            raise AirflowException(
                f"Unable to acquire Fabric API token (correlation ID: {correlation_id})"
            )
        self._access_token = str(token)
        return self._access_token

    def _headers(self) -> dict[str, str]:
        return {
            "Authorization": f"Bearer {self._token()}",
            "Content-Type": "application/json",
        }

    @staticmethod
    def _validate_uuid(value: str, label: str) -> None:
        try:
            UUID(value)
        except (TypeError, ValueError, AttributeError) as exc:
            raise AirflowException(f"{label} must be a UUID") from exc

    @staticmethod
    def _validate_parameters(parameters: list[dict[str, Any]] | None) -> None:
        if parameters is None:
            return
        if not isinstance(parameters, list):
            raise AirflowException("Notebook parameters must be a list")
        for parameter in parameters:
            if not isinstance(parameter, dict):
                raise AirflowException("Each notebook parameter must be an object")
            if not isinstance(parameter.get("name"), str) or not parameter["name"]:
                raise AirflowException("Each notebook parameter requires a nonempty name")
            if not isinstance(parameter.get("type"), str) or not parameter["type"]:
                raise AirflowException("Each notebook parameter requires a nonempty type")
            if "value" not in parameter:
                raise AirflowException("Each notebook parameter requires a value")

    @staticmethod
    def _retry_after(response: requests.Response, default: float) -> float:
        try:
            return max(0.0, float(response.headers.get("Retry-After", default)))
        except (TypeError, ValueError):
            return default

    @staticmethod
    def _request_id(response: requests.Response) -> str | None:
        return (
            response.headers.get("x-ms-request-id")
            or response.headers.get("request-id")
            or response.headers.get("x-ms-correlation-request-id")
        )

    @staticmethod
    def _job_id_from_location(location: str) -> str:
        parsed = urlparse(location)
        path_parts = [unquote(part) for part in parsed.path.split("/") if part]
        if not path_parts:
            raise AirflowException(
                "Fabric submission Location header did not contain a job instance UUID"
            )
        candidate = path_parts[-1]
        try:
            return str(UUID(candidate))
        except (TypeError, ValueError, AttributeError) as exc:
            raise AirflowException(
                "Fabric submission Location header did not contain a valid job instance UUID"
            ) from exc

    @staticmethod
    def _sanitize_failure_detail(value: Any) -> str:
        if value is None:
            return "unavailable"
        value = _redact_sensitive_values(value)
        if isinstance(value, str):
            rendered = value
        else:
            try:
                rendered = json.dumps(value, ensure_ascii=True, sort_keys=True)
            except (TypeError, ValueError):
                rendered = repr(value)
        rendered = _CONTROL_CHARACTERS.sub(" ", rendered)
        rendered = _SENSITIVE_VALUE.sub(r"\1\2[REDACTED]", rendered)
        rendered = " ".join(rendered.split())
        if len(rendered) > 500:
            rendered = f"{rendered[:497]}..."
        return rendered or "unavailable"

    def submit_notebook(
        self,
        workspace_id: str,
        notebook_id: str,
        compute_configuration: ComputeConfiguration | None,
        parameters: list[dict[str, Any]] | None = None,
    ) -> FabricJobSubmission:
        self._validate_uuid(workspace_id, "workspace_id")
        self._validate_uuid(notebook_id, "notebook_id")
        self._validate_parameters(parameters)
        endpoint, _, _, _ = self._connection_settings()
        url = (
            f"{endpoint}/v1/workspaces/{workspace_id}/notebooks/{notebook_id}"
            "/jobs/execute/instances?beta=false"
        )
        body: dict[str, Any] = {"executionData": {"compute": "Spark"}}
        if compute_configuration is not None:
            body["executionData"]["computeConfiguration"] = (
                compute_configuration.to_api_dict()
            )
        if parameters is not None:
            body["parameters"] = parameters

        response = self._session.post(
            url,
            headers=self._headers(),
            json=body,
            timeout=self.request_timeout,
        )
        if response.status_code != 202:
            raise AirflowException(
                "Fabric notebook submission failed "
                f"(HTTP {response.status_code}, request ID: {self._request_id(response) or 'unavailable'})"
            )
        location = response.headers.get("Location")
        if not location:
            raise AirflowException("Fabric accepted the notebook but omitted the Location header")
        job_instance_id = self._job_id_from_location(location)
        return FabricJobSubmission(
            job_instance_id=job_instance_id,
            location=location,
            request_id=self._request_id(response),
            retry_after=self._retry_after(response, self.min_poll_interval),
        )

    @staticmethod
    def _status_payload(response: requests.Response) -> tuple[str, dict[str, Any]]:
        try:
            payload = response.json()
        except ValueError as exc:
            raise AirflowException("Fabric job status response was not valid JSON") from exc
        if not isinstance(payload, dict):
            raise AirflowException("Fabric job status response was not an object")
        status = payload.get("status") or payload.get("state")
        if not isinstance(status, str):
            raise AirflowException("Fabric job status response omitted status")
        return status.upper(), payload

    @staticmethod
    def _monitoring_evidence(payload: dict[str, Any]) -> dict[str, Any]:
        allowed = {
            "startTimeUtc",
            "endTimeUtc",
            "startTime",
            "endTime",
            "rootActivityId",
            "sparkApplicationId",
            "applicationId",
            "exitValue",
        }
        return {
            key: _redact_sensitive_values(value)
            for key, value in payload.items()
            if key in allowed and value is not None
        }

    def wait_for_completion(
        self,
        submission: FabricJobSubmission,
        timeout_seconds: float,
    ) -> dict[str, Any]:
        if timeout_seconds <= 0:
            raise AirflowException("Fabric notebook timeout must be positive")
        deadline = time.monotonic() + timeout_seconds
        delay = max(self.min_poll_interval, submission.retry_after)
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= delay:
                raise FabricJobTimeout(
                    f"Fabric notebook job {submission.job_instance_id} timed out"
                )
            time.sleep(delay)
            response = self._session.get(
                submission.location,
                headers=self._headers(),
                timeout=self.request_timeout,
            )
            if response.status_code == 429:
                delay = max(
                    self.min_poll_interval,
                    self._retry_after(response, min(delay * 2, self.max_poll_interval)),
                )
                continue
            if response.status_code != 200:
                raise AirflowException(
                    "Fabric job polling failed "
                    f"(HTTP {response.status_code}, request ID: {self._request_id(response) or 'unavailable'})"
                )
            status, payload = self._status_payload(response)
            if status in SUCCESS_STATES:
                return {
                    "job_instance_id": submission.job_instance_id,
                    "request_id": submission.request_id,
                    "status": status,
                    "monitoring": self._monitoring_evidence(payload),
                }
            if status in FAILURE_STATES:
                raise AirflowException(
                    f"Fabric notebook job {submission.job_instance_id} ended in {status} "
                    f"(failureReason: {self._sanitize_failure_detail(payload.get('failureReason'))}; "
                    f"exitValue: {self._sanitize_failure_detail(payload.get('exitValue'))})"
                )
            delay = min(
                self.max_poll_interval,
                max(self.min_poll_interval, delay * 2),
            )

    def cancel(
        self,
        workspace_id: str,
        notebook_id: str,
        job_instance_id: str,
    ) -> None:
        self._validate_uuid(workspace_id, "workspace_id")
        self._validate_uuid(notebook_id, "notebook_id")
        self._validate_uuid(job_instance_id, "job_instance_id")
        endpoint, _, _, _ = self._connection_settings()
        url = (
            f"{endpoint}/v1/workspaces/{workspace_id}/items/{notebook_id}"
            f"/jobs/instances/{job_instance_id}/cancel"
        )
        response = self._session.post(
            url,
            headers=self._headers(),
            timeout=self.request_timeout,
        )
        if response.status_code != 202:
            raise AirflowException(
                "Fabric notebook cancellation failed "
                f"(HTTP {response.status_code}, request ID: {self._request_id(response) or 'unavailable'})"
            )


from typing import Any

from airflow.exceptions import AirflowException
from airflow.models.baseoperator import BaseOperator



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
