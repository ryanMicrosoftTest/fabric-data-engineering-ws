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

