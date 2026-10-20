"""Configuration loading for the on-prem-to-fabric-wh pipeline.

Loads required settings from environment variables (optionally hydrated from a
``.env`` file located at the project root). The :class:`Config` dataclass holds
non-secret connection metadata; the SPN client secret itself is fetched at
runtime by ``secrets.py`` from Azure Key Vault and is intentionally not stored
on this object.
"""

from __future__ import annotations

import logging
import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import ClassVar, Pattern

from dotenv import load_dotenv

logger = logging.getLogger(__name__)

_ENV_PATH: Path = Path(__file__).resolve().parent.parent / ".env"

_GUID_RE: Pattern[str] = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$",
    re.IGNORECASE,
)

_FABRIC_DW_SUFFIX: str = ".datawarehouse.fabric.microsoft.com"


@dataclass(frozen=True)
class Config:
    """Immutable configuration for the on-prem-to-Fabric warehouse pipeline.

    All fields are non-secret. The SPN client secret is fetched dynamically
    from Azure Key Vault by ``secrets.py`` and must never be added here.
    """

    azure_tenant_id: str
    spn_client_id: str
    kv_name: str
    kv_secret_name: str
    warehouse_server: str
    warehouse_database: str
    warehouse_schema: str = "dbo"
    warehouse_table: str = "synthetic_orders"

    _REQUIRED_ENV_VARS: ClassVar[dict[str, str]] = {
        "azure_tenant_id": "AZURE_TENANT_ID",
        "spn_client_id": "SPN_CLIENT_ID",
        "kv_name": "KV_NAME",
        "kv_secret_name": "KV_SECRET_NAME",
        "warehouse_server": "WAREHOUSE_SERVER",
        "warehouse_database": "WAREHOUSE_DATABASE",
    }

    _OPTIONAL_ENV_VARS: ClassVar[dict[str, str]] = {
        "warehouse_schema": "WAREHOUSE_SCHEMA",
        "warehouse_table": "WAREHOUSE_TABLE",
    }

    @classmethod
    def from_env(cls) -> "Config":
        """Build a :class:`Config` from environment variables.

        Loads ``.env`` from the project root (parent of ``src/``) if present;
        otherwise relies on existing OS environment variables. Raises
        :class:`ValueError` listing *all* missing required vars at once.
        """
        load_dotenv(dotenv_path=_ENV_PATH)

        values: dict[str, str] = {}
        missing: list[str] = []

        for attr, env_name in cls._REQUIRED_ENV_VARS.items():
            raw = os.getenv(env_name)
            if raw is None or raw.strip() == "":
                missing.append(env_name)
            else:
                values[attr] = raw

        if missing:
            raise ValueError(
                "Missing required environment variables: " + ", ".join(missing)
            )

        for attr, env_name in cls._OPTIONAL_ENV_VARS.items():
            raw = os.getenv(env_name)
            if raw is not None and raw.strip() != "":
                values[attr] = raw

        cfg = cls(**values)
        cfg._validate()
        return cfg

    def _validate(self) -> None:
        """Run non-fatal sanity checks; warnings only, never raises."""
        if _GUID_RE.match(self.warehouse_database):
            logger.warning(
                "WAREHOUSE_DATABASE looks like a GUID (%s); expected the "
                "warehouse display name, not its workspace/item ID.",
                self.warehouse_database,
            )
        if not self.warehouse_server.endswith(_FABRIC_DW_SUFFIX):
            logger.warning(
                "WAREHOUSE_SERVER (%s) does not end with %s; double-check "
                "this is a Fabric Warehouse SQL connection string.",
                self.warehouse_server,
                _FABRIC_DW_SUFFIX,
            )

    def __repr__(self) -> str:
        return (
            f"{type(self).__name__}("
            f"azure_tenant_id={self.azure_tenant_id!r}, "
            f"spn_client_id={self.spn_client_id!r}, "
            f"kv_name={self.kv_name!r}, "
            f"kv_secret_name={self.kv_secret_name!r}, "
            f"warehouse_server={self.warehouse_server!r}, "
            f"warehouse_database={self.warehouse_database!r}, "
            f"warehouse_schema={self.warehouse_schema!r}, "
            f"warehouse_table={self.warehouse_table!r})"
        )
