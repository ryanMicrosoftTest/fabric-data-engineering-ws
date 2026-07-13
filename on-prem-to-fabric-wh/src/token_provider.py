"""Azure AD token acquisition and ODBC packing for Fabric Warehouse connections.

This module provides utilities to acquire Microsoft Entra ID (Azure AD) access
tokens for Fabric Warehouse / Azure SQL endpoints and to pack them into the
binary structure required by the ODBC pre-connect attribute
``SQL_COPT_SS_ACCESS_TOKEN`` (1256).

The module is intentionally self-contained: it has no dependency on project
configuration or other internal modules so that it can be reused and unit
tested in isolation.
"""

from __future__ import annotations
import logging
import struct
from azure.identity import ClientSecretCredential
logger = logging.getLogger(__name__)

WAREHOUSE_TOKEN_SCOPE = "https://database.windows.net/.default"

# ODBC pre-connect attribute identifier for supplying an Azure AD access token
# to the Microsoft ODBC Driver for SQL Server. Passed via
# ``pyodbc.connect(..., attrs_before={SQL_COPT_SS_ACCESS_TOKEN: <packed bytes>})``.
SQL_COPT_SS_ACCESS_TOKEN = 1256


def pack_token_for_odbc(access_token: str) -> bytes:
    """Pack an Azure AD access token into the ODBC ``SQL_COPT_SS_ACCESS_TOKEN`` struct.

    The MS ODBC driver expects the token bytes encoded as UTF-16-LE, prefixed
    by a 4-byte little-endian unsigned int that holds the length (in bytes) of
    the encoded token.

    This function performs no I/O and is safe to call in any context.

    Args:
        access_token: The raw OAuth2 access token string.

    Returns:
        The packed byte structure suitable for use as the value in
        ``attrs_before={SQL_COPT_SS_ACCESS_TOKEN: ...}``.
    """
    token_bytes = access_token.encode("utf-16-le")
    return struct.pack(f"<I{len(token_bytes)}s", len(token_bytes), token_bytes)


def get_warehouse_access_token(tenant_id: str, client_id: str, client_secret: str) -> bytes:
    """Acquire an Azure AD access token for Fabric Warehouse and pack it for ODBC.

    Uses a service principal (client credentials flow) to acquire a token for
    the ``https://database.windows.net/.default`` scope, then packs it into the
    binary structure required by the ODBC ``SQL_COPT_SS_ACCESS_TOKEN`` attribute.

    The token's expiry timestamp is logged at INFO level. The token string
    itself is **never** logged.

    Args:
        tenant_id: Microsoft Entra tenant ID (GUID).
        client_id: Service principal (application) client ID.
        client_secret: Service principal client secret.

    Returns:
        Packed token bytes ready for ``attrs_before={1256: ...}``.
    """
    credential = ClientSecretCredential(tenant_id, client_id, client_secret)
    token = credential.get_token(WAREHOUSE_TOKEN_SCOPE)
    logger.info("Warehouse token acquired; expires_on=%s", token.expires_on)
    return pack_token_for_odbc(token.token)
