"""
Script to write data to the Fabric Data Warehouse
"""
from __future__ import annotations
import pyodbc
import pandas as pd
import os
from pathlib import Path
from dataclasses import dataclass
from dotenv import load_dotenv
from azure.identity import DefaultAzureCredential, ClientSecretCredential
from azure.keyvault.secrets import SecretClient
from azure.core.exceptions import (
    ResourceNotFoundError,
    ClientAuthenticationError,
    HttpResponseError,
)
import struct

# Load .env from the project root (parent of scripts/)
_ENV_PATH = Path(__file__).resolve().parent.parent / '.env'
load_dotenv(dotenv_path=_ENV_PATH)


WAREHOUSE_TOKEN_SCOPE = 'https://database.windows.net/.default'
# ODBC pre-connect attribute identifier for supplying an Azure AD access token
# to the Microsoft ODBC Driver for SQL Server. Passed via
# ``pyodbc.connect(..., attrs_before={SQL_COPT_SS_ACCESS_TOKEN: <packed bytes>})``.
SQL_COPT_SS_ACCESS_TOKEN = 1256

@dataclass(frozen=True)
class Config:
    """
    Immutable configuration for the on-prem-to-Fabric warehouse pipeline
    """

    azure_tenant_id: str
    spn_client_id: str
    kv_name: str
    kv_secret_name: str
    warehouse_server: str
    warehouse_database: str
    warehouse_schema: str = 'dbo'
    warehouse_table: str = 'synthetic_orders'

def load_config() -> Config:
    """
    Build a Config from environment variables
    """
    required = [
        'AZURE_TENANT_ID',
        'SPN_CLIENT_ID',
        'KV_NAME',
        'KV_SECRET_NAME',
        'WAREHOUSE_SERVER',
        'WAREHOUSE_DATABASE',
    ]

    missing = [name for name in required if not os.getenv(name, '').strip()]
    if missing:
        raise ValueError(
            'Missing required environment variables: ' + ', '.join(missing)
        )

    return Config(
        azure_tenant_id=os.environ['AZURE_TENANT_ID'],
        spn_client_id=os.environ['SPN_CLIENT_ID'],
        kv_name=os.environ['KV_NAME'],
        kv_secret_name=os.environ['KV_SECRET_NAME'],
        warehouse_server=os.environ['WAREHOUSE_SERVER'],
        warehouse_database=os.environ['WAREHOUSE_DATABASE'],
        warehouse_schema=os.getenv('WAREHOUSE_SCHEMA', 'dbo'),
        warehouse_table=os.getenv('WAREHOUSE_TABLE', 'synthetic_orders'),
    )

def get_spn_secret(kv_name: str, secret_name: str) -> str:
    """
    Fetch a secret value from an Azure Key Vault.
    """
    vault_url = f'https://{kv_name}.vault.azure.net'
    credential = DefaultAzureCredential()
    client = SecretClient(vault_url=vault_url, credential=credential)

    try:
        secret = client.get_secret(secret_name)
    except ResourceNotFoundError as exc:
        raise LookupError(
            f'Secret {secret_name} was not found in Key Vault {kv_name}.'
        ) from exc
    except ClientAuthenticationError as exc:
        raise RuntimeError(
            'Failed to authenticate to Azure while fetching secret '
            f'{secret_name} from Key Vault {kv_name}. '
            'Run az login (or configure a managed identity / service '
            'principal) and try again.'
        ) from exc
    except HttpResponseError as exc:
        status = getattr(exc, 'status_code', None)
        if status == 403:
            raise PermissionError(
                f'Access denied reading secret {secret_name} from Key '
                f'Vault {kv_name}. Grant the Key Vault Secrets User '
                'role to your identity on this vault and try again.'
            ) from exc
        if status == 404:
            raise LookupError(
                f'Secret {secret_name} was not found in Key Vault '
                f'{kv_name}.'
            ) from exc
        raise

    return secret.value


def pack_token_for_odbc(access_token: str) -> bytes:
    """
    Pack an Azure AD access token into the ODBC ``SQL_COPT_SS_ACCESS_TOKEN`` struct.

    This is to get around issues I had using the ODBC driver's built-in Azure AD auth flows, which requireing the token to be passed 
    in this specific struct format via the SQL_COPT_SS_ACCESS_TOKEN
    """
    token_bytes = access_token.encode('utf-16-le')
    return struct.pack(f'<I{len(token_bytes)}s', len(token_bytes), token_bytes)


def get_warehouse_access_token(tenant_id: str, client_id: str, client_secret: str) -> bytes:
    """
    Acquire an Azure AD access token for Fabric Warehouse and pack it for ODBC.

    Uses a service principal (client credentials flow) to acquire a token for
    the ``https://database.windows.net/.default`` scope, then packs it into the
    binary structure required by the ODBC ``SQL_COPT_SS_ACCESS_TOKEN`` attribute.
    """

    credential = ClientSecretCredential(tenant_id, client_id, client_secret)
    token = credential.get_token(WAREHOUSE_TOKEN_SCOPE)
    return pack_token_for_odbc(token.token)


def connect(server: str, database: str, token_struct: bytes) -> 'pyodbc.Connection':
    """
    Open a pyodbc connection to a Fabric Warehouse using an AAD access token.
    """
    conn_str = (
        f'DRIVER={{ODBC Driver 18 for SQL Server}};'
        f'SERVER={server};'
        f'DATABASE={database};'
        f'Encrypt=yes;TrustServerCertificate=no;'
    )
    return pyodbc.connect(
        conn_str,
        attrs_before={SQL_COPT_SS_ACCESS_TOKEN: token_struct},
    )


def main() -> None:
    print('loading configuration from environment variables...')
    cfg = load_config()

    # get secrets from kv
    print(f'Fetching SPN secret from Key Vault: {cfg.kv_name}')
    spn_secret = get_spn_secret(cfg.kv_name, cfg.kv_secret_name)

    # get access token
    print('get access token for SPN')
    token_struct = get_warehouse_access_token(cfg.azure_tenant_id, cfg.spn_client_id, spn_secret)

    # connect to warehouse
    cxn = connect(server=cfg.warehouse_server, database=cfg.warehouse_database, token_struct=token_struct)

    # read data and display
    df = pd.read_sql_query(f"""
                           SELECT *
                           FROM {cfg.warehouse_schema}.{cfg.warehouse_table}
                           """, cxn) 
    print(df)

    # write data to table
    insert_sql = f"""
    INSERT INTO [{cfg.warehouse_schema}].[{cfg.warehouse_table}]
    ([order_id], [customer_id], [order_ts], [amount], [region])
    VALUES (?, ?, ?, ?, ?)
    """

    values = (9999, 8888, pd.Timestamp.now().to_pydatetime(), 123.45, 'NORTH')

    cursor = cxn.cursor()
    cursor.execute(insert_sql, *values)
    cxn.commit()
    print(f'Inserted 1 row into {cfg.warehouse_schema}.{cfg.warehouse_table}')

    cxn.close()


if __name__ == "__main__":
    main()