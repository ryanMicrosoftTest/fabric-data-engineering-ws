# Why Token Injection (Not ActiveDirectoryServicePrincipal)

## The broken pattern

This is what the customer in [issue #10](https://github.com/RyanMicrosoftContosoUniversity/fabric-data-engineering-ws-forked/issues/10) tried — and what *looks* like it should work because it works fine against Azure SQL Database:

```python
import pyodbc

conn_str = (
    "DRIVER={ODBC Driver 18 for SQL Server};"
    "SERVER=xxxxxxxx.datawarehouse.fabric.microsoft.com;"
    "DATABASE=MyWarehouse;"
    "Authentication=ActiveDirectoryServicePrincipal;"
    "UID=<spn-client-id>;"
    "PWD=<spn-client-secret>;"
    "Encrypt=yes;"
)
conn = pyodbc.connect(conn_str)   # 💥
```

The result is the bare:

```
('28000', "[28000] [Microsoft][ODBC Driver 18 for SQL Server][SQL Server]
  Login failed for user '' (18456)")
```

This is exactly what issue #10 hit.

## Why it fails

The Fabric Data Warehouse TDS endpoint **does not honor the `Authentication=ActiveDirectoryServicePrincipal` ODBC keyword path for SPN credentials**. The driver builds an authentication request but ends up forwarding an empty `UID` field on the wire, which the server logs as user `''`. The login is rejected before any database lookup or workspace ACL check runs — that's why the error is the bare `18456` with no principal name, and not the `"authentication was successful, but the database was not found, or you have insufficient permissions to connect to it"` variant you'd see for a wrong `DATABASE=`.

In short: the keyword exists in the driver, but the *server side* of Fabric Warehouse won't accept that flow for an SPN. There is currently no Microsoft-documented sample that uses it for Warehouse; every working sample uses access-token injection.

## The correct pattern

Acquire an Entra ID access token yourself, then hand it to pyodbc via a connection attribute:

1. **Build a `ClientSecretCredential`** from the tenant ID, SPN client ID, and SPN client secret.
2. **Call `get_token("https://database.windows.net/.default")`** to obtain an access token whose audience matches what the Warehouse TDS endpoint validates.
3. **Pack the token** as a 4-byte little-endian length prefix followed by the token encoded as UTF-16-LE.
4. **Call `pyodbc.connect(...)`** with `attrs_before={1256: token_struct}`, where `1256` is `SQL_COPT_SS_ACCESS_TOKEN`.

Here is the same flow as it lives in the `src/` modules of this POC:

```python
import struct
import pyodbc
from azure.identity import ClientSecretCredential

# Step 1
credential = ClientSecretCredential(
    tenant_id=TENANT_ID,
    client_id=SPN_CLIENT_ID,
    client_secret=spn_secret_from_kv,      # pragma: allowlist secret
)

# Step 2 — audience MUST be database.windows.net
access_token = credential.get_token("https://database.windows.net/.default").token

# Step 3 — 4-byte LE length prefix + UTF-16-LE bytes
token_bytes = access_token.encode("utf-16-le")
token_struct = struct.pack(f"<I{len(token_bytes)}s", len(token_bytes), token_bytes)

# Step 4 — note: NO Authentication=, NO UID=, NO PWD=
conn_str = (
    "DRIVER={ODBC Driver 18 for SQL Server};"
    f"SERVER={WH_SERVER};"
    f"DATABASE={WH_DATABASE};"
    "Encrypt=yes;"
    "TrustServerCertificate=no;"
)
SQL_COPT_SS_ACCESS_TOKEN = 1256
conn = pyodbc.connect(conn_str, attrs_before={SQL_COPT_SS_ACCESS_TOKEN: token_struct})
```

The POC splits this across `src/secrets.py` (Key Vault read), `src/token_provider.py` (steps 1–3), and `src/warehouse_writer.py` (step 4 + writes), but the wire shape is identical.

## Audience gotchas

The token audience **must** be:

```
https://database.windows.net/.default
```

It must **NOT** be any of these — they all return tokens that look valid locally but get rejected by the Warehouse TDS endpoint:

- `https://api.fabric.microsoft.com/.default` — this audience is for Fabric REST APIs (workspaces, items), not for TDS / SQL.
- `https://analysis.windows.net/powerbi/api/.default` — this is the Power BI XMLA / dataset audience.
- `https://<tenant>.fabric.microsoft.com/.default` — not a real audience.

If you get a token successfully but `pyodbc.connect` still fails with `Login failed`, the audience is the first thing to check.

## Connection-string gotchas

When using token injection, the connection string must obey these rules:

- **No `Authentication=...`** — leave the keyword out entirely. Setting it to `ActiveDirectoryAccessToken` is also unnecessary and can confuse some driver versions; just omit it.
- **No `UID=`** — including a `UID=` (even empty, even matching the SPN client ID) can override the token-based identity in some driver paths. Leave it out.
- **No `PWD=`** — same reason.
- **`DATABASE=` must be the Warehouse item NAME**, not a GUID, not derived from the FQDN. If `config.py` sees a GUID-shaped value it warns; the canonical wrong-database error text is `"authentication was successful, but the database was not found, or you have insufficient permissions to connect to it"` which is *different* from the bare `18456`.
- **`Encrypt=yes`** is required; `TrustServerCertificate=no` is correct for Fabric (the cert chain is valid).
- **Do not set `MultipleActiveResultSets=true`** — Fabric Warehouse does not support MARS.

## Reference: the same pattern already lives in this repo

The Fabric notebook [`fabric-lakehouse-migration/lakehouse_dependency_discovery_nb.py`](../../fabric-lakehouse-migration/lakehouse_dependency_discovery_nb.py) (lines 49–80) shows the same token + connect pattern, with one difference: because that notebook runs *inside* Fabric, it acquires the token via `mssparkutils.credentials.getToken("DW")` instead of `ClientSecretCredential`. Once the token is in hand, the packing (`<I{len}s` UTF-16-LE) and the `attrs_before={1256: token_struct}` shape are identical to what this POC does externally. That file is the canonical working example to compare against if anything in this POC stops connecting.
