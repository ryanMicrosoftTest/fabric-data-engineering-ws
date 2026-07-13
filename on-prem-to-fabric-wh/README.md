# On-Prem → Fabric Data Warehouse — SPN Write POC

## Purpose

This POC demonstrates the **correct, working pattern** for writing to a Microsoft Fabric Data Warehouse from Python on a developer laptop (or any non-Fabric host) using a Service Principal whose secret lives in Azure Key Vault. It exists to resolve [issue #10 — *Fabric Data Warehouse ODBC Connection Issue*](https://github.com/RyanMicrosoftContosoUniversity/fabric-data-engineering-ws-forked/issues/10), where the customer was getting a bare `Login failed for user '' (18456)` while using `Authentication=ActiveDirectoryServicePrincipal` with `UID`/`PWD`. The end-to-end flow mirrors the architecture diagram in [`overview.excalidraw`](./overview.excalidraw) — laptop reads the SPN secret from Key Vault, exchanges it for a Warehouse access token, and injects that token into pyodbc.

## Architecture

The diagram in `overview.excalidraw` shows five numbered steps; the script implements each one in order:

```
┌─────────────────────────────────────┐         ┌──────────────────────────────────┐
│        On-Premise Network           │         │              Fabric              │
│  ┌────────────────────────────┐     │         │   ┌──────────────────────────┐   │
│  │   VS Code / Python script  │     │         │   │       Workspace          │   │
│  │   (write_to_warehouse.py)  │     │         │   │   ┌──────────────────┐   │   │
│  └────────────┬───────────────┘     │         │   │   │  Data Warehouse  │   │   │
│               │                     │         │   │   └────────▲─────────┘   │   │
│               │ 1. DefaultAzureCred │         │   └────────────┼─────────────┘   │
│               ▼                     │         └────────────────┼─────────────────┘
│  ┌────────────────────────────┐     │                          │
│  │      Azure Key Vault       │─────┼──── 2. read SPN secret ──┘
│  │  secret: <spn-secret-name> │     │      3. acquire token (audience: database.windows.net/.default)
│  └────────────────────────────┘     │      4. pyodbc.connect(..., attrs_before={1256: token_struct})
│                                     │      5. write rows (executemany / fast_executemany)
└─────────────────────────────────────┘
```

1. **Authenticate the laptop user** with `DefaultAzureCredential` (uses `az login` context).
2. **Read the SPN client secret** from Key Vault.
3. **Acquire a Warehouse access token** via `ClientSecretCredential` against audience `https://database.windows.net/.default`.
4. **Connect via pyodbc** with `attrs_before={1256: token_struct}` — no `Authentication=`, no `UID`, no `PWD`.
5. **Write rows** with `cursor.fast_executemany = True`.

## Folder Layout

```
on-prem-to-fabric-wh/
├── overview.excalidraw              architecture diagram (source of truth)
├── README.md                        this file
├── requirements.txt                 pinned dependencies
├── .env.sample                      template for required environment variables
├── src/
│   ├── __init__.py
│   ├── config.py                    loads & validates environment configuration
│   ├── secrets.py                   reads the SPN client secret from Key Vault
│   ├── token_provider.py            acquires the Warehouse access token + packs it for pyodbc
│   ├── warehouse_writer.py          connect() and write_dataframe() helpers
│   └── synthetic_data.py            generate_orders(n_rows) → pandas DataFrame
├── scripts/
│   └── write_to_warehouse.py        thin orchestration entry point
├── docs/
│   ├── prerequisites.md             what must exist BEFORE running
│   ├── auth-pattern-explained.md    why ActiveDirectoryServicePrincipal fails
│   └── troubleshooting.md           common errors → fixes
└── tests/
    └── test_token_format.py         unit test for the token-packing format
```

## Quickstart

1. **Install ODBC Driver 18 for SQL Server** — see [Download ODBC Driver for SQL Server](https://learn.microsoft.com/sql/connect/odbc/download-odbc-driver-for-sql-server).
2. **Create and activate a virtual environment**:
   - Windows: `python -m venv .venv && .venv\Scripts\activate`
   - Unix: `python -m venv .venv && source .venv/bin/activate`
3. **Install dependencies**: `pip install -r requirements.txt`
4. **Copy the env template and fill in values**: `cp .env.sample .env` (Windows: `copy .env.sample .env`). See [`docs/prerequisites.md`](./docs/prerequisites.md) for what each variable means and how to find the value.
5. **Sign in with the Azure CLI** as the developer identity that has `Key Vault Secrets User` on the vault: `az login`.
6. **Run the script**: `python scripts/write_to_warehouse.py --rows 100`

## CLI Flags

| Flag          | Description                                                             | Default   |
|---------------|-------------------------------------------------------------------------|-----------|
| `--rows`      | Number of synthetic order rows to generate and write.                   | `100`     |
| `--if-exists` | Behavior if the target table exists: `append` or `replace`.             | `append`  |
| `--table`     | Target table name (the schema is taken from `WH_SCHEMA` in `.env`).     | `orders`  |
| `--seed`      | Random seed for reproducible synthetic data.                            | `42`      |

## Why This Pattern

The Fabric Data Warehouse TDS endpoint **does not honor** the `Authentication=ActiveDirectoryServicePrincipal` ODBC keyword path with `UID`/`PWD` for service principal credentials — the driver ends up forwarding an empty principal and the server logs `Login failed for user '' (18456)`. The supported pattern is to acquire an Entra access token (audience `https://database.windows.net/.default`) and inject it into pyodbc via `attrs_before={1256: token_struct}` where `1256` is `SQL_COPT_SS_ACCESS_TOKEN`. **Do NOT use `Authentication=ActiveDirectoryServicePrincipal` with `UID`/`PWD` against Fabric Warehouse** — use access-token injection. Full explanation, including audience and connection-string gotchas, lives in [`docs/auth-pattern-explained.md`](./docs/auth-pattern-explained.md).

## Troubleshooting

A symptom-→-cause-→-fix table for the most common errors (18456, missing driver, port 1433 blocked, KV `Forbidden`, MARS, etc.) is in [`docs/troubleshooting.md`](./docs/troubleshooting.md).

## Out of Scope

- Provisioning the Key Vault, the SPN, or the Warehouse (any IaC).
- Certificate-based SPN authentication.
- Interactive (`DefaultAzureCredential` / `az login`) auth path *to the Warehouse* (we only use it to read the KV secret).
- Reading from any external data source — synthetic data only.
- Production hardening: retry/backoff beyond a single retry, large-volume bulk load (e.g., `COPY INTO` from ADLS), schema migration, CI/CD.
- SQLAlchemy integration — see "Next Steps" below.

## Next Steps

- **SQLAlchemy variant** — wrap the same token-injection in a SQLAlchemy `creator=` callable so existing ORM code can plug in without changing how the token is acquired.
- **Certificate-based SPN** — replace `ClientSecretCredential` with `CertificateCredential` to remove the long-lived secret in Key Vault.
- **CI/CD** — run the unit tests in a pipeline; add a smoke job that runs the end-to-end write against a non-prod Warehouse using a federated workload identity.
