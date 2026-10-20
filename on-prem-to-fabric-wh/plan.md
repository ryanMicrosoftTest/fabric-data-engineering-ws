# Plan: On-Prem → Fabric Warehouse SPN Write POC

**Target folder for all artifacts:** `on-prem-to-fabric-wh/`
**Architecture source of truth:** `on-prem-to-fabric-wh/overview.excalidraw`

---

## Problem Statement

Issue [`fabric-data-engineering-ws-forked#10`](https://github.com/RyanMicrosoftContosoUniversity/fabric-data-engineering-ws-forked/issues/10) describes a customer failing to write to a Fabric Data Warehouse from Python (SQLAlchemy + pyodbc) using `Authentication=ActiveDirectoryServicePrincipal` with `UID/PWD` — they get the bare `Login failed for user '' (18456)`.

Research (see session report `…/research/can-you-review-this-in-detail-don-t-create-any-fil.md`) established that this auth mode is **not the supported SPN path for the Fabric Warehouse TDS endpoint**. The supported pattern is:

> `azure-identity` → token for audience `https://database.windows.net/.default` → inject into pyodbc via `attrs_before={1256: token_struct}`.

This POC implements the **correct** pattern end-to-end, runnable from a developer's laptop (or any non-Fabric host) over the on-prem network, and uses Key Vault to hold the SPN client secret — exactly matching `overview.excalidraw`.

## Approach (Confirmed Scope)

| Decision | Choice |
|---|---|
| Data source for "step 2: get data" | **Synthetic data generated in-script** (no external DB / file dependency) |
| Auth path | **SPN with client secret pulled from Key Vault** (single path; matches diagram exactly) |
| Infrastructure | **Assume Key Vault, SPN, and Warehouse already exist**; document prerequisites only — no Bicep/Terraform/CLI provisioning scripts |

## Architecture (mirrors `overview.excalidraw`)

```
┌─────────────────────────────────────┐         ┌──────────────────────────────────┐
│        On-Premise Network           │         │              Fabric              │
│  ┌────────────────────────────┐     │         │   ┌──────────────────────────┐   │
│  │   VS Code / Python script  │     │         │   │       Workspace          │   │
│  │   (write_to_warehouse.py)  │     │         │   │   ┌──────────────────┐   │   │
│  └────────────┬───────────────┘     │         │   │   │  Data Warehouse  │   │   │
│               │                     │         │   │   └────────▲─────────┘   │   │
│               │ 1. get SPN secret   │         │   └────────────┼─────────────┘   │
│               │  (DefaultAzureCred) │         │                │                 │
│               ▼                     │         └────────────────┼─────────────────┘
│  ┌────────────────────────────┐     │                          │
│  │      Azure Key Vault       │─────┼──── 2. read secret ──────┘
│  │  secret: <spn-secret-name> │     │      3. acquire token (database.windows.net/.default)
│  └────────────────────────────┘     │      4. pyodbc connect with attrs_before={1256:…}
│                                     │      5. write rows (executemany / bulk insert)
└─────────────────────────────────────┘
```

## Deliverables (all under `on-prem-to-fabric-wh/`)

```
on-prem-to-fabric-wh/
├── overview.excalidraw              (already exists — architecture diagram)
├── README.md                        ← step-by-step runbook + troubleshooting
├── requirements.txt                 ← pinned: azure-identity, azure-keyvault-secrets, pyodbc, pandas (for synthetic gen)
├── .env.sample                      ← template for KV name, secret name, tenant id, SPN client id, server, database, schema, table
├── src/
│   ├── __init__.py
│   ├── config.py                    ← reads .env / env vars, validates required values
│   ├── secrets.py                   ← get_spn_secret(kv_name, secret_name) using DefaultAzureCredential
│   ├── token_provider.py            ← get_warehouse_access_token(tenant, client_id, client_secret) → bytes (4-byte LE-prefixed UTF-16-LE)
│   ├── warehouse_writer.py          ← connect() + write_dataframe(df, schema, table, if_exists)
│   └── synthetic_data.py            ← generate_orders(n_rows) returns pandas DataFrame
├── scripts/
│   └── write_to_warehouse.py        ← thin entry point: load config → fetch secret → get token → generate data → write
├── docs/
│   ├── prerequisites.md             ← what must exist BEFORE running (KV, SPN, Warehouse role, network)
│   ├── auth-pattern-explained.md    ← why ActiveDirectoryServicePrincipal fails; why token injection works
│   └── troubleshooting.md           ← maps common errors (18456, "database not found", driver missing) to fixes
└── tests/
    └── test_token_format.py         ← unit test for the 4-byte LE-prefixed UTF-16-LE token packing (no network)
```

## Key Implementation Notes

1. **Token packing must be exact** — `struct.pack(f"<I{len(token_bytes)}s", len(token_bytes), token_bytes)` where `token_bytes = access_token.encode("utf-16-le")`. The `1256` constant is `SQL_COPT_SS_ACCESS_TOKEN`.
2. **Connection string must NOT contain** `Authentication=`, `UID=`, or `PWD=`. Only: `DRIVER={ODBC Driver 18 for SQL Server};SERVER=<fqdn>;DATABASE=<warehouse-item-name>;Encrypt=yes;TrustServerCertificate=no;`.
3. **`DATABASE=` is the Warehouse item name**, not a GUID, not derived from the FQDN. Validation in `config.py` should warn if it looks like a GUID.
4. **Token acquisition via `ClientSecretCredential`** (not `DefaultAzureCredential`) for the Warehouse TDS auth — `DefaultAzureCredential` is only used to read the KV secret in the first place. Two distinct credentials by design (matches diagram: laptop user reads KV; SPN talks to Warehouse).
5. **Audience for the token** must be `https://database.windows.net/.default` — not `api.fabric.microsoft.com`, not `analysis.windows.net/powerbi/api`. Common mistake worth calling out in `auth-pattern-explained.md`.
6. **Write strategy**: `pyodbc` `cursor.fast_executemany = True` + `executemany("INSERT INTO …")`. No SQLAlchemy required for the POC — keeps the surface small and avoids dialect quirks. (README will note where SQLAlchemy fits if the user wants it later.)
7. **Idempotence**: `write_dataframe(..., if_exists="replace"|"append")`. Default is `append`. Schema creation is one-shot DDL via `CREATE TABLE IF NOT EXISTS` equivalent (Fabric Warehouse: use `IF OBJECT_ID(...) IS NULL CREATE TABLE …`).
8. **Secrets hygiene**: `.env` is gitignored; `.env.sample` is committed. The script never logs the secret or the token (only logs token expiry timestamp).
9. **Reusability**: every module is importable; `scripts/write_to_warehouse.py` is ~30 lines orchestration only.

## Prerequisites the runbook will document (not provision)

- Fabric Warehouse exists; note the **Workspace ID**, **Warehouse item name**, and **SQL Connection String FQDN** from the Fabric portal.
- SPN exists in Entra ID; tenant setting **"Service principals can use Fabric APIs"** is enabled and the SPN is in the allowed security group.
- SPN has been granted access to the **workspace** (Member or Contributor) AND to the Warehouse item; for SQL DML the SPN also needs the appropriate database role (e.g., `db_datareader` + `db_datawriter`, or schema-scoped grants).
- Key Vault exists; the **client secret** is stored under a known secret name. The laptop user has `Key Vault Secrets User` role on the vault.
- ODBC Driver 18 (or higher) for SQL Server is installed on the laptop. Port 1433 outbound is reachable.

## Out of Scope (explicitly)

- Provisioning Key Vault, SPN, or Warehouse (any IaC).
- Certificate-based SPN auth.
- Interactive (DefaultAzureCredential / az-login) auth path to the Warehouse.
- Reading from any external data source — synthetic data only.
- Production hardening (retry backoff beyond a single retry, large-volume bulk-load patterns like COPY INTO from ADLS, schema migrations, CI/CD).
- SQLAlchemy integration (mentioned in README as a follow-on, not implemented).

## Todos

Tracked in SQL (see below). The plan covers ten ordered todos grouped into four phases:

1. **Foundation** — folder skeleton, requirements, .env.sample, .gitignore.
2. **Core code** — config → secrets → token → writer → synthetic data → entry script.
3. **Documentation** — prerequisites, auth-pattern explanation, troubleshooting, top-level README.
4. **Validation** — unit test for token packing; end-to-end smoke instructions in README (cannot be auto-run without live KV/Warehouse).

## Open Items / Notes

- The user's environment already has `azure-identity` and `azure-keyvault-secrets` in the root `requirements.txt`; `on-prem-to-fabric-wh/requirements.txt` will pin minimum versions and add `pyodbc` and `pandas`.
- The Excalidraw arrow labels say "1. get SPN client secret for AAD user" — interpreted as "the developer's laptop (AAD user) reads the SPN client secret from KV." Plan reflects that.
- No comments / sub-issues / labels exist on issue #10, so no further customer-side context to incorporate.
- Once the POC is working, a natural follow-on is to wrap the same pattern in a SQLAlchemy `creator=` callable and document the SQLAlchemy variant — left as a README "next steps" note.
