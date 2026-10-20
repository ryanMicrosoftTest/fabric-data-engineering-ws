# Prerequisites

Everything in this checklist must be in place **before** you run `scripts/write_to_warehouse.py`. The script provisions nothing — it assumes the Fabric Warehouse, the Service Principal, and the Key Vault already exist and are wired up correctly.

## 1. Fabric Warehouse

*Why it matters:* the script needs the exact server FQDN and the Warehouse item name to build the connection string; using a GUID or the workspace name will produce `Cannot open database` or `Login failed`.

- [ ] **Workspace ID** — find it in the Fabric portal: open the workspace, click the gear → *Workspace settings*; the ID is in the URL (`/groups/<workspace-id>/...`) and in *About this workspace*.
- [ ] **Warehouse item NAME** (not GUID) — open the Warehouse in the portal; the name shown in the breadcrumb / title bar is what goes into `DATABASE=` and into `WH_DATABASE` in `.env`.
- [ ] **SQL connection FQDN** — open the Warehouse → *Settings* (gear icon) → *SQL connection string* → copy the server value (looks like `xxxxxxxx.datawarehouse.fabric.microsoft.com`). This is `WH_SERVER` in `.env`.

## 2. Service Principal (SPN)

*Why it matters:* the SPN is the identity the Warehouse will see; without the right tenant setting and group membership, even a perfectly-formed token will be rejected at the Fabric tenant boundary.

- [ ] **SPN exists in Microsoft Entra ID** — Azure Portal → *Microsoft Entra ID* → *App registrations* → confirm the application is present.
- [ ] **Client (Application) ID is recorded** — copy it from the app registration overview blade into `SPN_CLIENT_ID` in `.env`.
- [ ] **Tenant ID is recorded** — same blade, *Directory (tenant) ID* → `TENANT_ID` in `.env`.
- [ ] **Tenant setting "Service principals can use Fabric APIs" is enabled** — Fabric Admin portal → *Tenant settings* → *Developer settings* → toggled **on**, scoped to either the entire org or to a security group.
- [ ] **SPN is a member of that security group** (only required if the tenant setting is scoped to a group). Verify in Entra ID → *Groups* → group → *Members*.

## 3. Workspace + Warehouse permissions

*Why it matters:* tenant-level permission to call Fabric APIs is necessary but not sufficient; the SPN also needs workspace and item-level access, plus a SQL database role for DML.

- [ ] **SPN added to the workspace** as **Member** or **Contributor** — Fabric portal → workspace → *Manage access* → *Add people or groups* → search by SPN display name → assign role.
- [ ] **SPN granted access to the Warehouse item** — open the Warehouse → *Share* (or *Manage permissions*) → add the SPN with *Read* + *Write* (or higher).
- [ ] **SPN has a SQL database role** — at minimum `db_datareader` + `db_datawriter` on the Warehouse, or schema-scoped `GRANT SELECT, INSERT, UPDATE, DELETE ON SCHEMA::<schema> TO [<spn-display-name>]`. Run from a SQL editor connected as a Warehouse admin.

## 4. Azure Key Vault

*Why it matters:* the SPN's client secret is held in Key Vault so it never lives on disk or in source control; the *laptop* user (not the SPN) needs read access to fetch it.

- [ ] **Key Vault exists** — Azure Portal → *Key Vaults* → confirm the vault is present and note its name → `KV_NAME` in `.env`.
- [ ] **SPN client secret is stored in the vault** under a known secret name (e.g., `fabric-wh-spn-secret`) → `KV_SECRET_NAME` in `.env`. Create it via *Secrets* → *Generate/Import* if it isn't there yet.
- [ ] **The laptop user has the `Key Vault Secrets User` role on the vault** — Azure Portal → vault → *Access control (IAM)* → *Add role assignment* → *Key Vault Secrets User* → assign to the user account that runs `az login`. (Note: this role goes to the *laptop user*, NOT the SPN.)

## 5. Local machine

*Why it matters:* missing the ODBC driver or having outbound port 1433 blocked produces errors that look like auth failures but aren't — installing prerequisites up front prevents an hour of wrong-tree-barking.

- [ ] **Python 3.10 or higher** — `python --version`.
- [ ] **ODBC Driver 18 for SQL Server** installed — see the [download page](https://learn.microsoft.com/sql/connect/odbc/download-odbc-driver-for-sql-server). On Windows, verify with *ODBC Data Sources (64-bit)* → *Drivers* tab.
- [ ] **Outbound port 1433 reachable** to `*.datawarehouse.fabric.microsoft.com` — test with `Test-NetConnection <fqdn> -Port 1433` (PowerShell) or `nc -zv <fqdn> 1433` (Unix).
- [ ] **Azure CLI installed** and logged in: `az --version`, then `az login` as the laptop user that has `Key Vault Secrets User`.

## 6. Environment file

*Why it matters:* the script reads everything from environment variables (loaded from `.env`) so the same code runs unchanged across environments; missing values fail fast with a clear error.

- [ ] **Copy the template**: `cp .env.sample .env` (Windows: `copy .env.sample .env`).
- [ ] **Fill in all 8 values** (each maps to one of the items above):
  - [ ] `TENANT_ID`
  - [ ] `SPN_CLIENT_ID`
  - [ ] `KV_NAME`
  - [ ] `KV_SECRET_NAME`
  - [ ] `WH_SERVER` (FQDN)
  - [ ] `WH_DATABASE` (Warehouse item name)
  - [ ] `WH_SCHEMA`
  - [ ] `WH_TABLE`
- [ ] **Confirm `.env` is gitignored** before saving real values to it.
