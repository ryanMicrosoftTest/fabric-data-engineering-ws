# Fabric Cluster Start-Time Service

Telemetry service that captures Fabric Spark cluster start-time performance
metrics, persists them to a Fabric Warehouse, and exposes them via an Azure
Function App composed of 18 functions (HTTP triggers, timer triggers,
durable orchestrators, and activities).

## Layout

| Path | Purpose |
|---|---|
| `function/` | Azure Functions Python v2 app (Python 3.13) — all 18 functions |
| `iac/function_app/` | Terraform for the Function App + dependencies (per-env tfvars in `envs/`) |
| `iac/warehouse/FabricTelemetryWarehouse.sqlproj` | Microsoft.Build.Sql 2.1.0 schema project for the Fabric Warehouse |

## CI/CD

Pipeline definition: [`.cicd/fabric-cluster-start-time-service-pl.yml`](../.cicd/fabric-cluster-start-time-service-pl.yml)

The pipeline runs as a single multi-stage build:

1. **Validate** — `ruff`, `black --check`, `mypy` (warn-only), `pytest` with `--cov-fail-under=80`
2. **InfraDeploy** — `terraform apply` against `iac/function_app/` using the env-specific tfvars (skippable via `skipInfra=true`)
3. **WarehouseDeploy** — builds the `.sqlproj` to a `.dacpac` and publishes via `sqlpackage` against the Fabric Warehouse using an AAD access token (skippable via `skipWarehouse=true`)
4. **FunctionDeploy** — `func azure functionapp publish ... --python --build remote`
5. **Smoke** — `GET /api/health` on the deployed Function App with retries

`FunctionDeploy` runs whenever `InfraDeploy` and `WarehouseDeploy` either
succeeded or were skipped, so code-only redeploys are supported by setting
both `skipInfra=true` and `skipWarehouse=true` when queueing.

### Required Azure DevOps configuration

**Service connections** (Project Settings → Service connections):

| Name | Used for |
|---|---|
| `fabric-sc` | DEV / QA — Azure subscription + Fabric SPN |
| `fabric-sc-prod` | PROD — Azure subscription + Fabric SPN |

**Variable group(s)** linked to the pipeline must provide:

| Variable | Purpose |
|---|---|
| `KeyVaultNameNonProd` | Key Vault name for DEV/QA secrets |
| `KeyVaultNameProd` | Key Vault name for PROD secrets |
| `TF_BACKEND_RG` | Resource group of the Terraform state storage account |
| `TF_BACKEND_SA` | Storage account name for Terraform state |
| `TF_BACKEND_CONTAINER` | Blob container for Terraform state (state key is set to `fabric-cluster-start-time-service/<env>.tfstate`) |

### Pipeline parameters

| Parameter | Values | Default | Notes |
|---|---|---|---|
| `environment` | `DEV` / `QA` / `PROD` | `DEV` | Selects service connection, KV, and tfvars file |
| `skipInfra` | bool | `false` | Skip the Terraform stage |
| `skipWarehouse` | bool | `false` | Skip the dacpac publish stage |
