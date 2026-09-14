# medallion_orchestrator

This folder is based on the source snapshot of Fabric workspace
`airflow-example-ws` (`896bca78-0d4a-4dc9-8716-05539ecf4ea5`) and Apache
Airflow Job `medallion_orchestrator`
(`3add6022-0759-47a1-ab32-2f038e8308f8`).

## Snapshot contents

- `.platform` and `apacheairflowjob-content.json` are decoded from the deployed
  `getDefinition` response.
- `baseline-manifest.json` records the original deployed workspace/item
  metadata and SHA-256 hashes.
- `dags/fabric_notebook_fallback.py` contains the self-contained
  `SqlConfiguredFabricNotebookOperator` recovery implementation.
- Both DAGs import the sibling fallback module; no private wheel is required.

## Deployed environment

| Setting | Value |
| --- | --- |
| Airflow environment | `FabricAirflowJob-1.0.0` |
| Airflow | `2.10.5` |
| Python | `3.12` |
| Compute | Starter Pool, Small |
| Autoscale | Disabled |
| Availability zones | Enabled |
| Extra nodes | `0` |
| Microsoft Entra integration | Enabled |
| Fabric connections | Enabled |
| Triggerers | Disabled |
| Environment variables | None |
| Airflow configuration overrides | None |
| Additional requirements | None |

The definition does not expose the installed
`apache-airflow-microsoft-fabric-plugin` package version. Determining that
version requires runtime package inspection and is outside this read-only
baseline capture.

## Behavior-preservation checklist

- DAG ID: `medallion_nyc_taxi`
- Schedule: `0 10 * * *` (daily at 10:00)
- Start date: `2024-01-01`; catchup disabled
- Owner: `data-engineering`; two retries with a five-minute delay
- Operator: `SqlConfiguredFabricNotebookOperator`, synchronous polling, not
  deferrable
- Notebook tasks: `bronze_ingest`, `silver_transform`, `gold_aggregate`
- Notebook IDs remain exactly as recorded in the DAG
- Dependency order: `bronze >> silver >> gold`

Each task reads its notebook-specific compute configuration at execution time
through `airflow_config_sql` and submits through `fabric_conn`. No Spark sizing
is stored in the DAG.

## Airflow connections

Create these Airflow connections with the exact field mappings below. Both use
the same service principal unless separate principals are intentionally
provisioned.

### `fabric_conn`

| Airflow field | Value |
| --- | --- |
| Connection ID | `fabric_conn` |
| Connection type | `Microsoft Fabric notebook` (`fabric`) |
| Host | `https://api.fabric.microsoft.com` |
| Login | Microsoft Entra application (client) ID |
| Password | Client secret |
| Schema, Port | Leave blank |
| Extra | `{"tenantId": "<tenant-id>"}` |

`Host` may be omitted to use the default Fabric API endpoint. In `Extra`,
`endpoint`, `clientId`, and `clientSecret` are supported fallbacks, but Login
and Password are the preferred mappings.

### `airflow_config_sql`

| Airflow field | Value |
| --- | --- |
| Connection ID | `airflow_config_sql` |
| Connection type | `Fabric SQL notebook compute configuration` (`fabric_sql_config`) |
| Host | Fabric Warehouse or SQL analytics endpoint server name (without `https://`) |
| Schema | Database/Warehouse name |
| Login | Microsoft Entra application (client) ID |
| Password | Client secret |
| Port | Leave blank |
| Extra | `{"tenantId": "<tenant-id>", "driver": "ODBC Driver 18 for SQL Server", "token_scope": "https://database.windows.net/.default"}` |

The `tenantId` value is required. `driver` and `token_scope` may be omitted to
use the exact defaults shown. In `Extra`, `clientId` and `clientSecret` are
supported fallbacks for Login and Password.
