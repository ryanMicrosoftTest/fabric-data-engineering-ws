# References

Every substantive claim in this guide traces to one of the Microsoft Learn articles
below. Fabric changes quickly — re-check anything marked **preview** before relying on
it, and treat the review date in each document as the freshness boundary.

---

## Decision guidance

| Topic | Source |
|---|---|
| Lakehouse vs. Warehouse decision guide | https://learn.microsoft.com/fabric/fundamentals/decision-guide-lakehouse-warehouse |
| Choosing an analytical data store in Fabric | https://learn.microsoft.com/azure/architecture/data-guide/technology-choices/fabric-analytical-data-stores |
| Where to store data in Fabric | https://learn.microsoft.com/fabric/fundamentals/store-data |
| Fabric decision guide: choose a data store | https://learn.microsoft.com/fabric/fundamentals/decision-guide-data-store |

Used by: [01-decision-framework.md](01-decision-framework.md),
[02-capability-gap-analysis.md](02-capability-gap-analysis.md).

---

## Lakehouse and the SQL analytics endpoint

| Topic | Source |
|---|---|
| SQL analytics endpoint overview — read-only scope, DQL/DML/DDL boundaries | https://learn.microsoft.com/fabric/data-engineering/lakehouse-sql-analytics-endpoint |
| SQL analytics endpoint performance considerations | https://learn.microsoft.com/fabric/data-engineering/sql-analytics-endpoint-performance |
| Metadata sync for the SQL analytics endpoint | https://learn.microsoft.com/fabric/data-engineering/sql-analytics-endpoint-metadata-sync |
| Lakehouse overview | https://learn.microsoft.com/fabric/data-engineering/lakehouse-overview |
| Lakehouse schemas | https://learn.microsoft.com/fabric/data-engineering/lakehouse-schemas |
| Delta Lake table format interoperability in Fabric | https://learn.microsoft.com/fabric/fundamentals/delta-lake-interoperability |
| Lakehouse REST API | https://learn.microsoft.com/fabric/data-engineering/lakehouse-api |

**Key constraints sourced here**

- The endpoint is **read-only**: full DQL, **no DML**, and only limited DDL — views,
  inline TVFs, stored procedures, and functions, but **not tables**.
- Only **Delta Parquet** tables under the **`Tables/`** folder are visible. Content in
  `Files/`, and non-Delta formats, are not surfaced.
- **External Delta tables created by Spark code are not visible** to the endpoint;
  create a shortcut in the `Tables` section instead.
- Delta **column mapping by name** is supported; **by ID is not**.
- Adding a **foreign key constraint** on the endpoint **blocks all subsequent schema
  changes** on the affected tables, including adding columns.
- Automatic metadata discovery runs as a **single instance per workspace**, so many
  lakehouses in one workspace increases sync latency.
- Refresh paths: portal on-demand, the **Refresh SQL endpoint metadata REST API**, a
  T-SQL stored procedure, and the pipeline **Refresh SQL endpoint** activity.
- The **new metadata sync (preview)** offers seconds-level freshness via an
  external-tables architecture, is enabled per workspace in Warehouse settings, and
  applies to **new endpoints only**. It does not support multi-part checkpoints and is
  unavailable with workspace private link. Monitor via
  `sys.dm_db_external_tables_log_status`.

Used by: [02-capability-gap-analysis.md](02-capability-gap-analysis.md),
[06-operations-and-performance.md](06-operations-and-performance.md).

---

## Security

| Topic | Source |
|---|---|
| OneLake security for SQL analytics endpoints | https://learn.microsoft.com/fabric/onelake/security/sql-analytics-endpoint-onelake-security |
| Troubleshoot OneLake security for SQL analytics endpoints | https://learn.microsoft.com/fabric/onelake/security/troubleshoot-onelake-security-for-sql-analytics-endpoints |
| Create and manage OneLake security roles | https://learn.microsoft.com/fabric/onelake/security/create-manage-roles |
| OneLake security overview | https://learn.microsoft.com/fabric/onelake/security/get-started-security |
| Fabric permission model | https://learn.microsoft.com/fabric/security/permission-model |
| Row-level security in Fabric Warehouse | https://learn.microsoft.com/fabric/data-warehouse/row-level-security |
| Column-level security in Fabric Warehouse | https://learn.microsoft.com/fabric/data-warehouse/column-level-security |
| Dynamic data masking in Fabric Warehouse | https://learn.microsoft.com/fabric/data-warehouse/dynamic-data-masking |

**Key constraints sourced here**

- Two access modes govern a Lakehouse SQL analytics endpoint:
  - **User identity mode** — OneLake security roles govern tables; SQL `GRANT`/`REVOKE`
    on tables is **ignored**; RLS, CLS, and OLS are expressed in OneLake roles;
    **DDM is not supported**. SQL permissions still apply to views, procedures, and
    functions.
  - **Delegated identity mode** — SQL governs everything (`GRANT`/`REVOKE`,
    `CREATE SECURITY POLICY` for RLS, column-list `GRANT SELECT` for CLS,
    `ALTER TABLE ... MASKED` for DDM); OneLake roles **do not carry over**; the endpoint
    connects using the **item owner's identity**, and the owner **cannot be a service
    principal**; **shortcuts are blocked** when the source table carries any OneLake
    RLS/CLS/OLS.
- Users need **item-level Read permission** to connect at all, regardless of SQL grants.
- SQL endpoint security applies **only through the endpoint** — Spark and direct OneLake
  access are not constrained by it.
- Hub-and-spoke OneLake role patterns require **exact 1:1 identity mapping**; nested or
  effective group membership is **not** resolved from producer to consumer.

Used by: [05-security-mapping.md](05-security-mapping.md).

---

## Table maintenance and performance

| Topic | Source |
|---|---|
| Lakehouse table maintenance | https://learn.microsoft.com/fabric/data-engineering/lakehouse-table-maintenance |
| Table maintenance and optimization in Fabric | https://learn.microsoft.com/fabric/fundamentals/table-maintenance-optimization |
| Delta Lake table optimization and V-Order | https://learn.microsoft.com/fabric/data-engineering/delta-optimization-and-v-order |
| Table compaction (bin-compaction) | https://learn.microsoft.com/fabric/data-engineering/delta-lake-table-compaction |
| Table Maintenance REST API | https://learn.microsoft.com/fabric/data-engineering/lakehouse-api |

**Key constraints sourced here**

- Lakehouse layout is **user-managed**; Warehouse layout is **system-managed**. This is
  a transfer of operational burden, not a removal of it.
- **V-Order** costs roughly **15% at write time**, yields up to **50% more compression**,
  and improves read performance by around **10%** for the SQL analytics endpoint and
  Warehouse. It is the recommended write format for Direct Lake.
- `VACUUM` with a retention period **under 7 days is rejected by default**; override via
  `spark.databricks.delta.retentionDurationCheck.enabled = false` only with a deliberate
  decision, since it destroys time-travel and rollback capability.
- `REORG TABLE ... APPLY (PURGE)` physically removes rows soft-deleted by deletion
  vectors.
- Maintenance can be run from the portal, the **Lakehouse Maintenance activity
  (preview)** in Data Factory, notebooks, or the asynchronous **Table Maintenance REST
  API** (submit lakehouse-scoped; poll `/items/{itemId}/jobs/instances/{jobInstanceId}`;
  statuses `NotStarted`, `InProgress`, `Completed`, `Failed`, `Canceled`, `Deduped`).

Used by: [06-operations-and-performance.md](06-operations-and-performance.md).

---

## Direct Lake

| Topic | Source |
|---|---|
| Direct Lake overview | https://learn.microsoft.com/fabric/fundamentals/direct-lake-overview |
| How Direct Lake works | https://learn.microsoft.com/fabric/fundamentals/direct-lake-how-it-works |
| Develop Direct Lake semantic models | https://learn.microsoft.com/fabric/fundamentals/direct-lake-develop |
| Direct Lake security integration | https://learn.microsoft.com/fabric/fundamentals/direct-lake-security-integration |
| Direct Lake guardrails and capacity | https://learn.microsoft.com/fabric/enterprise/powerbi/service-premium-what-is |

**Key constraints sourced here**

- **Direct Lake on SQL** falls back to DirectQuery when SQL-based RLS, DDM, or OLS is
  present; when a table is built on a non-materialized SQL view; or when guardrails
  (parquet file count, row group count, row count) are breached. **One table breaching a
  guardrail drops the whole model to DirectQuery.**
- **Direct Lake on OneLake** has **no DirectQuery fallback**, supports composite models
  and calculated columns, **does not support non-materialized SQL views** (use a
  materialized lake view instead), and **does not apply SQL-based RLS** — it returns
  unfiltered data.
- Creating a semantic model across regions is unsupported; the workaround is a lakehouse
  with shortcuts in the target region.
- Direct Lake "refresh" is **framing** — a metadata operation that completes in seconds,
  not a data copy.

Used by: [02-capability-gap-analysis.md](02-capability-gap-analysis.md),
[05-security-mapping.md](05-security-mapping.md),
[06-operations-and-performance.md](06-operations-and-performance.md).

---

## Materialized lake views

| Topic | Source |
|---|---|
| Materialized lake views overview | https://learn.microsoft.com/fabric/data-engineering/materialized-lake-views/overview-materialized-lake-view |
| Create a materialized lake view | https://learn.microsoft.com/fabric/data-engineering/materialized-lake-views/create-materialized-lake-view |
| Data quality constraints in materialized lake views | https://learn.microsoft.com/fabric/data-engineering/materialized-lake-views/data-quality |
| Monitor materialized lake views | https://learn.microsoft.com/fabric/data-engineering/materialized-lake-views/monitor-materialized-lake-views |

**Key points sourced here**

- Declarative `CREATE MATERIALIZED LAKE VIEW`; Fabric selects the refresh strategy
  (incremental, full, or skip), orders dependencies, and enforces data-quality
  constraints.
- **PySpark authoring is in preview and full-refresh only.**
- Well suited to aggregations, complex recurring joins, and medallion SQL. Poorly suited
  to ML inference, API calls, complex procedural Python, and sub-second streaming.

Used by: [04-tsql-to-spark-patterns.md](04-tsql-to-spark-patterns.md) §10,
[08-reference-architecture.md](08-reference-architecture.md) Pattern C.

---

## Warehouse features without a Lakehouse equivalent

| Topic | Source |
|---|---|
| Warehouse snapshots | https://learn.microsoft.com/fabric/data-warehouse/warehouse-snapshot |
| Clone tables (zero-copy clone) | https://learn.microsoft.com/fabric/data-warehouse/clone-table |
| `COPY INTO` (T-SQL) | https://learn.microsoft.com/fabric/data-warehouse/ingest-data-copy |
| Transactions in Fabric Warehouse | https://learn.microsoft.com/fabric/data-warehouse/transactions |
| Warehouse T-SQL surface area | https://learn.microsoft.com/fabric/data-warehouse/tsql-surface-area |

The features with **no Lakehouse equivalent**: `IDENTITY` columns, `COPY INTO`, CTAS,
zero-copy clone, warehouse snapshots, multi-table transactions, and system-managed
layout.

Used by: [02-capability-gap-analysis.md](02-capability-gap-analysis.md),
[04-tsql-to-spark-patterns.md](04-tsql-to-spark-patterns.md).

---

## Spark and Delta authoring

| Topic | Source |
|---|---|
| Spark SQL language reference (Delta `MERGE`) | https://learn.microsoft.com/azure/databricks/sql/language-manual/delta-merge-into |
| Fabric Spark runtime versions | https://learn.microsoft.com/fabric/data-engineering/runtime |
| Fabric notebooks | https://learn.microsoft.com/fabric/data-engineering/how-to-use-notebook |
| Spark job definitions | https://learn.microsoft.com/fabric/data-engineering/spark-job-definition |
| Delta Lake in Fabric | https://learn.microsoft.com/fabric/data-engineering/lakehouse-and-delta-tables |

> Verify `WHEN NOT MATCHED BY SOURCE` support against the **specific Fabric Spark
> runtime version** you are targeting before relying on it. This guide flags it as
> "verify" rather than asserting support.

Used by: [04-tsql-to-spark-patterns.md](04-tsql-to-spark-patterns.md).

---

## Data movement and orchestration

| Topic | Source |
|---|---|
| OneLake shortcuts | https://learn.microsoft.com/fabric/onelake/onelake-shortcuts |
| Data Factory pipelines in Fabric | https://learn.microsoft.com/fabric/data-factory/create-first-pipeline-with-sample-data |
| Copy activity | https://learn.microsoft.com/fabric/data-factory/copy-data-activity |
| Mirroring in Fabric | https://learn.microsoft.com/fabric/mirroring/overview |
| Deployment pipelines | https://learn.microsoft.com/fabric/cicd/deployment-pipelines/intro-to-deployment-pipelines |
| Fabric Git integration | https://learn.microsoft.com/fabric/cicd/git-integration/intro-to-git-integration |

Used by: [03-migration-playbook.md](03-migration-playbook.md),
[07-validation-and-cutover.md](07-validation-and-cutover.md),
[08-reference-architecture.md](08-reference-architecture.md).

---

## Capacity, monitoring, and cost

| Topic | Source |
|---|---|
| Fabric capacity metrics app | https://learn.microsoft.com/fabric/enterprise/metrics-app |
| Fabric capacity and SKUs | https://learn.microsoft.com/fabric/enterprise/licenses |
| Monitoring hub | https://learn.microsoft.com/fabric/admin/monitoring-hub |
| Query insights in Fabric Warehouse | https://learn.microsoft.com/fabric/data-warehouse/query-insights |
| Workspace monitoring | https://learn.microsoft.com/fabric/fundamentals/workspace-monitoring-overview |

Used by: [01-decision-framework.md](01-decision-framework.md) §5,
[06-operations-and-performance.md](06-operations-and-performance.md).

---

## How to re-verify this guide

1. Re-read the **decision guide** and the **SQL analytics endpoint** articles first —
   they anchor the central constraint (read-only endpoint).
2. Re-check anything labelled **preview** in this guide: the new metadata sync, the
   Lakehouse Maintenance activity, and PySpark authoring for materialized lake views.
3. Re-check **Direct Lake** guardrail values against your capacity SKU; this guide
   deliberately references them generically rather than quoting per-SKU numbers.
4. Re-check the **OneLake security** articles for new feature support, particularly
   whether dynamic data masking has been added to user identity mode.
5. Update the "Last reviewed" line in [README.md](README.md).
