# 06 — Operations and Performance

The Warehouse manages its physical layout for you. The Lakehouse does not. This is the
largest ongoing operational change in the migration, and it is permanent.

---

## 1. Layout ownership shifts to you

| Data store | Writer | Layout and maintenance ownership |
|---|---|---|
| Warehouse | T-SQL engine | **System-managed** |
| Lakehouse | Spark | **User-managed** |

| Condition | Warehouse action | Lakehouse action |
|---|---|---|
| Deletion-vector accumulation | No action — system-managed | Keep auto compaction enabled; schedule `OPTIMIZE`; use `REORG TABLE ... APPLY (PURGE)` only for explicit purge requirements |
| Poor file skipping | Configure warehouse data clustering | Configure liquid clustering, or Z-Order for an existing partitioned table |
| Direct Lake transcoding overhead | Keep V-Order enabled; evaluate data clustering | Compact small files, review row groups, apply V-Order to Spark-written tables |
| Unreferenced file storage growth | No action — cleanup is system-managed | Run `VACUUM` per your retention requirement |

---

## 2. Table maintenance

Table maintenance applies **only to Delta tables**. Legacy Hive tables using Parquet,
ORC, AVRO, or CSV are not supported.

### 2.1 `OPTIMIZE` (bin-compaction)

Compacts many small Parquet files into fewer, larger ones. Run it after major ingestion
or update activity, or when you observe many small files and degrading read performance.

```sql
OPTIMIZE silver.fact_order;

-- With Z-Order on high-selectivity predicate columns
OPTIMIZE silver.fact_order ZORDER BY (order_date, customer_id);
```

### 2.2 V-Order

V-Order applies optimized sorting, encoding, and compression as part of `OPTIMIZE`.

| Effect | Magnitude |
|---|---|
| Write-time cost | ~15% impact on average write times |
| Compression gain | Up to 50% more compression |
| Read gain, SQL analytics endpoint and Warehouse | ~10% improvement |

**Decision rule:** data read **once** benefits more from V-Order **off**; data read
**many times** benefits more from V-Order **on**.

| Layer | V-Order recommendation |
|---|---|
| Bronze — landed once, transformed once | **Off** |
| Silver — intermediate, read a few times | Depends; measure |
| Gold — served to Direct Lake, SQL endpoint, Warehouse | **On** — recommended write format for Direct Lake consumption |

For Dataflow Gen2, the destination-level **Enable use of V-Order compression** option
controls writes to the destination lakehouse; the dataflow-level **Enable V-Order
compression** option on the Scale tab controls the staging lakehouse.

### 2.3 `VACUUM`

Removes files no longer referenced by the Delta log, reclaiming storage and reducing
read overhead.

```sql
VACUUM silver.fact_order RETAIN 168 HOURS;   -- 7 days
```

> ⚠️ **Retention shorter than seven days impacts Delta time travel and can cause reader
> failures or table corruption** if snapshots or uncommitted files are still in use. The
> Fabric UI and REST APIs **reject retention periods under seven days by default.** To
> allow a shorter interval you must set
> `spark.databricks.delta.retentionDurationCheck.enabled` to `false` in workspace
> settings. Do this only with a clear understanding of the consequences.

Because `VACUUM` destroys time-travel history, and time travel is your primary
cross-table rollback mechanism, **retention policy and rollback policy are the same
decision.** If your rollback window is 14 days, your `VACUUM` retention must be at least
14 days.

### 2.4 Ways to run maintenance

| Method | Use when |
|---|---|
| Lakehouse explorer → table → **Maintenance** | Ad hoc, one table |
| **Lakehouse Maintenance activity** in a Data Factory pipeline (preview) | Scheduled, orchestrated; chains with data loads and a **Refresh SQL analytics endpoint** activity in the same pipeline |
| Spark notebook running `OPTIMIZE` / `VACUUM` | Code-first, custom logic, table loops |
| **Table Maintenance REST API** | Automation outside Fabric; asynchronous — submit, then poll the job instance |

The REST API submit is lakehouse-scoped, but job-instance polling uses the generic item
job endpoint (`/items/{itemId}/jobs/instances/{jobInstanceId}`) by design. Possible
statuses: `NotStarted`, `InProgress`, `Completed`, `Failed`, `Canceled`, `Deduped`.

### 2.5 Recommended baseline schedule

| Job | Frequency | Scope |
|---|---|---|
| `OPTIMIZE` (with V-Order for gold) | After each major load, or nightly | Tables with high update/delete churn |
| `OPTIMIZE ... ZORDER BY` | Weekly | Large fact tables with selective predicates |
| `VACUUM` | Weekly | All tables, retention ≥ your rollback window |
| `REORG TABLE ... APPLY (PURGE)` | On demand | Only for explicit purge requirements |
| SQL endpoint metadata refresh | End of every pipeline that writes | Tables consumed by SQL or Direct Lake |

Tune from measurement, not from this table. Start here, then adjust on evidence from
`DESCRIBE HISTORY` and storage growth.

---

## 3. SQL analytics endpoint metadata sync

This is the behaviour with no Warehouse equivalent, and the most common post-migration
surprise.

### How it works

A background process reads the Delta logs from the `Tables/` folder in OneLake and keeps
the SQL schema up to date. It handles:

1. **Table discovery** — detecting newly created or dropped Delta tables.
2. **Data freshness** — detecting inserts, updates, and deletes in existing tables.
3. **Schema change detection** — detecting column additions, removals, and type changes.

### What causes latency

| Cause | Mitigation |
|---|---|
| **Many lakehouses in one workspace.** Automatic metadata discovery is a **single instance per Fabric workspace.** | Migrate lakehouses to separate workspaces so discovery scales. |
| **Small-file accumulation.** Updates and deletes add Parquet files; unmaintained tables build read overhead that slows sync. | Schedule regular table maintenance. |
| **Very large volume of table changes during ETL.** | Expect a delay until all changes are processed; schedule consumption accordingly. |
| **Unsupported Delta features.** The automatic sync process does not support all Delta features. | Check Delta Lake table format interoperability for your feature set. |

### Forcing a refresh

Three options when changes are not yet visible:

1. On-demand metadata sync in the Fabric portal.
2. The **Refresh SQL analytics endpoint metadata** REST API.
3. A T-SQL stored procedure.

In pipelines, use the **Refresh SQL endpoint activity** as the last step after any write.

### New metadata sync (preview)

A preview option announced in May 2026 that keeps data queryable within **seconds** of
landing:

- External-tables-based architecture for parsing Delta logs.
- Decoupled schema-change and data-change detection.
- Periodic background refresh plus on-demand refresh triggered by an incoming read when
  data is detected as stale.
- Enabled per workspace under **Workspace settings → Warehouse settings**.
- Applies to **new** SQL analytics endpoints only; existing endpoints stay on legacy sync.

**Limitations:** does not support multi-part checkpoint (a deprecated Delta feature) —
affected tables fail to update; cannot currently be enabled on workspaces using workspace
private link.

Monitor with `sys.dm_db_external_tables_log_status`:
`last_update_time_utc`, `latest_log_version`, `latest_checkpoint_version`, `is_blocked`.

---

## 4. Direct Lake operations

### 4.1 Framing

A Direct Lake refresh copies only **metadata** (framing), not data — it analyses the
latest Delta table metadata and updates references to the latest files in OneLake. It
takes seconds and is low cost, unlike an Import refresh.

**Operational rule:** refresh (frame) the semantic model **after** you create or modify
the underlying Delta tables. An unframed model serves the previous version.

### 4.2 Staying in Direct Lake mode

Queries remain in Direct Lake mode (no DirectQuery fallback) only when **all** of these
hold:

1. No referenced table has SQL **RLS** defined at the SQL analytics endpoint.
2. No referenced table has SQL **DDM** defined at the endpoint.
3. No referenced table has SQL **OLS** defined at the endpoint.
4. No referenced table is based on an **unmaterialized SQL view**.
5. **No single table exceeds the guardrail limits** for your capacity SKU:
   - number of Parquet files,
   - number of row groups,
   - number of rows.
6. You have **framed** the model after creating or modifying the Delta tables.

> ⚠️ A single table that exceeds any guardrail prevents Direct Lake mode for the
> **entire model.**

Control the behaviour with the **DirectLakeBehavior** property — note it applies only to
Direct Lake on SQL analytics endpoints. Direct Lake on OneLake runs exclusively in
`DirectLakeOnly` and does not fall back.

### 4.3 Transcoding efficiency

Excessive files, small row groups, or broad retranscoding after updates all increase
Direct Lake overhead. Use Delta Analyzer to inspect. Remedies: compact small files,
review row group sizes, apply V-Order to Spark-written tables, and optionally configure
liquid clustering to improve compression quality within Parquet files.

### 4.4 Region constraint

Creating a Direct Lake semantic model in a workspace in a **different region** from the
data source workspace is not supported. Workaround: create a lakehouse in the other
region's workspace, shortcut to the tables, then build the model there.

---

## 5. Capacity and cost

### New CU line items after migration

| Item | Was | Becomes |
|---|---|---|
| Transformation compute | Warehouse autonomous compute (background) | **Spark pool** compute, including session start-up |
| Layout maintenance | System-managed, invisible | **Explicit `OPTIMIZE`/`VACUUM` jobs** consuming background CU |
| Metadata sync | n/a | Background cost, grows with table count and churn |
| Query serving | Warehouse interactive CU | SQL endpoint interactive CU (same engine) |
| Semantic model | Import refresh or Direct Lake | Direct Lake framing (cheap) + potential DirectQuery fallback (expensive) |

### Practical guidance

- **Baseline before you migrate.** Capture CU by operation class in the Fabric Capacity
  Metrics app for a representative period.
- **Budget for a migration spike.** Backfill and dual-run temporarily double your ETL CU.
  Plan the window, or temporarily scale the capacity.
- **Spark session reuse matters.** Many small notebook runs each paying session start-up
  is a real and avoidable cost. Consolidate related work into fewer, longer sessions,
  or use high-concurrency sessions.
- **Watch DirectQuery fallback rate.** A model that silently falls out of Direct Lake is
  both slow and expensive. Alert on it.
- **Storage is not free.** Unreferenced files from updates and deletes accumulate until
  `VACUUM` runs. Compare OneLake storage growth against active table size.

---

## 6. Monitoring checklist

Stand this up **before** cutover, not after.

| Signal | Source | Alert threshold |
|---|---|---|
| Pipeline/notebook failures | Monitoring hub | Any failure |
| End-to-end refresh duration | Pipeline run history | > agreed SLA at p95 |
| Small-file count per large table | `DESCRIBE DETAIL` / Delta Analyzer | Rising trend over 7 days |
| Deletion vectors added vs. removed | `DESCRIBE HISTORY` metrics | Vectors accumulating faster than compaction removes them |
| OneLake storage vs. active table size | Capacity Metrics | Storage growing materially faster than data |
| SQL endpoint sync lag | `sys.dm_db_external_tables_log_status` (new sync) or write-then-read probe | > agreed freshness SLA |
| Direct Lake fallback events | Semantic model refresh history / query logs | Any sustained fallback |
| CU utilisation and throttling | Capacity Metrics | Approaching SKU limit |
| Maintenance job outcomes | Job instance status | `Failed`, or repeated `Deduped` |

---

## 7. Operational runbook skeleton

| Scenario | First check | Action |
|---|---|---|
| "The report shows stale data" | Was the model framed after the last load? | Frame the model; add a framing step to the pipeline |
| "A new table is missing from SQL" | Is it Delta, and is it under `Tables/`? | Convert/move; then force a metadata refresh |
| "A Spark-created external table is missing from SQL" | Is it an external Delta table? | Create a shortcut in the `Tables` section |
| "Queries got slow overnight" | Small-file count, deletion vectors, Direct Lake fallback | Run `OPTIMIZE`; investigate fallback cause |
| "Storage cost jumped" | Unreferenced files | Run `VACUUM` at the agreed retention |
| "Columns stopped appearing after a schema change" | Is there a foreign key constraint on the endpoint table? | Drop the FK constraint; FKs block further schema changes |
| "Sync lag is rising across the workspace" | How many lakehouses are in this workspace? | Split lakehouses across workspaces |
| "A shortcut query fails in delegated mode" | Does the source table have OneLake RLS/CLS/OLS? | Switch to user identity mode, or remove source rules; then verify item owner access |
