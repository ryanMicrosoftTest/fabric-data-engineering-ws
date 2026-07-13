# Querying in Fabric

- The optimizer considers join order, data movement, CPU, memory, network cost, metadata, and statistics
- The warehouse is a massively parallel processing system, but not every operation parallelizes equally

Warehouse elapsed time = compile / optimize time + data access time + distributed processing time + data movement time + single-node finishing time + client/result transfer time + capacity queueing or throttling effects

When a warehouse query is slow, it is usually because one or more of these is true. The summary table below points to a dedicated section for each cause with concrete *how to identify* and *how to resolve* steps.

| Cause | Typical Symptom | How to Identify (quick) | How to Resolve (quick) |
|---|---|---|---|
| [Bad cardinality estimates](#bad-cardinality-estimates) | Poor join order, excessive data movement, unexpected spills or long joins | Big gap between estimated vs actual rows in the plan; missing/stale `sys.stats`; long automatic-stats build at first run | `CREATE`/`UPDATE STATISTICS ... WITH FULLSCAN`; fix data-type mismatches; precise `varchar(n)` lengths; pre-warm stats |
| [Too much data scanned](#too-much-data-scanned) | Large remote storage reads, slow scans, high CPU | High `data_scanned_remote_storage_mb` in `queryinsights.exec_requests_history`; `SELECT *`; missing predicates | Project only needed columns; push filters down; use clustering / partitioning; rely on warm cache; use result-set caching |
| [Inefficient physical layout](#inefficient-physical-layout) | Too many small files, poor clustering, weak row group / file skipping | Many small parquet files, small row groups, V-Order disabled, lots of deletion vectors | Run `OPTIMIZE` / let warehouse compaction run; target ~1 GB files & ~2 M-row groups; keep V-Order on for read-heavy WH |
| [Data type or join mismatch](#data-type-or-join-mismatch) | Implicit conversions, poor stats, non-sargable predicates | `CONVERT_IMPLICIT` in the plan; functions wrapping join/filter columns; mixed `varchar` / `nvarchar` | Align data types on both sides of joins; remove function wrappers from predicates; right-size string types |
| [Single-node bottleneck](#single-node-bottleneck) | Slow global sort, `TOP`, final aggregation, large result set | Warning *"One or more non-scalable operation is detected"*; long single-node finishing step | Reduce result size; filter earlier; consider `OPTION (FORCE DISTRIBUTED PLAN)`; avoid pulling huge result sets to client |
| [Capacity pressure](#capacity-pressure) | Queries slow broadly, operations delayed or rejected | `queryinsights.sql_pool_insights` shows pressure; throttling banner; Capacity Metrics app shows >100% CU and overage | Smooth load; resize SKU; offload to other capacity; use background ops; tune top CU consumers |
| [Transaction / write conflicts](#transaction--write-conflicts) | DML failures, lock waits, retry needs | `sys.dm_tran_locks` shows `Sch-M` blocking; long-running explicit txns; portal/catalog views slow | Keep transactions short; avoid long DDL inside txns; add retry with exponential backoff; serialize conflicting writers |
| [Semantic model / report pressure](#semantic-model--report-pressure) | Many Power BI visuals causing many warehouse queries | Burst of identical `query_hash` from a Power BI / DAX login; many small fast queries hitting same tables; Direct Lake fallback to DirectQuery | Reduce visuals per page; use aggregations or import; ensure Direct Lake (V-Order, large row groups, few files); enable result-set caching |

> Tip: use the `queryinsights.exec_requests_history`, `queryinsights.long_running_queries`, and `queryinsights.sql_pool_insights` views together with `sys.dm_exec_requests` / `sys.dm_exec_sessions` for live and historical signal.

---

## Specific Query Performance Investigation

Use this workflow when you already have a specific query you want to investigate — i.e. you don't need to find slow queries, you need to find out *why this one* is slow. Each step below points to the cause section(s) you should jump to once a signal lights up.

> All queries in this section have been executed end-to-end against a Fabric Warehouse SQL endpoint and verified to return the expected shape of results. The `queryinsights.exec_requests_history` view has roughly a 1–2 minute ingestion lag — if your query just finished, give it a moment before expecting it to show up in steps 2–4.

### Step 1 — Tag the query so you can find it later

Add an `OPTION (LABEL = '...')` hint. This lights the query up in `queryinsights.exec_requests_history.label` and makes every subsequent step trivial.

```sql
SELECT TOP 5 MedallionID, COUNT(*) AS trips
FROM dbo.Trip
GROUP BY MedallionID
ORDER BY trips DESC
OPTION (LABEL = 'PERF_INVESTIGATION_TRIP_TOP_MEDALLIONS');
```

If you can't modify the query (e.g. it comes from a BI tool), skip to Step 2 and search by command text instead.

### Step 2 — Find recent runs of the query

By label (preferred):

```sql
SELECT TOP 50
    distributed_statement_id,
    start_time,
    total_elapsed_time_ms,
    label,
    LEFT(command, 120) AS command_preview
FROM queryinsights.exec_requests_history
WHERE label LIKE 'PERF_INVESTIGATION%'
ORDER BY start_time DESC;
```

By command text (when you can't tag):

```sql
SELECT TOP 50
    distributed_statement_id,
    start_time,
    total_elapsed_time_ms,
    status,
    row_count,
    LEFT(command, 200) AS command_preview
FROM queryinsights.exec_requests_history
WHERE command LIKE '%FROM dbo.Trip%'
  AND command LIKE '%MedallionID%'
ORDER BY start_time DESC;
```

Grab a `distributed_statement_id` from the row you care about — you'll feed it into Step 3.

### Step 3 — Pull the full diagnostic columns for one run

```sql
DECLARE @id uniqueidentifier = '<paste-distributed_statement_id-here>';

SELECT
    distributed_statement_id,
    submit_time, start_time, end_time,
    total_elapsed_time_ms,
    allocated_cpu_time_ms,
    data_scanned_remote_storage_mb,
    data_scanned_memory_mb,
    data_scanned_disk_mb,
    row_count,
    result_cache_hit,
    error_code, error_severity, error_state,
    status, label
FROM queryinsights.exec_requests_history
WHERE distributed_statement_id = @id;
```

How to read the output:

| Column | What it tells you | What "bad" looks like | Where to look next |
|---|---|---|---|
| `total_elapsed_time_ms` | Wall-clock duration | Slower than the user-facing SLA; or much higher than the median for the same `query_hash` in Step 4. There is no universal threshold — interactive BI queries usually want < 5 s; nightly ETL batches can legitimately run for hours. Compare against history, not against an absolute number. | (overall verdict — pick a step below based on the other columns) |
| `allocated_cpu_time_ms` | CPU work done across all nodes (can exceed `total_elapsed_time_ms` for parallel queries) | `cpu` ≪ `elapsed` (e.g. < 25% of elapsed) on a non-trivial query → time was spent waiting (I/O, queueing, locks), not computing. `cpu` ≈ `elapsed` on a small query → single-threaded finishing step. See the *Reading the elapsed-vs-CPU gap* tables above. | [Capacity pressure](#capacity-pressure), [Single-node bottleneck](#single-node-bottleneck), [Transaction / write conflicts](#transaction--write-conflicts) |
| `data_scanned_remote_storage_mb` | Cold reads from OneLake | High and *roughly the size of the whole table* (or larger if joins fan out) — means no pruning happened. Or: still high on the **second** run of the same query, which means the cache isn't warming. Use `sys.dm_db_partition_stats` or `sp_spaceused` on the table to know what "the whole table" is in MB. | [Too much data scanned](#too-much-data-scanned), [Inefficient physical layout](#inefficient-physical-layout) |
| `data_scanned_memory_mb` / `data_scanned_disk_mb` | Warm cache reads (in-memory cache, then on-disk SSD cache) | Stays near zero across repeated runs of the same query (cache never warms — usually data churn, recompiles, or per-run table replacement). Or `disk_mb` dominates `memory_mb` consistently — working set doesn't fit in the in-memory cache. | [Inefficient physical layout](#inefficient-physical-layout), [Capacity pressure](#capacity-pressure) |
| `row_count` | Rows returned to the client | Millions of rows for an interactive query — the cost of the final single-node step (sort, dedupe, return) usually dominates everything else. Or: much larger than you expected for your `WHERE` clause — predicate isn't being pushed down. | [Single-node bottleneck](#single-node-bottleneck), [Data type or join mismatch](#data-type-or-join-mismatch) |
| `result_cache_hit` | `1` = served from result-set cache (~ms response, no compute) | `0` when you expected `1` (e.g. you just re-ran the exact same query). Common invalidators: any DML against a referenced table, schema change, statistics update, non-deterministic functions like `GETDATE()` in the query, or a different login/session context. | (expected behaviour — but if it never hits, investigate cache invalidation) |
| `error_code` / `error_severity` / `error_state` | Whether the query actually finished | Anything non-zero. Severity 16+ = user-correctable (bad SQL, perm denied); 17–19 = resource issue; 20+ = fatal. The query never produced results — look up the error number on Microsoft Learn first; don't waste time on perf analysis of a failed run. | (read the error first, then re-run with the fix) |

A common pattern: first-run heavy in `remote_storage_mb`, second-run mostly `memory_mb`, third-run `result_cache_hit = 1`. If runs *don't* warm up like that, something is invalidating the cache (data churn, parameter sniffing, schema change).

#### Reading the elapsed-vs-CPU gap

`total_elapsed_time_ms` is wall-clock that the user waited. `allocated_cpu_time_ms` is the sum of CPU time consumed across **all** compute nodes. The two are deliberately different metrics — and the gap between them tells you where the time went.

The non-CPU portion of wall-clock time can come from any of these:

| Where the time goes | What's happening |
|---|---|
| Compilation / plan generation | Parsing, binding, optimization, plan-cache lookup. First-time or recompiled queries pay this; cache hits skip most of it. |
| Distributed orchestration | The frontend / DMS coordinator splitting the query into tasks, dispatching them to compute nodes, waiting for results. |
| I/O wait | Time blocked on reads from OneLake / cache / disk. The bytes read show up in `data_scanned_*_mb`; the wait on those reads is wall-clock, not CPU. |
| Data movement / shuffles | When intermediate data is exchanged between nodes, network transfer is wall-clock, not CPU. |
| Result streaming | Serializing rows and sending them over TDS to the client. Big `row_count` → bigger gap. |
| Queueing under capacity pressure | If the pool is busy, the query can sit queued — wall-clock burns, no CPU spent. Cross-check `queryinsights.sql_pool_insights`. |
| Locks / blocking | Waiting on `Sch-S`, `Sch-M`, etc. Surfaces in `sys.dm_tran_locks` while the query is running. |

Note that `allocated_cpu_time_ms` can legitimately *exceed* `total_elapsed_time_ms` for highly parallel queries — 4 nodes working 50 ms each = 200 ms CPU consumed in 50 ms of wall clock. That's healthy parallelism, not a bug.

How to interpret the ratio:

| Ratio | Interpretation | Where to look next |
|---|---|---|
| `cpu` ≪ `elapsed` (e.g. 50 ms CPU, 5000 ms elapsed) | I/O bound, queued, or blocked — most of the wait was *not* doing real work | [Too much data scanned](#too-much-data-scanned), [Capacity pressure](#capacity-pressure), [Transaction / write conflicts](#transaction--write-conflicts) |
| `cpu` ≈ `elapsed` on a small/single query | CPU-bound on a single node — common for final sorts, `TOP`, single-stream aggregation | [Single-node bottleneck](#single-node-bottleneck) |
| `cpu` > `elapsed` | Healthy parallelism — multiple nodes working concurrently | (no action — the engine is doing what it should) |
| `cpu` modestly < `elapsed` (e.g. 119 ms / 219 ms on a small query) | Normal: compile + dispatch + result-return overhead dominates for tiny queries | (no action — typical small-query profile) |

### Step 4 — Compare runs of the same query over time

`exec_requests_history` includes a stable `query_hash` per statement shape — group on it to see variance across all runs of the *same* query.

```sql
SELECT TOP 20
    query_hash,
    COUNT(*) AS run_count,
    AVG(total_elapsed_time_ms) AS avg_elapsed_ms,
    MIN(total_elapsed_time_ms) AS min_elapsed_ms,
    MAX(total_elapsed_time_ms) AS max_elapsed_ms,
    AVG(data_scanned_remote_storage_mb) AS avg_remote_mb,
    SUM(CASE WHEN result_cache_hit = 1 THEN 1 ELSE 0 END) AS cache_hits
FROM queryinsights.exec_requests_history
WHERE label LIKE 'PERF_INVESTIGATION%'
   OR command LIKE '%FROM dbo.Trip%MedallionID%'
GROUP BY query_hash
ORDER BY avg_elapsed_ms DESC;
```

Big spread between `min_elapsed_ms` and `max_elapsed_ms` for the same `query_hash` usually means either (a) cache state variance, (b) capacity pressure on some runs, or (c) parameter values that hit very different data volumes.

The two pre-aggregated views give you the same picture without writing the GROUP BY:

```sql
SELECT TOP 20 *
FROM queryinsights.frequently_run_queries
ORDER BY number_of_runs DESC;

SELECT TOP 20 *
FROM queryinsights.long_running_queries
ORDER BY median_total_elapsed_time_ms DESC;
```

### Step 5 — Capture the execution plan

`SHOWPLAN_XML` gives you the estimated plan including operators, estimated row counts, and warnings (like *"One or more non-scalable operation is detected"*).

> **Important**: `SET SHOWPLAN_XML ON` must be the **only statement in its batch**. No `PRINT`, no `SET NOCOUNT ON`, no anything else before the next `GO`. Run it as a clean script:

```sql
SET SHOWPLAN_XML ON;
GO

SELECT TOP 5 MedallionID, COUNT(*) AS trips
FROM dbo.Trip
GROUP BY MedallionID
ORDER BY trips DESC;
GO

SET SHOWPLAN_XML OFF;
GO
```

The result is a single `<ShowPlanXML>` row. Skim it for:

* `<Warnings>` blocks — typically point at [single-node bottleneck](#single-node-bottleneck) or [bad cardinality estimates](#bad-cardinality-estimates).
* Big mismatches between estimated and actual row counts (when you have actual stats from a previous run) — see [bad cardinality estimates](#bad-cardinality-estimates).
* `CONVERT_IMPLICIT` in the plan text — see [data type or join mismatch](#data-type-or-join-mismatch).
* Operators with very large estimated sizes feeding a single-node finishing step — see [single-node bottleneck](#single-node-bottleneck).

### Step 6 — If the query is still running right now

Live state via DMVs (note: DMVs are session-scoped — only your own work shows full text without elevated permissions):

```sql
SELECT
    r.session_id,
    r.request_id,
    r.start_time,
    r.status,
    r.command,
    r.cpu_time,
    r.total_elapsed_time,
    r.reads,
    r.writes,
    LEFT(t.text, 200) AS sql_text_preview
FROM sys.dm_exec_requests r
OUTER APPLY sys.dm_exec_sql_text(r.sql_handle) t
ORDER BY r.total_elapsed_time DESC;
```

If the query is sitting in `suspended` status with a non-NULL `wait_type`, it's waiting on something — most often a lock. Check Step 6b.

#### Step 6b — Check for blocking

```sql
SELECT
    l.request_session_id,
    l.resource_type,
    l.resource_database_id,
    l.resource_associated_entity_id,
    l.request_mode,
    l.request_status,
    s.login_name,
    s.program_name
FROM sys.dm_tran_locks l
LEFT JOIN sys.dm_exec_sessions s
    ON s.session_id = l.request_session_id
ORDER BY l.request_session_id;
```

Look for `request_status = 'WAIT'` rows and the `request_mode` of the blocker (often `Sch-M` from a concurrent DDL/MERGE) — see [transaction / write conflicts](#transaction--write-conflicts).

### Step 7 — Check stats freshness on the tables the query touches

Stale stats are one of the most common silent causes of slow queries. List the stats objects on every table referenced by your query and look at `last_updated`:

```sql
SELECT
    t.name AS table_name,
    s.name AS stat_name,
    c.name AS column_name,
    s.auto_created,
    s.user_created,
    sp.last_updated
FROM sys.stats s
JOIN sys.tables t ON s.object_id = t.object_id
LEFT JOIN sys.stats_columns sc
    ON sc.object_id = s.object_id AND sc.stats_id = s.stats_id
LEFT JOIN sys.columns c
    ON c.object_id = sc.object_id AND c.column_id = sc.column_id
CROSS APPLY sys.dm_db_stats_properties(s.object_id, s.stats_id) sp
WHERE t.name IN ('Trip')   -- list every table your query references
ORDER BY t.name, s.name;
```

If `last_updated` predates a recent large load, or is `NULL` on stats that should be relevant (e.g. on a column you filter or join on), jump to [bad cardinality estimates](#bad-cardinality-estimates) — that section covers `CREATE STATISTICS ... WITH FULLSCAN`, `UPDATE STATISTICS`, and `DBCC SHOW_STATISTICS`.

### Step 8 — Rule out capacity pressure

Even a perfectly written query will be slow when the capacity is throttled or queueing.

```sql
SELECT TOP 20
    sql_pool_name,
    timestamp,
    max_resource_percentage,
    is_optimized_for_reads,
    is_pool_under_pressure
FROM queryinsights.sql_pool_insights
ORDER BY timestamp DESC;
```

`is_pool_under_pressure = 1` or sustained `max_resource_percentage` near or above 100 around the time your query ran means you have a capacity problem, not a query problem — see [capacity pressure](#capacity-pressure).

### Putting it together — quick triage flowchart

| Signal you found | Most likely cause section |
|---|---|
| Plan shows huge estimate vs actual row mismatch, or stats `last_updated` is old | [Bad cardinality estimates](#bad-cardinality-estimates) |
| Very high `data_scanned_remote_storage_mb` even on warm runs | [Too much data scanned](#too-much-data-scanned) |
| Many small files, low cache reuse, scans dominate | [Inefficient physical layout](#inefficient-physical-layout) |
| Plan contains `CONVERT_IMPLICIT` or function-wrapped predicates | [Data type or join mismatch](#data-type-or-join-mismatch) |
| Plan warning about non-scalable operation, large `row_count`, or huge final sort/agg | [Single-node bottleneck](#single-node-bottleneck) |
| `is_pool_under_pressure = 1` at the run's `start_time` | [Capacity pressure](#capacity-pressure) |
| `sys.dm_tran_locks` shows blockers (`Sch-M`, long `WAIT`) | [Transaction / write conflicts](#transaction--write-conflicts) |
| Burst of identical `query_hash` from a Power BI session | [Semantic model / report pressure](#semantic-model--report-pressure) |

---

### Bad cardinality estimates

The query optimizer enumerates candidate plans and picks the cheapest one based on estimated rows at each operator. When those estimates are wrong, the chosen join order, join algorithm, and data-movement strategy are wrong too — the symptom is "the plan looks weird", spills, and over-shuffled data.

### How to identify

- Capture the plan with `SET SHOWPLAN_XML ON` (or via SSMS / Azure Data Studio "Estimated Plan") and look for operators where **estimated rows ≪ actual rows** (or vice versa). A 10×+ gap is a strong signal.
- Inspect statistics objects. First, discover what stats already exist on the table — the second argument to `DBCC SHOW_STATISTICS` is the **statistics object name** (not the column name), so you need to know its name before you can inspect it:
  ```sql
  -- List all stats on a table, including the column they cover and when they were last refreshed.
  SELECT
      s.name              AS stat_name,
      c.name              AS column_name,
      s.auto_created,
      s.user_created,
      STATS_DATE(s.object_id, s.stats_id) AS last_updated
  FROM sys.stats s
  JOIN sys.stats_columns sc ON s.object_id = sc.object_id AND s.stats_id = sc.stats_id
  JOIN sys.columns       c  ON sc.object_id = c.object_id AND sc.column_id = c.column_id
  WHERE OBJECT_NAME(s.object_id) = 'DimCustomer'
  ORDER BY s.name;

  -- Then inspect a specific stat's histogram, density vector, and header.
  DBCC SHOW_STATISTICS ('dbo.DimCustomer', 'DimCustomer_CustomerKey_FullScan');
  ```
  Statistics object names come from one of three places:
  1. **You created them explicitly** with `CREATE STATISTICS <name> ON ...` — you choose the name (convention: `<Table>_<Column>_<Modifier>`, e.g. `DimCustomer_CustomerKey_FullScan`).
  2. **Fabric auto-created them** the first time a query needed them. Look for system-generated names like `ACE-Cardinality` (table cardinality), `ClusteredIndex` (from the clustered index), or `_WA_Sys_<colId>_<objId>` (auto histogram on a column).
  3. **An index created them implicitly** — every index has a stats object with the same name as the index.
- Check whether the first execution of a query was slow because automatic stats were being built synchronously — Fabric Warehouse auto-creates and auto-refreshes histogram, average column length, and table cardinality stats at query time, and that work is added to the duration of the triggering query.
- Look at `data_scanned_remote_storage_mb` in `queryinsights.exec_requests_history` to confirm whether you're also paying a cold-start cost on top of the bad estimate.

### How to resolve

- Pre-build stats during a maintenance window so user `SELECT`s don't pay the synchronous cost:
  ```sql
  CREATE STATISTICS DimCustomer_CustomerKey_FullScan
      ON dbo.DimCustomer (CustomerKey) WITH FULLSCAN;
  ```
  **Which columns deserve a `FULLSCAN` stat?** Don't blanket every column — focus on the ones the optimizer actually uses to size operators:

  | Column type | Why it matters |
  |---|---|
  | **Surrogate / primary keys on dimensions** (e.g. `DimCustomer.CustomerKey`) | Drives every fact-to-dim join cardinality estimate. Bad estimate here = wrong join algorithm and wrong distribution strategy. |
  | **Foreign keys on fact tables** (e.g. `FactSales.CustomerKey`) | Same as above, but on the high-cardinality side where row counts are huge. |
  | **Filter columns in `WHERE` clauses** (e.g. `OrderDate`, `Region`, `Status`) | Optimizer needs accurate selectivity to decide predicate pushdown, partition elimination, and join order. |
  | **`GROUP BY` columns** | Helps size the aggregation operator and decide hash vs sort aggregation. |
  | **Skewed columns** (e.g. `Country` where 80% of rows = `'US'`) | Default sampled stats often miss skew; `FULLSCAN` captures the true distribution. |

  The `WITH FULLSCAN` option matters as much as the column choice — it tells Fabric to read every row instead of sampling, producing an *exact* histogram rather than an estimate. Use Query Insights to find the columns your slow queries actually touch:
  ```sql
  -- Find the join/filter columns of your slow queries, then create FULLSCAN stats on those.
  SELECT TOP 20 query_hash, command, total_elapsed_time_ms
  FROM queryinsights.exec_requests_history
  WHERE total_elapsed_time_ms > 5000
  ORDER BY total_elapsed_time_ms DESC;
  ```
- After large loads, refresh stats explicitly:
  ```sql
  UPDATE STATISTICS dbo.DimCustomer (DimCustomer_CustomerKey_FullScan) WITH FULLSCAN;
  ```
- Eliminate root causes that distort estimates: data-type mismatches, oversized `varchar(max)` / `varchar(8000)` columns, and function-wrapped predicates (see [Data type or join mismatch](#data-type-or-join-mismatch)).
- For Lakehouse tables read via the SQL analytics endpoint, enable Spark-side automated table statistics so ACE-Cardinality is accurate: `spark.conf.set("spark.databricks.delta.stats.collect", "true")` and use Spark 3.5+ to get rowgroup-level stats on timestamp columns.

---

## Too much data scanned

MPP scanning is fast, but the cheapest scan is the one you don't do. Scanning more bytes than needed inflates I/O, CPU, memory, and network, and crowds out the cache.

### How to identify

- Query insights first. Anything materially non-zero in `data_scanned_remote_storage_mb` means the engine had to go back to OneLake instead of serving from the in-memory + SSD cache:
  ```sql
  SELECT TOP 50 distributed_statement_id, query_hash,
         data_scanned_remote_storage_mb, data_scanned_memory_mb, data_scanned_disk_mb,
         label, command
  FROM queryinsights.exec_requests_history
  ORDER BY data_scanned_remote_storage_mb DESC;
  ```
- Look for `SELECT *`, missing `WHERE` clauses, and predicates that can't be pushed to Parquet (functions on columns, mismatched types).
- Check whether you're judging cold-start performance. The first execution after a node spin-up or first access of a table will hit OneLake; second/third executions are the fair benchmark.
- Compare `data_scanned_remote_storage_mb` vs `data_scanned_memory_mb + data_scanned_disk_mb` over time to see cache hit ratio for the workload.

### How to resolve

- Project only the columns you need; avoid `SELECT *`, especially against wide tables.
- Push filtering into the warehouse, not the client. Filter on low-cardinality columns *before* joins, then join on high-cardinality columns.
- Use clustering (Liquid Clustering on Lakehouse) or partitioning on Lakehouse tables that drive your filters.
- Don't use enforced PK/UK as join hints — Fabric Warehouse doesn't enforce them, so they aren't a quality signal for join planning.
- For repeated identical queries, enable [result-set caching](https://learn.microsoft.com/en-us/fabric/data-warehouse/result-set-caching) so the second run avoids the scan entirely.

---

## Inefficient physical layout

Fabric reads Delta + Parquet. Lots of tiny files, tiny row groups, lots of deletion vectors, or V-Order off on a read-heavy warehouse all defeat row-group / file skipping and inflate metadata work.

### How to identify

- For Warehouse-written tables, layout is system-managed; problems usually show up only when *Spark or mirroring* writes the underlying Delta. For SQL analytics endpoint and Warehouse reads, the targets are: max ~4 GB per file, ~2 M rows per row group, V-Order on. Direct Lake prefers ≥8 M rows per row group and few files.
- Inspect the Parquet/Delta on OneLake: file count vs total size, row-group count, presence of deletion vectors. A useful smoke test is "table size ÷ file count" — if it's MBs not hundreds of MBs, you have a small-file problem.
- Symptoms in queries: high CPU per row scanned, slow `SELECT TOP n` from Lakehouse tables, slow SQL endpoint metadata sync (background scanner has to walk many files), and long tail latency on otherwise small queries.

### How to resolve

- For Lakehouse / Spark-written tables, run table maintenance (`OPTIMIZE` and `VACUUM`) on a schedule. The Fabric portal exposes this from the Lakehouse item, or run it from a Spark notebook.
- For Spark writers, turn on optimize-write and auto-compact so files land at the right size:
  ```python
  spark.conf.set('spark.databricks.delta.optimizeWrite.enabled', 'true')
  spark.conf.set('spark.databricks.delta.autoCompact.enabled', 'true')
  ```
- Keep V-Order **on** for warehouses that serve SQL/Power BI. Disable it only for write-heavy staging warehouses where the data is read once or twice — and remember disabling V-Order at warehouse level is currently irreversible.
- For Warehouse tables, the system already compacts when file overhead > 10%, deleted rows > 10%, or files are < 25% of ideal size — make sure you're not blocking that with very long open transactions.
- For SQL analytics endpoint metadata lag, also reduce the number of lakehouses per workspace (the metadata scanner is a single instance per workspace) and run the on-demand Refresh metadata API after big lakehouse loads.

---

## Data type or join mismatch

Mismatched types force implicit conversions that often make predicates non-sargable, ruin row-group elimination, and degrade statistics quality. Long string types make the optimizer pessimistic about memory.

### How to identify

- In the execution plan, look for `CONVERT_IMPLICIT(...)` wrappers on join keys and `WHERE` columns. Hover the predicate node in the graphical plan.
- Look for joins between `varchar` and `nvarchar`, `int` and `bigint`, or numeric and string keys.
- Look for predicates like `WHERE CAST(col AS date) = ...`, `WHERE UPPER(col) = ...`, `WHERE LEFT(col,3) = ...` — anything that wraps the column.
- Check column definitions for `varchar(8000)` / `varchar(max)` where actual values are short. Lakehouse string columns without an explicit length are surfaced as `varchar(8000)` to the SQL endpoint.

### How to resolve

- Make join key types match exactly on both sides. If you can't match exactly, use compatible types so implicit conversion is at least cheap.
- Right-size strings: use the smallest `varchar(n)` that fits the data; declare lengths explicitly in Spark `CREATE TABLE` (`varchar(n)`) so the SQL endpoint sees the right length.
- Rewrite predicates to be sargable: filter on the raw column with literals already converted, e.g. `WHERE col = CAST('2024-01-01' AS date)` instead of wrapping `col`.
- After fixing types, refresh stats on the changed columns so the optimizer sees the new distribution.

---

## Single-node bottleneck

Some operators inherently funnel results to a single node — `TOP`, global `ORDER BY`, the final aggregation step, and the result-shipping step. When these dominate, more nodes won't help.

### How to identify

- The engine surfaces a warning: **"One or more non-scalable operation is detected"**. The query may run slowly or fail after a long execution.
- In the plan, look for a single-node finishing step that consumes most of the elapsed time.
- In `queryinsights.exec_requests_history`, look at the gap between distributed processing and total elapsed time, plus very large result sets returned to the client.

### How to resolve

- Reduce the size of the dataset reaching the single-node step: add filters, aggregate earlier, avoid unnecessary `ORDER BY` or `TOP n` over very large intermediate sets.
- If query semantics don't actually require single-node execution, force a distributed plan:
  ```sql
  SELECT ...
  FROM ...
  OPTION (FORCE DISTRIBUTED PLAN);
  ```
- Don't ship enormous result sets to the client. Aggregate, page (`OFFSET`/`FETCH`), or write to a table with `CTAS` and pull from there.
- Remember query hints assume you already know more than the optimizer — use them after the simpler fixes (stats, types, filters) haven't helped.

---

## Capacity pressure

Throttling is applied at the *capacity* level, not per workspace or per warehouse. Once the capacity has burned its 10-minute "overage protection", interactive operations get a 20-second delay (stage 1), then are rejected (stage 2), and finally everything including background ops is rejected (stage 3). Smoothing spreads CU usage over future timepoints — interactive ops over 5–64 minutes, background ops over 24 hours.

### How to identify

- Use `queryinsights.sql_pool_insights` to see pool-level pressure, configuration changes, and resource allocation. Correlate pressure spikes with `long_running_queries`.
- Use the **Microsoft Fabric Capacity Metrics** app to see CU% over time, smoothed vs raw consumption, overage usage, and which items / operations are the top consumers.
- Watch for the throttling stage indicators in the portal and for HTTP 429-style "capacity limit exceeded" errors from APIs and pipelines.
- Symptom across the capacity: queries that previously ran in seconds now take significantly longer or are rejected, and the slowdown affects unrelated workspaces sharing the capacity.

### How to resolve

- Smooth the demand: stagger heavy refreshes and ETL; let background smoothing (24 h) absorb large jobs by scheduling earlier.
- Tune the top CU consumers found in Capacity Metrics first — that's usually a small number of queries / refreshes.
- Right-size the SKU, or move noisy workloads to a separate capacity so they stop throttling other workspaces.
- For Spark, consider Autoscale Billing (Pay-As-You-Go), but note that bursting and smoothing don't apply in that mode.
- Add retry with exponential backoff in pipelines and apps so transient throttling doesn't fail jobs outright.

---

## Transaction / write conflicts

Fabric Warehouse uses snapshot isolation and table-level locking. DML takes Intent Exclusive (`IX`) on the table; DDL takes Schema-Modification (`Sch-M`), which is incompatible with everything — including readers' Schema-Stability (`Sch-S`) locks.

### How to identify

- Inspect locks live:
  ```sql
  SELECT * FROM sys.dm_tran_locks;
  ```
- Find long-running explicit transactions via `sys.dm_exec_requests` / `sys.dm_exec_sessions` (open since long ago, status `sleeping`/`running`).
- Symptoms: the Fabric portal feels slow on a warehouse (it reads system catalog views like `sys.tables`, which are blocked by an open `Sch-M`); pipelines fail intermittently; `MERGE`/`UPDATE` jobs queue behind each other.
- Check whether a writer is holding `Sch-M` because it has DDL bundled inside a long explicit transaction.

### How to resolve

- Keep transactions short. Always `COMMIT` or `ROLLBACK`. Don't leave them open across user think-time.
- Avoid bundling DDL with long DML inside the same explicit transaction; split them where business logic allows.
- Add retry-with-exponential-backoff to writers so transient conflicts don't surface as job failures.
- Serialize known conflicting writers (e.g., two pipelines that both `MERGE` into the same dim) with orchestration, not with table locks.
- Don't try to change the isolation level — Fabric Warehouse uses snapshot isolation and ignores attempts to change it.

---

## Semantic model / report pressure

A single Power BI page can fan out into many DAX queries → many warehouse / SQL endpoint queries. When the model is in DirectQuery, or when a Direct Lake model falls back to DirectQuery, the warehouse takes the full hit. Direct Lake performance is itself sensitive to the same Delta layout described in [Inefficient physical layout](#inefficient-physical-layout).

### How to identify

- In `queryinsights.exec_requests_history` / `frequently_run_queries`, look for bursts of identical `query_hash` values from a Power BI / Analysis Services service principal or login.
- Check the semantic model: is it Import, DirectQuery, or Direct Lake? For Direct Lake, check whether queries are falling back to DirectQuery (visible in Power BI diagnostics / Performance Analyzer; see *DirectQuery fallback* warnings).
- Many small fast queries hitting the same table are usually a visual count problem; many slow queries are usually a model / measure problem (unsupported DAX patterns, missing relationships, big cross-joins).
- Cross-reference with `sql_pool_insights` — report bursts often coincide with pressure events.

### How to resolve

- Reduce visuals per page; collapse multiple cards into one matrix; remove duplicate visuals.
- Prefer Import or Direct Lake for high-traffic reports. For Direct Lake, ensure the source Delta tables are V-Ordered, have large row groups (≥8 M rows) and few files — that's what keeps Direct Lake out of DirectQuery fallback.
- Move expensive DAX into measures that can be folded; remove unsupported patterns that force fallback.
- Turn on result-set caching on the warehouse so identical Power BI queries don't re-execute.
- For very heavy dashboards, build pre-aggregated Gold tables in the warehouse and point the model at those, not at the raw Silver/Bronze tables.


### Distributed Query Processing
- Common Distributed-Query pressure points include

|                 Pattern                    |       Why it can hurt                                           |
|------------------------------------------- | ----------------------------------------------------------------|
| Large Joins without selective predicates   | High shuffle/data movement                                      |
| Joining columns with mismatched data types | Conversions, worse estimates, reduced optimization opportunities|
| Global ORDER BY on large results           | Requires distributed sort plus final gather                     |
| SELECT * FROM wide tables                  | Scans and transfers unnecessary columns                         |
| Many small files in source Delta data      | File metadata and open/scan overhead                            |
| High-cardinality groupings                 | Large intermediate result sets                                  |
| Large result sets returned to client       | Client transfer becomes part of elapsed time                    |


### V-Order
V-Order is a write-time Parquet optimization in Fabric.  For warehouses, V-Order is enabled by default and improves query performance through sorting, encoding, and compression characteristics, with a small ingestion overhead.  Microsoft warns that once V-Order is disabled on a warehouse, it cannot be re-enabled for that warehouse.

For Spark/lakehouse writes, V-Order is especially important for read-hevy analytics tables and dashboards because it reorganizes row group distribution, encoding, and compression while preserving open Parquet compatibility.  Microsoft describes the average write overhead as about 15%

### Compaction and Checkpointing
Warehouse tables benefit from automatic compaction and checkpointing.  Compaction reduces the number of small files; checkpointing reduces the amount of Delta log metadata the engine must scan.  Microsoft notes that Fabric Warehouses automatically perform data compaction and checkpointing, and that checkpointing reduces metadata and Delta log scanning during queries.

For lakehosue tables, you usually have more responsibility for maintenance.  Use OPTIMIZE and where appropriate ZORDER or liquid clustering, to improve file skipping and data locality.  Microsoft recommends partitioning only when columns are low-cardinality and predictable, while clustering or ZORDER can help high-selectivity filters.


---

## Reading and analyzing execution plans (`.sqlplan`)

Once you've captured an execution plan via `SET SHOWPLAN_XML ON` (Step 5 of [Specific Query Performance Investigation](#specific-query-performance-investigation)) and opened the resulting `.sqlplan` file in SSMS or Azure Data Studio, you've got a graphical operator tree in front of you. This section is a practical guide to actually reading it — what every visual element means, what to look for, and how each finding maps back to a cause section.

> Fabric Warehouse is a distributed (MPP) engine, so its plans contain a mix of standard SQL Server operators (Hash Match, Sort, Filter) **and** distribution-specific operators (data movement, broadcast, shuffle). Both matter.

### Orientation: how to read the diagram

| Visual element | What it means |
|---|---|
| **Direction of flow** | Data flows **right-to-left, bottom-to-top**. The leaves on the right are the data sources (table/index access). The root on the far left is the operator that returns rows to the client (typically `SELECT`). |
| **Arrow thickness** | Proportional to the **number of rows** flowing between operators. A fat arrow narrowing to a thin arrow downstream is a classic sign that work was done late (filter/aggregate) instead of being pushed down. |
| **% next to each operator** | The estimated cost of *this operator* as a fraction of the total query cost. Concentrate your attention on the highest-percentage operators first — they're where time is being spent. |
| **Yellow ⚠ / red triangle on an operator** | A warning. Hover over the operator → the tooltip explains it. Common ones: missing statistics, spill to tempdb, no-join-predicate, type conversion affecting cardinality estimate, non-scalable operation. Always investigate. |
| **Operator tooltip** (hover) | Shows estimated rows, estimated row size, estimated I/O cost, estimated CPU cost, estimated subtree cost, predicate text, output list. This is where the real diagnostic information lives. |
| **Properties pane** (F4 in SSMS) | Same data as the tooltip but persistently visible and richer — scroll down for `EstimateRows`, `EstimatedRowsRead`, `EstimatedExecutionMode`, `Storage`, `IndexKind`, plus the `Warnings` collection. |

### Estimated vs actual

`SET SHOWPLAN_XML ON` gives you the **estimated** plan — what the optimizer thinks will happen, based on statistics. The query is *not* run, so there are no actual row counts, no spill measurements, no real timings.

To compare estimates against reality:

- For runtime metrics (elapsed time, CPU, MB scanned, rows returned), use `queryinsights.exec_requests_history` (Step 3 of the investigation workflow).
- For "actual" execution plans with real row counts per operator, Fabric Warehouse currently does not expose a built-in actual-plan capture the way classic SQL Server does. The closest substitute is: capture the estimated plan, then compare each operator's `EstimateRows` against the `row_count` and `data_scanned_*_mb` you actually observed.

A **large gap between estimated and actual rows** at any operator is the textbook smoking gun for [Bad cardinality estimates](#bad-cardinality-estimates). Anything more than ~10x off is worth investigating.

### Operators you'll see most often

#### Data access (the leaves)

| Operator | What it does | Notes for Fabric Warehouse |
|---|---|---|
| `Clustered Index Scan` / `Columnstore Index Scan` | Reads from a table | All Fabric Warehouse tables are clustered columnstore — every base table read is a columnstore scan. There are no traditional rowstore B-tree indexes to "seek". |
| `Filter` | Applies a `WHERE` predicate **after** a scan | Big arrow in, small arrow out = the filter happened *after* the scan instead of being pushed into it. See [Too much data scanned](#too-much-data-scanned). |
| `Compute Scalar` | Evaluates an expression per row (e.g. `a + b`, `CONVERT(...)`) | Look at the `Defined Values` property. If you see `CONVERT_IMPLICIT(...)`, you have a [Data type or join mismatch](#data-type-or-join-mismatch). |

#### Joins

| Operator | When it's chosen | Watch out for |
|---|---|---|
| `Hash Match (Inner Join / Left Outer Join / etc.)` | Default for medium/large joins. Builds a hash table on the smaller (build) side, probes with the larger (probe) side. | Hash table doesn't fit in memory → **Hash Spill** warning. The build side being far larger than estimated is usually the cause. |
| `Nested Loops` | Picked for very small outer inputs joined against an indexed inner. | Rare in Fabric Warehouse because there are no rowstore seek indexes. If you see one over a large outer, that's almost always a cardinality misestimate. |
| `Merge Join` | Both inputs already sorted on the join key. | Uncommon unless inputs come pre-sorted from a `Sort` upstream. |

#### Aggregation and ordering

| Operator | What it does | Watch out for |
|---|---|---|
| `Hash Aggregate` | `GROUP BY` via a hash table, no input order required. | High-cardinality `GROUP BY` → big hash table → spill risk. |
| `Stream Aggregate` | `GROUP BY` over already-sorted input. | Often paired with a `Sort` immediately below it. |
| `Sort` | `ORDER BY`, or a sort to feed a downstream merge/stream operator. | **Sort spill** warning means tempdb hit. Sorts near the root over millions of rows are a [Single-node bottleneck](#single-node-bottleneck) red flag. |
| `Top` | `SELECT TOP N` / `OFFSET ... FETCH`. | A `Top` near the root over a heavy `Sort` means the engine sorted everything and then kept N — sometimes unavoidable, sometimes a rewrite opportunity. |

#### Data movement (Fabric / MPP-specific)

These operators don't exist in single-node SQL Server. They show up because Fabric Warehouse is distributed across multiple compute nodes.

| Operator | What it does | When it's a problem |
|---|---|---|
| `Shuffle Move` (a.k.a. `Repartition`) | Re-hashes rows across nodes by some key (often a join column) so that matching rows land on the same node before a join or aggregate. | A shuffle on a *huge* table is expensive. If you see two shuffles on the same big table for two different join keys, the join order may be sub-optimal. |
| `Broadcast Move` | Sends a full copy of one (small) input to every node — used when one side of a join is small enough that copying it everywhere beats shuffling the big side. | Broadcasting a *large* table is a serious problem — it inflates network traffic and per-node memory. Usually a sign of [Bad cardinality estimates](#bad-cardinality-estimates). |
| `Trim Move` / `Partition Move` | Reorganises data between distributions. | Usually fine; only a concern if it appears repeatedly on huge inputs. |
| `Gather` / `Return` (root) | Final consolidation back to the client. | If the input row count to `Gather` is huge, you have a [Single-node bottleneck](#single-node-bottleneck) at the finishing step. |

### A practical reading workflow

Walk through the plan in this order — it converges on the root cause faster than browsing top-down.

1. **Skim for warnings.** Yellow/red triangles first. Read each tooltip. Many warnings (missing stats, spills) point you straight at a cause section.
2. **Find the most expensive operator.** Sort your attention by the `% cost` labels. The top 1–2 operators usually account for most of the runtime.
3. **At the most expensive operator, ask three questions:**
   - *Is the estimated row count plausible?* If not → [Bad cardinality estimates](#bad-cardinality-estimates).
   - *Is the operator type appropriate for the row counts?* (e.g. Nested Loops over millions of rows, or Broadcast Move on a multi-GB table.) If not → likely the same cardinality-estimate root cause.
   - *Is there a spill warning?* If yes → memory pressure on this operator. Often a rewrite (filter earlier, smaller projection) helps; sometimes it's [Capacity pressure](#capacity-pressure).
4. **Trace arrow thickness.** Find any place a fat arrow narrows hard. That's where late filtering happens — the work upstream was wasted. Push the filter closer to the source: pre-filter in a CTE/subquery, or rewrite to make the predicate sargable.
5. **Look at data movement operators.** Any `Shuffle Move` or `Broadcast Move` on a large input deserves attention. Multiple shuffles of the same table = consider join order or distribution-friendly rewrites.
6. **Inspect the root operator.** If the root receives millions of rows from a `Sort` or `Hash Aggregate`, you've hit a [Single-node bottleneck](#single-node-bottleneck). Reduce the result size with a `WHERE`, `TOP`, or aggregation that pushes work down.
7. **Cross-check with runtime metrics.** Pop back to `queryinsights.exec_requests_history` (Step 3) — does the plan's story match the `data_scanned_*_mb`, `row_count`, and `allocated_cpu_time_ms` you actually observed?

### Pattern → cause cheat sheet

| What you see in the plan | Probable cause | Section |
|---|---|---|
| Warning: "Columns with no statistics" / "missing statistics" | Optimizer is guessing row counts | [Bad cardinality estimates](#bad-cardinality-estimates) |
| Warning: "Operator used tempdb to spill data" on Hash Match or Sort | Hash table or sort didn't fit in memory | [Bad cardinality estimates](#bad-cardinality-estimates) (build-side underestimate) or [Capacity pressure](#capacity-pressure) |
| Warning: "One or more non-scalable operations" | A finishing step doesn't parallelize across nodes | [Single-node bottleneck](#single-node-bottleneck) |
| Estimated rows ≫ actual rows (or vice versa) at any operator | Stats stale or missing on a filter/join column | [Bad cardinality estimates](#bad-cardinality-estimates) |
| `CONVERT_IMPLICIT(...)` in a `Compute Scalar` near a join/filter | Data type mismatch on the predicate | [Data type or join mismatch](#data-type-or-join-mismatch) |
| Fat arrow into a `Filter`, thin arrow out | Predicate not pushed into the scan — too much data scanned | [Too much data scanned](#too-much-data-scanned) |
| `Broadcast Move` on a multi-GB input | Optimizer thought one side was small — usually a stats problem | [Bad cardinality estimates](#bad-cardinality-estimates) |
| Multiple `Shuffle Move` operators on the same large table | Sub-optimal join order, or joining on different keys for different joins | Consider rewriting the query; review [Distributed Query Processing](#distributed-query-processing) |
| Big `Sort` immediately under the root with millions of rows | Final sort is the bottleneck | [Single-node bottleneck](#single-node-bottleneck) |
| Many small operators reading lots of files (look for high `EstimatedRowsRead` / file count) | File fragmentation hurting scans | [Inefficient physical layout](#inefficient-physical-layout) |
| Same expensive plan repeats N times in `frequently_run_queries` | Plan itself may be fine, but query frequency is the issue | [Semantic model / report pressure](#semantic-model--report-pressure) |

### Reading the raw XML when SSMS isn't available

If you're working from `sqlcmd` or a CI environment without graphical tooling, you can pull the same signals out of the XML directly. Useful XPath-like landmarks (search the file with any text editor):

| What to search for | Why it matters |
|---|---|
| `<Warnings>` | Plan-level or operator-level warnings — read these first. |
| `PhysicalOp="..."` on `<RelOp>` | The operator name (e.g. `PhysicalOp="Hash Match"`, `PhysicalOp="Sort"`, `PhysicalOp="Compute Scalar"`). |
| `EstimateRows="..."` on `<RelOp>` | What the optimizer thinks each operator will produce. Compare big values against actual `row_count` from queryinsights. |
| `EstimatedTotalSubtreeCost="..."` on `<RelOp>` | Cumulative cost up to and including this operator. Largest value at the root = total estimated cost; biggest jump between parent and child = most expensive single operator. |
| `CONVERT_IMPLICIT(` (anywhere) | Implicit type conversion — see [Data type or join mismatch](#data-type-or-join-mismatch). |
| `<MissingIndexes>` | Optimizer's guess at indexes that would help. **Note**: largely irrelevant for Fabric Warehouse since you can't create rowstore indexes — use it as a hint about which columns deserve `CREATE STATISTICS ... WITH FULLSCAN` instead. |
| `<MemoryGrant ... GrantedMemory="..."` and `RequestedMemory` | If `GrantedMemory` < `RequestedMemory`, the engine couldn't fully grant the requested working memory → spill risk. |
| `<NonParallelPlanReason ...>` | If present, the optimizer chose a serial plan and tells you why — closely related to [Single-node bottleneck](#single-node-bottleneck) cases. |

A quick PowerShell one-liner to summarize a plan from the CLI:

```powershell
$plan = [xml](Get-Content "C:\temp\trip-query.sqlplan" -Raw)
$plan.SelectNodes("//*[local-name()='RelOp']") |
    Sort-Object { [double]$_.EstimatedTotalSubtreeCost } -Descending |
    Select-Object -First 10 PhysicalOp, LogicalOp, EstimateRows, EstimatedTotalSubtreeCost
```

That ranks the 10 most expensive operators in the plan — a good starting point when you can't open the graphical view.

### What plans **don't** tell you

Plans are a model of what the optimizer *thinks* will happen. They don't show:

- Actual runtime, actual rows, or actual memory grants (Fabric Warehouse doesn't currently emit a SQL-Server-style "actual" plan).
- Cache state for this run (was data already in memory cache? was the result cache hit?). Cross-check `result_cache_hit` and the `data_scanned_*_mb` columns.
- Capacity pressure or queueing at execution time. Cross-check `queryinsights.sql_pool_insights`.
- Per-node CPU distribution or skew. The plan shows the *shape* of distribution; the runtime metrics show whether one node disproportionately did the work.

So a plan reading is necessary but rarely sufficient — combine it with the runtime signals from Steps 3, 4, and 8 of [Specific Query Performance Investigation](#specific-query-performance-investigation) for the full picture.