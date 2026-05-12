


# Table
name:  notebook_job_table
columns
- notebook_id:  Fabric item ID of the notebook
- workspace_id:  The GUID where the notebook lives
- notebook_name:  The name of the notebook
- workspace_name:  The name of the workspace
- activity_name:  The name of the activity from monitor (note this is NULL when the notebook is run ad-hoc)
- job_duration_seconds:  The total wall clock duration of the notebook job instance, in seconds, as reported by the monitoring Hub.  This includes session bootstrap, environment install, spark execution, and teardown
- job_start_utc:  UTC timestamp when Fabric accepted the notebook job instance
- job_end_utc:  UTC timestamp when Fabric finished the notebook job instance
- spark_submit_utc:  UTC timestamp when the spark app was submitted... that is, when a livy session became ready to accept the first statement.  The gap between job_start and spark_submit_time is the hidden bootstrap window
- spark_total_duration_seconds:  Total runtime of the spark app in seconds, from spark_submit_time to spark app end.  Excludes session bootstrap
- spark_queued_duration_seconds:  Time in seconds the spark app spent queued inside the spark pool after the session existed.  Note, this does not include time waiting for the session itself to be provisioned
- spark_running_duration_seconds:  Time in seconds the spark app was actively executing (spark_total_duration - spark_queued_duration)
- livy_session_id:  Identifier of the livy/spark session backing this notebook run.  Useful for correlating with spark monitoring and livy session APIs
- session_request_time:  UTC timestamp when a livy session was first requested for this notebook run.  Marks the start of session provisioning
- session_ready_time:  UTC timestamp when the livy session reached the idle state (libraries installed, kernel ready).  Marks the end of the session bootstrap
- environment_id:  The GUID of the environment attached to the notebook
- environment_published_version:  The published version number of the attached Fabric environment at the time the notebook ran.  Critical for correlating slow bootstraps with specific environment versions.
- library_count:  Number of custom libraries defined in the environment's published version.  Strong correlate with t_environment_install
- pool_name:  Name of the spark pool used for this run (i.e. the workspace's starter pool name or a custom pool name)
- pool_type:  Either Starter or Custom
- node_size:  Node size SKU used for driver/executors (e.g. Small, Medium, Large, XLarge)
- default_lakehouse_id:  Item ID of the lakehouse attached as the notebook's default lakehouse.  May be null if none is set.
- attached_lakehouses:  Array (or count) of additional lakehouse items IDs attached to the notebook beyond the default.  
- t_session_provision_seconds:  Seconds spent provisioning the spark session:  from session_request_time until the session enters the starting state.  Captures pool/capacity allocation time
- t_environment_install_seconds:  Seconds spent hydrating the custom environment (library install, container warmup) from session starting to session idle (session_ready_time).  Typically the largest component of bootstrap
- t_first_statement_dispatch_seconds:  Seconds between the session idle (session_ready_time) and the first statement actually executing

## Derived "hidden overhead" columns
The whole reason this table exists — quantifies the gap between what Fabric's UI shows as "Total duration" vs the real wall-clock time.
- bootstrap_seconds:  job_duration_seconds − spark_total_duration_seconds.  The hidden overhead window — session acquisition + environment install + kernel start + lakehouse mount, etc.
- bootstrap_pct:  bootstrap_seconds / job_duration_seconds.  Fraction of the run spent on overhead vs actual work; key KPI for spotting bad-actor environments/pools.

## Pipeline correlation
Lets you attribute overhead to specific pipelines or tell ad-hoc runs apart from scheduled ones.
- pipeline_run_id:  Parent data pipeline run ID; NULL when notebook is run ad-hoc
- pipeline_id:  Item ID of the parent pipeline; NULL for ad-hoc
- pipeline_name:  Display name of the parent pipeline; NULL for ad-hoc
- invocation_type:  How the run was triggered.  One of: Pipeline | Scheduled | Manual | API

## Capacity context
Capacity SKU and identity are major drivers of cold-start behavior (warm pool availability, throttling).
- capacity_id:  GUID of the Fabric capacity the workspace is bound to at run time
- capacity_sku:  SKU of the capacity at run time (e.g., F2, F64, F512)

## Outcome
- status:  Final status of the notebook job instance.  One of: Success | Failed | Cancelled | InProgress
- error_message:  Error text from the failed run; NULL on success
- submitter:  UPN of the user or service principal that triggered the run.  Useful for filtering out noise from a specific user or automated account.

## Executor configuration
Captures the Spark config that was in effect for the run; affects both startup time and execution time.
- executor_count_min:  Lower bound of dynamic executor allocation
- executor_count_max:  Upper bound of dynamic executor allocation
- dynamic_allocation_enabled:  Boolean; whether dynamic executor allocation was enabled
- runtime_version:  Fabric Spark runtime version string (e.g., "1.3 (Spark 3.5, Delta 3.2)")

## Ingestion bookkeeping
Standard fact-table hygiene; lets you re-run / backfill the collector safely and trace data lineage.
- run_id:  Primary key.  The notebook job instance ID (GUID) from the Monitoring Hub.
- ingested_at_utc:  UTC timestamp of when this row was written by the collector
- source_api_version:  Version string of the Fabric/Livy API the collector used (so schema changes upstream can be diagnosed later)

# Dimension table
name:  environment_versions_dim

Pulled out of the fact table because environments are reused across many notebook runs — storing the library snapshot once per (environment_id, environment_published_version) is far cheaper than repeating it on every fact row, and it lets you trend env changes over time.

columns
- environment_id:  GUID of the Fabric environment
- environment_published_version:  Published version number
- workspace_id:  Workspace the environment lives in
- environment_name:  Display name of the environment
- library_count:  Total number of libraries pinned in this published version
- libraries:  Array<struct<name, version, source>> snapshot of the libraries (PyPI / conda / custom wheel) at publish time
- published_at_utc:  When this version of the environment was published
- ingested_at_utc:  When the collector captured this row




### Columns Sourcing Summary
- notebook_id:  Job Instance API → `GET /v1/workspaces/{workspaceId}/items/{itemId}/jobs/instances/{jobInstanceId}`
- workspace_id:  Known from collector context (no API call)
- notebook_name:  Items API → `GET /v1/workspaces/{workspaceId}/items/{itemId}`
- workspace_name:  Workspaces API → `GET /v1/workspaces/{workspaceId}`
- activity_name:  Job Instance API → `GET /v1/workspaces/{workspaceId}/items/{itemId}/jobs/instances/{jobInstanceId}`
- job_duration_seconds:  Derived (job_end_utc − job_start_utc)
- job_start_utc:  Job Instance API → `GET /v1/workspaces/{workspaceId}/items/{itemId}/jobs/instances/{jobInstanceId}`
- job_end_utc:  Job Instance API → `GET /v1/workspaces/{workspaceId}/items/{itemId}/jobs/instances/{jobInstanceId}`
- spark_submit_utc:  Spark Monitoring → `GET /v1/workspaces/{workspaceId}/spark/applications/{applicationId}`
- spark_total_duration_seconds:  Spark Monitoring → `GET /v1/workspaces/{workspaceId}/spark/applications/{applicationId}`
- spark_queued_duration_seconds:  Spark Monitoring → `GET /v1/workspaces/{workspaceId}/spark/applications/{applicationId}`
- spark_running_duration_seconds:  Spark Monitoring → `GET /v1/workspaces/{workspaceId}/spark/applications/{applicationId}`
- livy_session_id:  Job Instance API → `GET /v1/workspaces/{workspaceId}/items/{itemId}/jobs/instances/{jobInstanceId}`
- session_request_time:  Livy Sessions API → `GET /v1/workspaces/{workspaceId}/spark/livySessions/{livyId}`
- session_ready_time:  Livy Sessions API → `GET /v1/workspaces/{workspaceId}/spark/livySessions/{livyId}`
- environment_id:  Notebook Def API → `POST /v1/workspaces/{workspaceId}/notebooks/{notebookId}/getDefinition`
- environment_published_version:  Environments API → `GET /v1/workspaces/{workspaceId}/environments/{environmentId}`
- library_count:  Environments API → `GET /v1/workspaces/{workspaceId}/environments/{environmentId}/libraries`
- pool_name:  Spark Pools API → `GET /v1/workspaces/{workspaceId}/spark/pools/{poolId}`
- pool_type:  Spark Pools API → `GET /v1/workspaces/{workspaceId}/spark/pools/{poolId}`
- node_size:  Spark Pools API → `GET /v1/workspaces/{workspaceId}/spark/pools/{poolId}`
- default_lakehouse_id:  Notebook Def API → `POST /v1/workspaces/{workspaceId}/notebooks/{notebookId}/getDefinition`
- attached_lakehouses:  Notebook Def API → `POST /v1/workspaces/{workspaceId}/notebooks/{notebookId}/getDefinition`
- t_session_provision_seconds:  Derived from Livy Sessions API → `GET /v1/workspaces/{workspaceId}/spark/livySessions/{livyId}`
- t_environment_install_seconds:  Derived from Livy Sessions API → `GET /v1/workspaces/{workspaceId}/spark/livySessions/{livyId}`
- t_first_statement_dispatch_seconds:  Derived from Livy Sessions API → `GET /v1/workspaces/{workspaceId}/spark/livySessions/{livyId}/statements`

Notes / caveats:
- Some fields (notably `livy_session_id`, the Livy state-log, and per-statement timings) are exposed under the Spark/Livy monitoring surface, which is in active development. Expect to use both the Fabric Spark Monitoring REST endpoints and a direct Livy session query, and snapshot raw responses in Bronze so you can re-derive Silver/Gold if the schema shifts.
- Always prefer values *snapshotted on the job instance* (config-as-run) over current values from the notebook/environment/pool APIs when the run is historical — notebooks, envs, and pools change.
- For the `environment_versions_dim`, the `libraries[]` snapshot comes from Environments API → `…/libraries` (published) at the time the version was first observed; capture once per `(environment_id, environment_published_version)` and never overwrite.



### Columns Sourcing Detailed
Notation:
- "Job Instance API"  = `GET /v1/workspaces/{workspaceId}/items/{itemId}/jobs/instances` (and `/{jobInstanceId}` for details). Filter `jobType=RunNotebook`.
- "Items API"         = `GET /v1/workspaces/{workspaceId}/items/{itemId}`
- "Workspaces API"    = `GET /v1/workspaces/{workspaceId}`
- "Notebook Def API"  = `POST /v1/workspaces/{workspaceId}/notebooks/{notebookId}/getDefinition` — returns the `.platform` + `notebook-content.py` payload with the trident metadata block (default lakehouse, attached lakehouses, environment ref, spark pool ref).
- "Spark Monitoring"  = `GET /v1/workspaces/{workspaceId}/spark/applications/{applicationId}` and `…/livySessions/{livyId}` (Fabric Spark monitoring REST). Returns submit/start/end times, executor config, runtime, and session state transitions.
- "Livy Sessions API" = `GET …/livyapi/versions/2023-12-01/sessions/{sessionId}` and `…/sessions/{sessionId}/statements` — used for fine-grained state transitions and per-statement timings.
- "Environments API"  = `GET /v1/workspaces/{workspaceId}/environments/{environmentId}` and `…/staging/libraries` | `…/libraries` (published).
- "Spark Pools API"   = `GET /v1/workspaces/{workspaceId}/spark/pools` (and `…/pools/{poolId}`); workspace settings via `…/spark/settings`.
- "Derived"           = computed by the collector from other captured fields, not pulled from an API.

columns
- notebook_id:  Job Instance API → `itemId` (also the path parameter you queried with). Confirms which notebook the run belonged to.
- workspace_id:  Known from the API call path (the collector iterates workspaces). No lookup needed.
- notebook_name:  Items API → `displayName` for the notebook item. Cache per `notebook_id` to avoid one call per run.
- workspace_name:  Workspaces API → `displayName`. Cache per `workspace_id`.
- activity_name:  Job Instance API (details) → `rootActivityName` / `invokeType`. NULL when `invokeType != "Pipeline"` (i.e., ad-hoc/manual/scheduled).
- job_duration_seconds:  Derived = `job_end_utc − job_start_utc`.
- job_start_utc:  Job Instance API → `startTimeUtc`.
- job_end_utc:  Job Instance API → `endTimeUtc`.
- spark_submit_utc:  Spark Monitoring → application `submittedTime` (a.k.a. `appInfo.startTime` on the Livy session). This is the moment the Spark app was registered; the gap to `job_start_utc` is the hidden bootstrap window.
- spark_total_duration_seconds:  Spark Monitoring → application `totalDuration` (ms, convert to s). Equivalent to `endTime − submittedTime` if you'd rather derive it.
- spark_queued_duration_seconds:  Spark Monitoring → application `queuedDuration`. NOTE: this is queue time *inside* the Spark pool after the session existed; it does NOT include session provisioning time.
- spark_running_duration_seconds:  Spark Monitoring → application `runningDuration`, or Derived = `spark_total_duration_seconds − spark_queued_duration_seconds`.
- livy_session_id:  Job Instance API (details) → look for `livyId` / "Operation (Livy) ID" in the run metadata. If absent, query Livy Sessions API filtered by `name`/`tags` matching the notebook job instance ID.
- session_request_time:  Livy Sessions API → session `createdAt` (or the first state-log entry, state = `not_started`). This marks when the session was requested.
- session_ready_time:  Livy Sessions API → session state-log entry where state transitions to `idle`. Libraries are installed and the kernel is ready.
- environment_id:  Notebook Def API → `metadata.dependencies.environment.id` in the `.platform`/notebook-content payload. (Falls back to job instance config snapshot if the notebook def has changed since the run.)
- environment_published_version:  Environments API → `properties.publishDetails.targetVersion` for the env at run time. For historical accuracy, prefer the value snapshotted in the job instance config rather than the current published version.
- library_count:  Environments API → length of `…/libraries` (published) response for `(environment_id, environment_published_version)`. Better stored once on `environment_versions_dim` and joined.
- pool_name:  Notebook Def API → `metadata.dependencies.sparkPool` reference, then Spark Pools API → `name`. If the notebook uses workspace defaults, fall back to workspace `spark/settings` → `pool.name`.
- pool_type:  Spark Pools API → `type` field (`Workspace` for starter, `Capacity`/`Custom` for custom). Normalize to `Starter` | `Custom` in the collector.
- node_size:  Spark Pools API → `nodeSize` (e.g., `Small`, `Medium`, `Large`, `XLarge`). Or job instance config snapshot if available.
- default_lakehouse_id:  Notebook Def API → `metadata.dependencies.lakehouse.default_lakehouse` (a.k.a. `trident.lakehouse.default_lakehouse`). NULL if none is configured.
- attached_lakehouses:  Notebook Def API → `metadata.dependencies.lakehouse.known_lakehouses[]` array (excluding the default). Suggest splitting in the collector into `attached_lakehouse_ids: array<string>` and `attached_lakehouse_count: int`.
- t_session_provision_seconds:  Derived = (Livy session state-log timestamp where `state == "starting"`) − `session_request_time`. Captures pool/capacity allocation.
- t_environment_install_seconds:  Derived = `session_ready_time` − (state-log timestamp where `state == "starting"`). Captures library hydration and container warmup.
- t_first_statement_dispatch_seconds:  Derived = (Livy Sessions API → `statements[0].started`) − `session_ready_time`. Captures kernel start, init cells, imports, `notebookutils` warmup before the first user statement runs.

Notes / caveats:
- Some fields (notably `livy_session_id`, the Livy state-log, and per-statement timings) are exposed under the Spark/Livy monitoring surface, which is in active development. Expect to use both the Fabric Spark Monitoring REST endpoints and a direct Livy session query, and snapshot raw responses in Bronze so you can re-derive Silver/Gold if the schema shifts.
- Always prefer values *snapshotted on the job instance* (config-as-run) over current values from the notebook/environment/pool APIs when the run is historical — notebooks, envs, and pools change.
- For the `environment_versions_dim`, the `libraries[]` snapshot comes from Environments API → `…/libraries` (published) at the time the version was first observed; capture once per `(environment_id, environment_published_version)` and never overwrite.

# Storage architecture

## Compute / collector
Azure Function (Python, timer trigger + HTTP trigger for ad-hoc runs). Keeps the collector in an independent failure domain from Fabric so it doesn't pollute its own cold-start metrics.

## Persistence: one Fabric Warehouse
A single Fabric Warehouse holds everything. All sources land in their own `raw` table before being merged into curated facts/dims. Schemas separate concerns:

| Schema | Purpose |
|---|---|
| `raw` | Landing tables — one per source API. Append-only. Raw JSON payload + parsed columns. |
| `dbo` | Curated fact + dim tables that Power BI queries. |
| `meta` | Collector bookkeeping — watermarks, run logs, errors. |

Rationale for one warehouse (not multiple): single audience and purpose, cross-table joins stay trivial, one Managed Identity grant, one Power BI semantic model, and the volume is far below any threshold that would justify splitting.

## Tables

### `raw.*` — one per source API

Every `raw.*` table includes `ingested_at_utc`, `collector_run_id`, and a `raw_payload nvarchar(max)` column holding the original JSON, alongside parsed columns. Append-only.

| Table | Grain | Source API | Notes |
|---|---|---|---|
| `raw.job_instance` | 1 row per notebook job instance | Job Instance API | Driver of the whole pipeline — everything else is keyed off `jobInstanceId` discovered here. |
| `raw.spark_application` | 1 row per Spark app | Spark Monitoring | Holds `spark_total/queued/running_duration`. |
| `raw.livy_session` | 1 row per Livy session | Livy Sessions API | Session state-log JSON parsed for `t_session_provision`, `t_environment_install`. |
| `raw.livy_statement` | 1 row per statement | Livy Statements API | Needed for `t_first_statement_dispatch`. Optional in v1 — derive from session-only if deferred. |
| `raw.notebook_definition` | 1 row per (notebook, observed_at) | Notebook Def API | Snapshot of `.platform` JSON — env_id, pool, lakehouses at observation time. |
| `raw.notebook_item` | 1 row per notebook | Items API | Slow-changing — re-poll on a longer cadence. |
| `raw.workspace` | 1 row per workspace | Workspaces API | Slow-changing. Fold `capacity_id` / `capacity_sku` here. |
| `raw.environment` | 1 row per (env, observed_at) | Environments API | Captures `publishedVersion` over time. |
| `raw.environment_library` | 1 row per (env, version, library) | Environments API `…/libraries` | Library snapshot. Capture once per `(environment_id, environment_published_version)` and never re-pull. |
| `raw.spark_pool` | 1 row per pool | Spark Pools API | Slow-changing. |

### `dbo.*` — curated model

**Essential (v1):**

| Table | Grain | Purpose |
|---|---|---|
| `dbo.notebook_run_facts` | 1 row per notebook job instance | Headline fact table. PK = `run_id`. Denormalizes notebook/workspace/pool name in for v1 simplicity. MERGE target. |
| `dbo.environment_versions_dim` | 1 row per (env_id, version) | Library snapshot — joined to facts on `(environment_id, environment_published_version)`. |

**Optional (add when reports demand them):**

| Table | When to add |
|---|---|
| `dbo.notebook_dim` | When you want notebook display names to live in one place (handles renames cleanly). |
| `dbo.workspace_dim` | Same reasoning, plus capacity binding history. |
| `dbo.spark_pool_dim` | When you want pool config history (node-size changes over time). |
| `dbo.notebook_run_statement_facts` | When you want statement-level latency reporting (drilldown beyond `bootstrap_seconds`). |

For v1, denormalize names into the fact table; promote to dims only when the pain shows up.

### `meta.*` — collector operations

| Table | Purpose |
|---|---|
| `meta.collection_watermark` | Per-(workspace, source) high-water mark. Function reads at start, writes at end. Drives `$filter=startTimeUtc gt …` for incremental Job Instance pulls. |
| `meta.collection_run_log` | One row per Function invocation — `run_id`, started/ended_at, status, rows landed per source, rows merged, error summary. |
| `meta.collection_error_log` | One row per failed API call — workspace, endpoint, status code, error body. Triage without trawling Function logs. |

### Table count

| Schema | Essential | Optional |
|---|---|---|
| `raw` | 9 | +1 (`raw.livy_statement`) |
| `dbo` | 2 | +4 (3 dims + statement fact) |
| `meta` | 3 | 0 |
| **Total** | **14** | **+5** |

## MERGE keys

- `dbo.notebook_run_facts` — MERGE on `run_id`. Idempotent: re-running the collector for the same window is safe.
- `dbo.environment_versions_dim` — MERGE on `(environment_id, environment_published_version)`. Also idempotent.

## Design notes

- **Two flavors of `raw` tables**:
  - *Per-event* (`job_instance`, `spark_application`, `livy_session`, `livy_statement`) — append-only, grow forever.
  - *Slow-changing reference* (`notebook_item`, `workspace`, `environment`, `spark_pool`) — for v1, snapshot every collection run with `observed_at`. Storage is cheap; "diff vs last known" can come later if needed.
- **`raw.environment_library` is special** — capture once per `(environment_id, environment_published_version)` and never re-pull. Saves a lot of API calls.
- **Function → Warehouse write path**: `pyodbc` against the Warehouse SQL endpoint with an Entra token from the Function's Managed Identity. Bulk insert into `raw.*`, then `EXEC` a stored proc that MERGEs raw → curated.
- **Power BI** consumes the curated `dbo.*` tables via the Warehouse's SQL analytics endpoint (Direct Lake on the auto-generated semantic model). No mirror, no refresh.