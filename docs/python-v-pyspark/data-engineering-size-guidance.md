# When to use PySpark vs. Python for Data Engineering in Microsoft Fabric

> Status: Guidance / opinionated reference
> Audience: Data engineers and architects building pipelines on Microsoft Fabric (Lakehouse + Warehouse)

---

## TL;DR (Executive Summary)

**Yes, the complaint is valid.** Spark is a distributed engine designed for datasets that don't fit on one machine. For the large fraction of real-world Fabric pipelines that process **megabytes to a few gigabytes per run**, PySpark adds:

- **Startup latency** — a Spark session/pool spin-up can be tens of seconds even with starter pools, dwarfing the actual compute time of a small job.
- **CU (Capacity Unit) cost** — Spark pools bill for the whole cluster while it's alive; a 30-second job on an idle pool can still consume meaningful CU vs. a 2-second pure-Python run.
- **Operational complexity** — partitioning, shuffles, broadcast hints, AQE tuning, and small-file problems all become things you have to think about even when the data is tiny.
- **Developer friction** — debugging, unit testing, and iteration are slower than plain Python.

**Rule of thumb (rough, defensible defaults):**

| Working-set size per run | Default choice |
|---|---|
| < ~100 MB and single source/sink | **Python** (Fabric **Python Notebook**, UDF, or Pipeline activity) |
| ~100 MB – ~10 GB, mostly SQL-shaped | **Warehouse T-SQL** or **Python + DuckDB/Polars** on a Python Notebook |
| ~10 GB – ~100 GB, or wide joins / shuffles | **PySpark** (Fabric Spark Notebook) |
| > ~100 GB, streaming, or heavy ML feature pipelines | **PySpark**, no debate |

These are *defaults*, not laws. The decision tree below captures the exceptions.

---

## The Decision Tree

```
                ┌─────────────────────────────────────────┐
                │ 1. How big is the data PER RUN?         │
                │    (post-filter, post-pushdown)         │
                └───────────────┬─────────────────────────┘
                                │
        ┌───────────────────────┼───────────────────────────┐
        │                       │                           │
     < 100 MB             100 MB – 10 GB                 > 10 GB
        │                       │                           │
        ▼                       ▼                           ▼
┌──────────────────┐  ┌──────────────────────┐   ┌──────────────────────┐
│ 2a. Is it pure   │  │ 2b. Is the workload  │   │  USE PYSPARK         │
│ SQL on Delta/    │  │ expressible as SQL   │   │  (Fabric Spark NB    │
│ Warehouse?       │  │ joins/aggregates?    │   │   or Spark Job Def.) │
│                  │  │                      │   │                      │
│ Yes → T-SQL in   │  │ Yes → Warehouse      │   │ Tune partitioning,   │
│   Warehouse or   │  │   T-SQL (preferred   │   │ enable AQE, watch    │
│   Lakehouse SQL  │  │   for set-based ELT) │   │ small-file count.    │
│   endpoint       │  │                      │   │                      │
│                  │  │ No → Python Notebook │   │                      │
│ No → Python      │  │   with DuckDB or     │   │                      │
│   Notebook       │  │   Polars             │   │                      │
│   (pandas/Polars)│  │                      │   │                      │
└────────┬─────────┘  └───────────┬──────────┘   └──────────┬───────────┘
         │                        │                         │
         └──────────┬─────────────┴─────────────────────────┘
                    ▼
   ┌──────────────────────────────────────────────────────┐
   │ 3. Override checks — any of these flip you to Spark? │
   │                                                      │
   │  • Need to write to a Delta table with concurrent    │
   │    writers? → Spark (Delta on Spark handles          │
   │    concurrency + OPTIMIZE/VACUUM natively).          │
   │  • Streaming / Structured Streaming?     → Spark     │
   │  • MLlib or distributed feature gen?     → Spark     │
   │  • Need Spark connector that has no Python           │
   │    equivalent (e.g. some 3rd-party sources)? → Spark │
   │  • Team only knows PySpark and job is one-off?       │
   │    → Spark is fine, don't over-engineer.             │
   └──────────────────────────────────────────────────────┘
                    │
                    ▼
   ┌──────────────────────────────────────────────────────┐
   │ 4. Override checks — any of these flip you AWAY      │
   │ from Spark?                                          │
   │                                                      │
   │  • Job runs more than ~10x/day on small data?        │
   │    → Python (startup tax compounds)                  │
   │  • Event-driven / sub-minute SLA?                    │
   │    → Fabric User Data Function or Azure Function     │
   │  • Mostly orchestration / API calls / file moves?    │
   │    → Pipeline activities or Python Notebook          │
   │  • You're hitting a REST/Graph/SaaS API row-by-row?  │
   │    → Python (Spark gives you nothing here)           │
   └──────────────────────────────────────────────────────┘
```

### Quick-glance lookup table

| Scenario | Best fit | Why |
|---|---|---|
| Ingest a 20 MB CSV from SharePoint daily into Bronze | Python Notebook or Pipeline Copy | Spark startup > the whole job |
| Hourly upsert of 500 MB CDC feed into Silver Delta | Python Notebook (`deltalake` / DuckDB) or small Spark | Either works; pick by team skill + concurrency needs |
| Nightly join of 50 GB orders × 200 GB clicks | PySpark | Shuffle-heavy, distributed-only |
| Build a star schema from Bronze for Power BI | Warehouse T-SQL stored procs | Set-based, governed, cached |
| Call a REST API 10k times, land JSON | Python (Notebook or UDF) | I/O-bound, no parallel compute benefit |
| Real-time scoring of events from Eventstream | Eventstream + KQL or Spark Structured Streaming | Depends on latency target |
| Train a model on 5 GB of features | Python Notebook (scikit-learn / LightGBM) | Single-node is faster and simpler |
| Train on 500 GB or distributed deep learning | PySpark + MLlib / SynapseML | Needs the cluster |

---

# Architect Section — Tradeoffs in Depth

## 1. The actual cost of Spark on small data

Spark's value proposition is **horizontal scale + fault tolerance + a unified API across batch and streaming**. The price you pay for that is fixed-cost overhead per job:

- **Session warm-up.** Even Fabric starter pools take ~5–30s to attach. Custom pools or non-warmed sessions can be 1–3 minutes. For a job whose actual work is 4 seconds, you've paid a 10–100x latency tax.
- **Driver + executor JVMs.** You're paying CU for the driver and at least one executor for the full session lifetime, not just the work.
- **Coordination overhead.** Even with one partition, Spark plans, optimizes, schedules, and serializes results through the driver. Pure Python skips all of that.
- **Small-file amplification.** Writing tiny Delta files from Spark requires you to think about `coalesce`, `repartition`, `OPTIMIZE`, and `VACUUM`. Native Delta writers (e.g. `delta-rs` from Python) make different but simpler tradeoffs.

On a Fabric F-SKU, the CU-seconds consumed by an idle-but-attached Spark session are not free. Multiply by frequency: a 30-second Spark job that runs every 5 minutes is very different from one that runs nightly.

## 2. When PySpark genuinely wins

Use Spark without hesitation when **any** of these are true:

- **Data exceeds single-node memory** (rough threshold: ~50–70% of the largest single-node VM you're willing to pay for — in Fabric Python Notebooks today that ceiling is meaningful but not unlimited).
- **Wide shuffles**: large joins where neither side fits in memory, or aggregations over high-cardinality keys.
- **Structured Streaming** with stateful operations, watermarks, exactly-once sinks to Delta.
- **Concurrent writers** to the same Delta table — Spark's Delta implementation is the reference and handles optimistic concurrency, OPTIMIZE, Z-ORDER, and deletion vectors most maturely.
- **Distributed ML / feature engineering** (MLlib, SynapseML, distributed XGBoost, Horovod-style training).
- **Native connectors only available in Spark** — some enterprise sources still ship Spark-first connectors.

## 3. When Python (non-Spark) wins

Pick a non-Spark option when:

- The dataset comfortably fits in memory on one node after pushdown/filtering.
- The job is **I/O-bound** (REST/Graph/SaaS APIs, file moves, blob copies) — Spark gives you no parallelism benefit on serialized HTTP calls, and the JVM overhead actively hurts.
- You need **fast iteration / unit testing / local dev parity**. Pure Python with `pytest` + `polars`/`duckdb`/`pandas` is dramatically easier to test than a Spark job.
- The job is **high-frequency, low-volume** (every minute, every 5 minutes). Pay the startup cost rarely, not constantly.
- The job is **event-driven** with a sub-minute SLA — a Fabric User Data Function or Azure Function will respond in hundreds of milliseconds; a Spark session won't.

Inside the Python lane, in 2025 the strongest general-purpose tools are:

- **Polars** — vectorized DataFrames in Rust, multi-threaded, lazy, kills pandas on perf, comfortable up to ~tens of GB on a beefy node.
- **DuckDB** — embedded analytical SQL engine; reads Parquet/Delta directly, can outperform Spark for single-node analytics in the 1–100 GB range. Great when the team thinks in SQL.
- **delta-rs / `deltalake`** — native Rust Delta Lake reader/writer; lets Python notebooks and Azure Functions write Delta without Spark.
- **pandas** — still fine for tiny data and for code that downstream consumers expect.

## 4. The Warehouse option (don't forget it)

For **set-based ELT** — joins, aggregations, slowly changing dimensions, star-schema builds, anything that's already SQL-shaped — the **Fabric Warehouse** is often the right answer over *either* PySpark or Python:

- T-SQL stored procedures are governed, versionable, observable, and the engine is built for this workload.
- Warehouse compute scales independently and is shared with the Power BI semantic layer (Direct Lake).
- No notebook session to warm up; queries dispatch immediately.

**Heuristic:** if you can write the transformation as a single SQL statement (or a handful of them) over Delta tables in OneLake, do it in the Warehouse or via the SQL endpoint, not in a notebook. Reserve notebooks (Spark or Python) for transformations that genuinely need procedural code: API calls, complex Python libraries, custom parsing, ML, etc.

## 5. Fabric-specific compute menu

When the decision tree says "use Python," you still have to pick *which* Python runtime. In order of typical fit:

| Option | Best for | Notes |
|---|---|---|
| **Fabric Python Notebook** (single-node) | Interactive dev, scheduled small/medium jobs, Polars/DuckDB workloads | First-class in Fabric, no Spark session, fast startup |
| **Fabric User Data Function (UDF)** | Event-driven, short-lived, sub-minute SLA, called from pipelines or apps | Serverless, scales to zero, ideal for "tiny piece of Python glue" |
| **Fabric Data Pipeline activities** | Pure orchestration, Copy activity, no/low custom code | Use Copy for source→Bronze moves; cheap and governed |
| **Azure Functions** | External triggers (Event Grid, Service Bus), workloads outside Fabric's billing/governance boundary | Reach for this when the trigger or dependency lives outside Fabric |
| **Azure Container Apps / Container Jobs** | Long-running custom Python with heavy dependencies, GPU, or non-trivial container images | When a notebook or Function isn't enough but Spark is still overkill |

A common, healthy pattern: **Pipeline (orchestration) → Copy activity (Bronze) → Python Notebook or T-SQL (Silver) → PySpark only where needed (heavy Gold transforms or ML)**.

## 6. Operational and governance considerations

- **Lineage & monitoring.** All Fabric notebooks (Spark and Python), UDFs, Pipelines, and Warehouse procs participate in Fabric lineage and Monitoring hub. External compute (Functions, Container Apps) does not — you'll need to instrument it (App Insights, custom logs) and stitch lineage manually.
- **Identity.** Prefer **workspace identity / managed identity** over service principals with secrets. This is true for every option above.
- **CU accounting.** Spark, Python Notebooks, UDFs, Warehouse, and Pipelines all bill against your Fabric capacity, but with very different shapes (burst vs. sustained, per-second vs. per-query). For a high-frequency small job, model the CU cost in both Spark and Python before committing — the difference is often an order of magnitude.
- **Skill portability.** PySpark code is portable to Databricks, Synapse, OSS Spark. Python+Polars+Delta-rs is portable to literally any Python runtime. T-SQL is portable across the SQL family. Single-vendor SDKs are the *least* portable; weight that into the decision for long-lived pipelines.
- **Testing.** Pure Python is dramatically easier to unit-test than Spark. If your pipeline has meaningful business logic, the testability advantage of plain Python is a real engineering benefit, not a cosmetic one.

## 7. Anti-patterns to avoid

- **"We use PySpark for everything."** Defensible only if data volumes justify it across the board. Otherwise you're paying the Spark tax on jobs that don't need it.
- **"We use pandas for everything."** Same problem in reverse — pandas falls over above a few GB and has no concurrency story.
- **Spark for REST API ingestion.** Almost always wrong. Use a Python Notebook, UDF, or Function.
- **Spark for orchestration.** Use Pipelines. Don't `subprocess.run` your way through a workflow inside a notebook.
- **Notebook code that's also a library.** If the same logic is needed in 3+ notebooks, lift it into a workspace library or a pip-installable package — applies equally to Spark and Python.
- **Ignoring the Warehouse.** Many "we need Spark for this join" problems are really "we should have built this in the Warehouse" problems.

## 8. A quick worked example

> *"We need to ingest ~30 MB of order data from a SaaS REST API every 15 minutes, merge it into a Silver Delta table, then rebuild a Gold star schema for Power BI."*

Decision tree applied:

1. **Ingest (30 MB, REST API, every 15 min)** → **Python**. Specifically a Fabric Python Notebook or UDF using `requests` + `delta-rs` to write to Bronze Delta. Spark startup alone would dominate runtime.
2. **Merge into Silver Delta** → **Python with `deltalake` MERGE**, *unless* multiple writers hit the same table concurrently, in which case **PySpark** for its mature Delta concurrency story.
3. **Gold star schema for Power BI** → **Warehouse T-SQL stored procedures**, scheduled by the same Pipeline. Set-based, governed, fast for BI.
4. **Orchestration** → **Fabric Data Pipeline** chaining the three steps with managed-identity auth and failure notifications.

Total Spark usage: zero, or one small job for step 2 — not the default for the whole pipeline.

---

## Summary heuristics (print these on a sticky note)

1. **Data size first.** < 100 MB → Python. > 10 GB → Spark. In between → it depends, default to Python+Polars/DuckDB or Warehouse SQL.
2. **Shape second.** SQL-shaped → Warehouse. Procedural / API / ML → Notebook. Streaming / huge shuffles → Spark.
3. **Frequency matters.** High-frequency small jobs compound the Spark startup tax.
4. **Don't forget the Warehouse.** It's often the right answer and gets skipped because everyone reaches for a notebook.
5. **Match compute to SLA.** Sub-minute event-driven → UDF or Function. Minutes-to-hours batch → Notebook or Spark. Streaming → Spark or Eventstream/KQL.
6. **Spark is a tool, not an identity.** Use it where it earns its overhead.
