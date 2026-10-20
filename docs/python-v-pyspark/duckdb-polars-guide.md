# DuckDB & Polars — A Practical Guide for Fabric Data Engineers

> Audience: Engineers new to DuckDB and Polars who want to use them in Fabric Python Notebooks as a lighter alternative to PySpark for small-to-medium data on Fabric Lakehouses (Delta tables).
> Companion to: `data-engineering-size-guidance.md` in this folder.

---

## Table of contents

1. [TL;DR](#1-tldr)
2. [Why these tools exist](#2-why-these-tools-exist)
3. [DuckDB in depth](#3-duckdb-in-depth)
4. [Polars in depth](#4-polars-in-depth)
5. [DuckDB vs. Polars — the differences](#5-duckdb-vs-polars--the-differences)
6. [Why not just pandas? Or pyspark.pandas?](#6-why-not-just-pandas-or-pysparkpandas)
7. [Using DuckDB and Polars together](#7-using-duckdb-and-polars-together)
8. [Fabric Lakehouses and Delta — does this work?](#8-fabric-lakehouses-and-delta--does-this-work)
9. [Reading and writing Delta — who does what](#9-reading-and-writing-delta--who-does-what)
10. [Getting started — a one-week onboarding plan](#10-getting-started--a-one-week-onboarding-plan)
11. [Common Fabric patterns](#11-common-fabric-patterns)
12. [Performance tips that actually matter](#12-performance-tips-that-actually-matter)
13. [Gotchas and footguns](#13-gotchas-and-footguns)
14. [Cheat-sheet: equivalent operations](#14-cheat-sheet-equivalent-operations)
15. [Further reading](#15-further-reading)
16. [Summary](#16-summary)

---

## 1. TL;DR

- **DuckDB** = an embedded **analytical SQL engine**. Think "SQLite for analytics." You write SQL; it runs in-process, no server, no cluster.
- **Polars** = a **DataFrame library** (like pandas) written in Rust. Multi-threaded, vectorized, lazy-by-default. You write expressions; it compiles them into an optimized plan.
- Both can read/write **Parquet, CSV, JSON, Delta Lake** directly from OneLake / ADLS / local disk.
- Both are **single-node** but **multi-core**, and both are dramatically faster than pandas while using less memory.
- **Rule of thumb:** comfortable up to ~tens of GB on a beefy Fabric Python Notebook node. Past that, reach for Spark.
- **When to pick which:**
  - Your team thinks in **SQL** → DuckDB.
  - Your team thinks in **DataFrames / method chains** → Polars.
  - Heavy joins/aggregations on Parquet → either works; DuckDB often slightly faster on pure SQL, Polars often faster on long expression pipelines.
  - You want both? **Use both.** They interoperate cheaply via Apache Arrow.
- **They fully support Fabric Lakehouses and Delta** — both read and write Delta tables natively, producing files identical to what Spark writes. Power BI Direct Lake and the SQL endpoint read them the same way.

---

## 2. Why these tools exist

For ~20 years the analytical-data world split into:

- **Single-node DataFrames** (pandas) — easy, but single-threaded and memory-hungry; falls over above a few GB.
- **Distributed engines** (Spark) — scale, but heavy startup, cluster ops, and operational overhead.

Modern hardware changed the math. A single node in 2025 routinely has 16–128 cores, 64 GB–1 TB of RAM, and NVMe SSDs. A well-engineered single-node engine can crush most "big data" jobs that historically went to Spark.

DuckDB and Polars are both bets on that thesis:

- **DuckDB** (2019, CWI Amsterdam) — columnar, vectorized, MIT-licensed SQL engine; embedded like SQLite.
- **Polars** (2020, Ritchie Vink) — columnar DataFrame in Rust on top of Apache Arrow; lazy query optimizer.

Both are designed for **OLAP** (scan-heavy analytics), not OLTP (high-frequency small updates).

---

## 3. DuckDB in depth

### 3.1 What it is

- An **in-process** OLAP database. Your Python (or Notebook) process *is* the database server.
- Speaks **standard SQL** with rich analytical extensions (window functions, `QUALIFY`, `PIVOT`, `UNNEST`, list/struct types, `ASOF JOIN`, etc.).
- Reads **Parquet, CSV, JSON, Arrow, Delta, Iceberg, Excel, Postgres, MySQL, SQLite** directly — often without copying data.
- Stores data in a single `.duckdb` file (or purely in-memory).

### 3.2 Install & first query

```python
%pip install duckdb
```

```python
import duckdb

con = duckdb.connect()           # in-memory; no file, no server

con.sql("""
    SELECT category, SUM(amount) AS total
    FROM read_parquet('/lakehouse/default/Files/orders/*.parquet')
    WHERE order_date >= '2026-01-01'
    GROUP BY category
    ORDER BY total DESC
""").show()
```

Key things to notice:

- `read_parquet()` is a **table function**, not a load step. DuckDB pushes the filter down and only reads the column chunks it needs.
- No schema declaration required.
- The result is a **DuckDB relation**; `.show()` prints it, `.df()` returns pandas, `.pl()` returns Polars, `.arrow()` returns an Arrow table.

### 3.3 The killer features (briefly)

- **`read_parquet` / `read_csv_auto` / `read_json_auto`** with globs and predicate pushdown.
- **`COPY (SELECT ...) TO 'out.parquet' (FORMAT PARQUET, PARTITION_BY (col))`** — partitioned writes in one statement.
- **Window functions + `QUALIFY`** — `QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY ts DESC) = 1` for "latest per key."
- **`ASOF JOIN`** — time-series joins ("most recent price as of each trade").
- **`PIVOT` / `UNPIVOT`** as first-class SQL.
- **`STRUCT` and `LIST` types** — JSON-shaped data without leaving SQL.
- **`SUMMARIZE table_name`** — instant per-column stats.
- **`EXPLAIN ANALYZE`** — actual query plan with timings.

### 3.4 When DuckDB shines

- Ad-hoc analytical queries over Parquet/Delta in OneLake.
- ELT transforms expressible in SQL (joins, aggregations, window functions).
- Anyone on the team already knows SQL.
- You want zero infrastructure — no cluster, no server, no session.

### 3.5 When DuckDB struggles

- Working set materially larger than node RAM.
- High-concurrency multi-writer scenarios on the same `.duckdb` file (it's not a concurrent OLTP DB).
- Heavy procedural logic — that's what Polars/Python are for.
- Real-time streaming — wrong tool.

---

## 4. Polars in depth

### 4.1 What it is

- A **DataFrame library** in Rust with Python bindings.
- **Columnar** storage on top of **Apache Arrow** — same memory format DuckDB uses, so zero-copy interop.
- **Multi-threaded** by default. Uses all your cores without you asking.
- Has two APIs:
  - **Eager** (`pl.DataFrame`) — executes immediately, pandas-like.
  - **Lazy** (`pl.LazyFrame`) — builds a query plan, optimizes it, executes when you call `.collect()`. **This is where most of Polars' performance comes from.**

### 4.2 Install & first query

```python
%pip install polars
```

```python
import polars as pl

lf = pl.scan_parquet('/lakehouse/default/Files/orders/*.parquet')  # lazy

result = (
    lf
    .filter(pl.col('order_date') >= pl.date(2026, 1, 1))
    .group_by('category')
    .agg(pl.col('amount').sum().alias('total'))
    .sort('total', descending=True)
    .collect()                  # now it runs — optimized end-to-end
)

print(result)
```

### 4.3 Eager vs. Lazy — pick lazy

- `pl.read_parquet(...)` → eager, loads everything immediately.
- `pl.scan_parquet(...)` → lazy, defers work.

For data engineering, **default to lazy**. It's almost always faster and uses less memory.

### 4.4 Expressions — the thing that makes Polars feel different

```python
import polars as pl

result = (
    pl.scan_parquet('orders.parquet')
    .with_columns([
        (pl.col('amount') * pl.col('quantity')).alias('line_total'),
        pl.col('order_date').dt.year().alias('year'),
        pl.when(pl.col('amount') > 1000).then(pl.lit('big')).otherwise(pl.lit('small')).alias('size_bucket'),
    ])
    .filter(pl.col('year') == 2026)
    .group_by(['region', 'size_bucket'])
    .agg([
        pl.col('line_total').sum().alias('revenue'),
        pl.col('order_id').n_unique().alias('orders'),
        pl.col('customer_id').n_unique().alias('customers'),
    ])
    .sort('revenue', descending=True)
    .collect()
)
```

One expression tree, one execution. No intermediate DataFrames.

### 4.5 The killer features (briefly)

- **Lazy query optimizer** — predicate pushdown, projection pushdown, common-subexpression elimination, join reordering.
- **Streaming engine** (`.collect(streaming=True)`) — process datasets larger than RAM in chunks.
- **Window expressions** — `pl.col('x').sum().over('group')` for grouped calcs without a join.
- **`join_asof`** — time-series joins.
- **`pivot` / `unpivot` / `explode`** — reshape data ergonomically.
- **First-class nested types** — `List`, `Struct`, with `.list.*` and `.struct.*` accessors.
- **Native Arrow** — zero-copy to/from DuckDB, pandas (via `.to_pandas()`), Arrow tables.

### 4.6 When Polars shines

- Long expression pipelines / method-chain ergonomics.
- Teams that prefer DataFrames over SQL.
- ETL with conditional logic, custom calculations, reshape operations.
- Quick iteration in a notebook — feels like pandas but actually fast.

### 4.7 When Polars struggles

- Working set materially larger than node RAM (streaming mode helps but isn't magic).
- Workloads dominated by SQL set operations — DuckDB is often more natural.
- Massive joins where you really need a distributed shuffle — that's Spark territory.

---

## 5. DuckDB vs. Polars — the differences

They overlap a lot, but they're fundamentally different *kinds* of tools.

### The one-line difference

- **DuckDB is a SQL database engine** that happens to run in your Python process.
- **Polars is a DataFrame library** that happens to have a query optimizer.

### Side-by-side

| Dimension | **DuckDB** | **Polars** |
|---|---|---|
| What it is | Embedded analytical **database** | **DataFrame library** |
| Primary API | **SQL** | **Method chains** (`.filter().group_by().agg()`) |
| Language | Written in C++ | Written in Rust |
| Stores data? | Yes — `.duckdb` file with tables, indexes, catalogs | No — operates on in-memory frames or files |
| Has a catalog? | Yes — schemas, tables, views, sequences | No — just Python variables |
| Transactions | ACID, `BEGIN/COMMIT/ROLLBACK` | None |
| Query optimizer | Mature SQL planner | Lazy plan optimizer for DataFrame expressions |
| Concurrency model | Multiple readers, single writer on a `.duckdb` file | N/A (just a library) |
| Streaming larger-than-RAM | Spills to disk automatically | Opt-in via `.collect(streaming=True)` |
| Custom Python logic | Awkward — UDFs or round-trip | Natural — it *is* Python |
| Best for | SQL-shaped ELT, ad-hoc analytics | DataFrame ETL pipelines, conditional logic, reshape |

### Same job, two styles

**DuckDB (SQL):**
```python
con.sql("""
    SELECT region, SUM(amount) AS total
    FROM read_parquet('orders/*.parquet')
    WHERE order_date >= '2026-01-01'
    GROUP BY region
""")
```

**Polars (DataFrame chain):**
```python
(
    pl.scan_parquet('orders/*.parquet')
      .filter(pl.col('order_date') >= pl.date(2026, 1, 1))
      .group_by('region')
      .agg(pl.col('amount').sum().alias('total'))
      .collect()
)
```

Same plan, same speed, different ergonomics.

### Where each wins

- **DuckDB has a *database* mental model.** `CREATE TABLE`, `CREATE VIEW`, `ATTACH` other databases (Postgres, SQLite, another DuckDB file), transactions, persistent files. You can re-open a `.duckdb` file later with all your tables intact. Polars has none of that.
- **Polars composes with Python more naturally.** `pl.when().then().otherwise()` inside expressions feels native; the DuckDB equivalent means string-building SQL or registering UDFs.
- **DuckDB is stronger at "pure SQL gymnastics"** — `QUALIFY`, `PIVOT`, `ASOF JOIN`, recursive CTEs, complex window frames read more cleanly in SQL.
- **Polars is stronger at long expression pipelines** — a 20-step ETL with conditional logic, reshape, type coercion reads more cleanly as a Polars chain.

### Performance — they're basically tied

Benchmarks bounce back and forth release-to-release. Real-world differences are usually under 2x and dominated by I/O, not engine choice. **Pick by ergonomics, not microbenchmarks.**

### How to choose

| If you… | Pick |
|---|---|
| Think in SQL, or your team does | **DuckDB** |
| Think in DataFrames / method chains | **Polars** |
| Need to persist tables and re-attach later in a single file | **DuckDB** |
| Need to compose tightly with Python logic and libraries | **Polars** |
| Want to query Postgres + Parquet + CSV in one statement | **DuckDB** (via `ATTACH`) |
| Want the cleanest path for "write Delta to Fabric Lakehouse" | **Polars** (`df.write_delta(...)` is one line) |
| Can't decide | Use **both** — they cost nothing to combine |

---

## 6. Why not just pandas? Or pyspark.pandas?

Both exist precisely because people keep wanting to. Here's the honest answer.

### 6.1 Why not just pandas?

You *can*. For tiny datasets (< a few hundred MB), it's fine. The problems show up as data grows or pipelines mature.

1. **It's single-threaded.** Your Fabric Python Notebook node has 16+ cores. Pandas uses one.
2. **It's memory-hungry — often 5–10x the file size.** Pandas is built on NumPy and stores strings as Python objects. A 1 GB Parquet file routinely needs 5–10 GB of RAM. Polars/DuckDB on Arrow typically need ~1–2x the file size.
3. **It has no query optimizer.** `pd.read_parquet('huge.parquet')` reads *everything* before you filter. `pl.scan_parquet(...)` pushes the filter into the read.
4. **Operations create copies, constantly.** Chain five `.assign(...)` calls and you've materialized five intermediate DataFrames.
5. **The API has 20 years of cruft.** `.loc` vs `.iloc` vs `.at`, `SettingWithCopyWarning`, index alignment surprises, inconsistent NaN handling.
6. **Delta Lake is awkward.** Pandas doesn't natively write Delta. Polars has `df.write_delta(...)` as one line.
7. **It's not really getting better.** Pandas 2.x added optional Arrow backing, but the single-thread execution model and API surface aren't changing.

**When pandas is the right choice:** datasets reliably under ~500 MB, legacy code you can't rewrite, or libraries that require pandas input (scikit-learn) — and even then, do the heavy lifting in Polars/DuckDB and call `.to_pandas()` at the boundary.

### 6.2 Why not just pyspark.pandas?

`pyspark.pandas` (formerly Koalas) is a **pandas-compatible API that runs on Spark**. It's a clever idea, and it's the wrong answer for the "small data" problem.

1. **It's still Spark.** Session warm-up, driver + executor JVMs consuming CU, cluster-shaped operational complexity. If your motivation for *not* using PySpark was "Spark is overkill for my 100 MB job," `pyspark.pandas` doesn't help. **It is Spark.**
2. **It's the worst of both worlds for small data.** Spark overhead plus pandas API quirks mapped onto a distributed engine where they translate awkwardly.
3. **It's not a full pandas implementation.** Many operations aren't implemented, behave differently, or fall back to single-node execution.
4. **Performance is rarely better than native PySpark.** If you're already on Spark, native PySpark DataFrames are faster and better documented.

**When `pyspark.pandas` is defensible:** a low-effort migration path from a large existing pandas codebase to native PySpark. It's a migration aid, not a target architecture.

### 6.3 The three tiers

Think of it as **three tiers**, not a binary choice:

| Tool | Scales? | Multi-core? | Memory-efficient? | Spark overhead? | Good default? |
|---|---|---|---|---|---|
| **pandas** | No | No | No | None | Only for < ~500 MB |
| **pyspark.pandas** | Yes | Yes (distributed) | OK | **Yes** | Rarely — migration aid only |
| **Polars** | Single-node (tens of GB) | Yes | Yes (Arrow) | None | **Yes**, for most "medium" data |
| **DuckDB** | Single-node (tens of GB) | Yes | Yes (Arrow) | None | **Yes**, for SQL-shaped work |
| **PySpark** | Distributed, unbounded | Yes | Yes | **Yes** | For genuinely big or streaming data |

1. **Tiny data, legacy code → pandas.**
2. **Small-to-medium data (MB to tens of GB) → Polars or DuckDB.** Most Fabric pipelines live here.
3. **Truly big data, streaming, distributed ML → PySpark.**

`pyspark.pandas` is a bridge from tier 1 to tier 3 that skips tier 2 — exactly the tier you want most pipelines to land in.

---

## 7. Using DuckDB and Polars together

Because both sit on Arrow, you can move data between them **without copying**:

```python
import duckdb, polars as pl

# Polars: scan + transform
lf = (
    pl.scan_parquet('/lakehouse/default/Files/bronze/orders/*.parquet')
    .filter(pl.col('order_date') >= pl.date(2026, 1, 1))
    .with_columns((pl.col('amount') * pl.col('quantity')).alias('line_total'))
)
df = lf.collect()

# DuckDB: gnarly window query on the Polars frame (no copy — queries `df` by name)
con = duckdb.connect()
result = con.sql("""
    SELECT
        region, customer_id, line_total,
        SUM(line_total) OVER (PARTITION BY customer_id ORDER BY order_date
                              ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS rolling_7
    FROM df
    QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY order_date DESC) = 1
""").pl()                       # back to Polars

# Polars: write Delta
result.write_delta('/lakehouse/default/Tables/silver_customer_rolling', mode='overwrite')
```

Common pattern: **Polars for the pipeline shape, DuckDB for the gnarly SQL bits.**

---

## 8. Fabric Lakehouses and Delta — does this work?

**Yes, completely.** Fabric Lakehouses are just Delta tables on OneLake (ADLS Gen2 under the hood), and both DuckDB and Polars speak Delta natively via `delta-rs`. You don't need Spark to participate in the Lakehouse.

### What you get for free

- **Read/write Delta tables** that show up in the Lakehouse explorer exactly like Spark-written tables.
- **Power BI Direct Lake** works against them — it reads the Delta files in OneLake regardless of who wrote them.
- **SQL endpoint** queries them.
- **Lineage and OneLake governance** still apply.

### Caveats worth knowing up front

1. **Table registration.** Spark notebooks auto-register tables in the Lakehouse metastore when you write to `/Tables/`. With `delta-rs`, you write the files to `/lakehouse/default/Tables/<name>` and Fabric's auto-discovery picks them up — usually within seconds, occasionally needing a refresh in the Lakehouse explorer. It works, it's just slightly less seamless than Spark's `saveAsTable`.
2. **Concurrent writers.** If multiple jobs MERGE into the same Delta table simultaneously, Spark's reference implementation is still the most battle-tested. For single-writer or serialized-writer pipelines (the common case), `delta-rs` is fine.
3. **Advanced Delta features.** Things like **deletion vectors, V2 checkpoints, liquid clustering, column mapping** — `delta-rs` support has been catching up fast but historically lags Spark by a release or two. For mainstream features (MERGE, time travel, schema evolution, partitioning, OPTIMIZE-style compaction, VACUUM), you're good.
4. **`OPTIMIZE` / `VACUUM`.** Available in `delta-rs`:
   ```python
   from deltalake import DeltaTable
   dt = DeltaTable('/lakehouse/default/Tables/silver_orders')
   dt.optimize.compact()           # small-file compaction
   dt.vacuum(retention_hours=168)  # cleanup
   ```
5. **Identity.** Use the notebook's workspace identity (default in Fabric Python Notebooks) — no connection strings or secrets needed for OneLake paths.

Standardizing on Fabric Lakehouses + Delta does **not** force you onto Spark. Both DuckDB and Polars are first-class Delta citizens.

---

## 9. Reading and writing Delta — who does what

The capability is symmetric, but the **convenience** differs.

| Operation | DuckDB | Polars |
|---|---|---|
| **Read Delta** | `delta_scan('/path')` (built-in) | `pl.scan_delta('/path')` (built-in) |
| **Write Delta** | via `deltalake.write_deltalake(...)` (one extra line) | `df.write_delta('/path')` (built-in) |
| **MERGE (upsert)** | via `deltalake` `TableMerger` API | `df.write_delta(..., mode='merge')` (built-in) |

Both end up calling the same underlying engine (`delta-rs`) and produce identical Delta tables.

### Polars — write Delta directly

```python
df.write_delta('/lakehouse/default/Tables/silver_orders', mode='overwrite')
```

### DuckDB — write Delta via `delta-rs`

```python
import duckdb
from deltalake import write_deltalake

result = duckdb.sql("""
    SELECT region, SUM(amount) AS total
    FROM delta_scan('/lakehouse/default/Tables/bronze_orders')
    GROUP BY region
""").arrow()                       # DuckDB → Arrow table (zero-copy)

write_deltalake(
    '/lakehouse/default/Tables/silver_orders_agg',
    result,
    mode='overwrite',
)
```

That `.arrow()` + `write_deltalake(...)` pair is the DuckDB equivalent of Polars' `.write_delta(...)`. Same result, two lines instead of one.

### Why the asymmetry

- **Polars** ships with opinionated, batteries-included I/O methods because it's a DataFrame *library* aimed at end-to-end pipelines.
- **DuckDB** is a SQL *engine*. Reads are extensions in its core; writes to specialized formats lean on the ecosystem (`delta-rs`). A native DuckDB Delta writer is on the roadmap and improving release-to-release, but as of mid-2026 the `delta-rs` route is still the production path.

### Practical implication

If you write a lot of Delta tables from DuckDB, factor the two-line pattern into a helper:

```python
def write_duckdb_to_delta(sql: str, path: str, mode: str = 'overwrite'):
    from deltalake import write_deltalake
    write_deltalake(path, duckdb.sql(sql).arrow(), mode=mode)
```

For MERGE-heavy workloads, Polars' built-in `mode='merge'` is more ergonomic today than the `deltalake.TableMerger` API directly.

### Polars MERGE pattern

```python
(
    df.write_delta(
        '/lakehouse/default/Tables/silver_orders',
        mode='merge',
        delta_merge_options={
            'predicate': 's.order_id = t.order_id',
            'source_alias': 's',
            'target_alias': 't',
        },
    )
    .when_matched_update_all()
    .when_not_matched_insert_all()
    .execute()
)
```

This is the Python-native equivalent of a Spark `MERGE INTO` and runs without any Spark session.

---

## 10. Getting started — a one-week onboarding plan

Short version: **start with one, add the other when you hit a wall.** Don't try to learn both at once.

### Step 1: Pick your starting tool

| If you naturally reach for… | Start with |
|---|---|
| `SELECT ... FROM ... GROUP BY ...` | **DuckDB** |
| `df.filter(...).group_by(...).agg(...)` | **Polars** |
| You have a SQL background (DBA, analyst, T-SQL dev) | **DuckDB** |
| You have a Python/pandas background | **Polars** |
| You don't know yet | **Polars** (slightly easier for general-purpose ETL) |

### Step 2: Use them together once you're comfortable

The pattern that emerges in real codebases:

- **Polars for the pipeline shape** — reading, filtering, deriving columns, writing Delta.
- **DuckDB for the gnarly SQL bit in the middle** — complex window functions, `QUALIFY`, `ASOF JOIN`, `PIVOT`.

### Day-by-day plan

**Day 1 — Setup and "hello world"** in a Fabric Python Notebook:
```python
%pip install polars duckdb deltalake
```
Read an existing Lakehouse table both ways and compare:
```python
import polars as pl, duckdb
pl.read_delta('/lakehouse/default/Tables/some_existing_table').head()
duckdb.sql("SELECT * FROM delta_scan('/lakehouse/default/Tables/some_existing_table') LIMIT 10").show()
```
Goal: prove the tools work against your Lakehouse.

**Day 2 — Build one real Bronze → Silver pipeline** in your chosen tool. Pick a small, real pipeline you currently run (or would run) in PySpark or pandas. Write the output to a new Delta table so you can compare.

**Day 3 — Add a MERGE (upsert).** This is the operation everyone needs eventually. Use the Polars MERGE pattern above. Goal: prove you can do incremental loads, not just full overwrites.

**Day 4 — Wire it into a Fabric Data Pipeline.** Schedule the notebook from a Pipeline. Confirm:
- It runs on the notebook's workspace identity (no secrets).
- The output table appears in the Lakehouse explorer.
- Power BI can query it via the SQL endpoint.

**Day 5 — Introduce the *other* tool.** Find one step in your pipeline that feels awkward — a complicated window function in Polars, or a long procedural transform in DuckDB. Rewrite *just that step* using the other tool. See how the zero-copy handoff feels.

After this week, you'll know which tool you reach for by default and where the other one earns its keep.

### Default project structure

Once you're past prototyping:

```
notebooks/
  bronze_ingest_orders.ipynb       # Polars: API/file → Bronze Delta
  silver_orders.ipynb              # Polars + DuckDB: Bronze → Silver Delta (MERGE)
  gold_orders_monthly.ipynb        # DuckDB: Silver → Gold aggregates
  _lib/
    delta_helpers.py               # write_delta wrappers, MERGE helpers
    schemas.py                     # column lists, type maps
```

### Know when to *stop* and reach for Spark

Even after you're fluent, these signals mean a specific step (not the whole pipeline) should be PySpark:

- Single table > ~30–50 GB after pushdown.
- Multiple concurrent writers to the same Delta table.
- Structured Streaming.
- Distributed ML (MLlib, SynapseML).

For everything else in the small-to-medium tier — which is most pipelines — Polars + DuckDB is the answer.

---

## 11. Common Fabric patterns

### 11.1 Bronze → Silver upsert with Polars + delta-rs

```python
import polars as pl

bronze = pl.scan_delta('/lakehouse/default/Tables/bronze_orders')

silver = (
    bronze
    .filter(pl.col('ingest_date') == pl.lit(pl.datetime_to_today()))
    .with_columns([
        pl.col('amount').cast(pl.Float64),
        pl.col('order_date').str.to_date('%Y-%m-%d'),
    ])
    .unique(subset=['order_id'], keep='last')
    .collect()
)

(
    silver.write_delta(
        '/lakehouse/default/Tables/silver_orders',
        mode='merge',
        delta_merge_options={
            'predicate': 's.order_id = t.order_id',
            'source_alias': 's',
            'target_alias': 't',
        },
    )
    .when_matched_update_all()
    .when_not_matched_insert_all()
    .execute()
)
```

### 11.2 Silver → Gold aggregation with DuckDB

```python
import duckdb
from deltalake import write_deltalake

con = duckdb.connect()
con.sql("INSTALL delta; LOAD delta;")

agg = con.sql("""
    SELECT
        region,
        DATE_TRUNC('month', order_date) AS month,
        COUNT(*)                         AS orders,
        COUNT(DISTINCT customer_id)      AS customers,
        SUM(amount)                      AS revenue
    FROM delta_scan('/lakehouse/default/Tables/silver_orders')
    GROUP BY region, DATE_TRUNC('month', order_date)
""").arrow()

write_deltalake('/lakehouse/default/Tables/gold_orders_monthly', agg, mode='overwrite')
```

### 11.3 REST API → Bronze with Polars

```python
import polars as pl, requests

data = requests.get('https://api.example.com/orders?since=2026-01-01', timeout=30).json()

df = pl.DataFrame(data)
df.write_delta('/lakehouse/default/Tables/bronze_orders_api', mode='append')
```

Spark would be the wrong tool for this entire pipeline.

---

## 12. Performance tips that actually matter

### DuckDB
- Use **`read_parquet` with globs**, not `read_csv`, whenever possible.
- Set threads if needed: `con.sql("SET threads TO 16")`.
- Set memory limit: `con.sql("SET memory_limit='32GB'")`.
- Use `EXPLAIN ANALYZE` to see what's actually slow.
- For repeated queries, store data in a `.duckdb` file with `CREATE TABLE AS SELECT ...`.

### Polars
- **Use `scan_*` not `read_*`.** Lazy beats eager almost always.
- **Push filters early** in the chain.
- **Avoid `.to_pandas()` in the middle of a pipeline** — it breaks Arrow zero-copy.
- **Stream when data is large**: `.collect(streaming=True)`.
- **Use expressions, not Python loops.** `df.with_columns([...])` with expressions is 100x faster than `.apply(lambda ...)`.

### Both
- Prefer **Parquet** over CSV for any data you control.
- **Partition writes** by a low-cardinality column (date, region) for downstream pushdown.
- Profile before optimizing. The bottleneck is usually I/O or your code, not the engine.

---

## 13. Gotchas and footguns

- **Polars `read_*` vs `scan_*`.** Forgetting to use `scan_*` on big files silently loads everything into RAM.
- **`with_columns([...])` order.** Polars evaluates the list in parallel by default; if you need sequential dependence, chain two `with_columns` calls.
- **Timestamps default to UTC** in both tools. Be explicit when reading CSVs from systems that aren't.
- **Delta concurrent writes from `delta-rs`** are improving but still less battle-tested than Spark's reference implementation. For multi-writer Silver tables, validate or fall back to Spark.
- **Polars expressions are *not* Python.** `if/else` in regular Python evaluates at definition time. Use `pl.when().then().otherwise()` inside expressions.
- **DuckDB extensions (`delta`, `azure`, `httpfs`) load per connection.** Re-load them in each new connection.
- **Memory.** Both are still single-node. Know your node size and your data size. Run `SUMMARIZE` (DuckDB) or `df.estimated_size()` (Polars) if you're guessing.

---

## 14. Cheat-sheet: equivalent operations

| Operation | DuckDB | Polars |
|---|---|---|
| Read Parquet | `read_parquet('*.parquet')` | `pl.scan_parquet('*.parquet')` |
| Read Delta | `delta_scan('/path')` | `pl.scan_delta('/path')` |
| Filter | `WHERE x > 10` | `.filter(pl.col('x') > 10)` |
| Project | `SELECT a, b` | `.select(['a', 'b'])` |
| New column | `SELECT a, a*2 AS b` | `.with_columns((pl.col('a')*2).alias('b'))` |
| Group by | `GROUP BY k` | `.group_by('k').agg(...)` |
| Join | `JOIN t2 USING (id)` | `.join(t2, on='id')` |
| Window | `SUM(x) OVER (PARTITION BY k)` | `pl.col('x').sum().over('k')` |
| Top-N per group | `QUALIFY ROW_NUMBER() OVER ... = 1` | `.group_by('k').agg(pl.col('x').top_k(1))` |
| Write Parquet | `COPY (...) TO 'out.parquet'` | `df.write_parquet('out.parquet')` |
| Write Delta | `delta-rs` (`write_deltalake`) | `df.write_delta(...)` |

---

## 15. Further reading

- DuckDB: https://duckdb.org/docs/
- Polars: https://docs.pola.rs/
- delta-rs (Python `deltalake` package): https://delta-io.github.io/delta-rs/
- Apache Arrow (shared in-memory format): https://arrow.apache.org/

---

## 16. Summary

- **DuckDB and Polars are both single-node, multi-core, columnar engines that fill the gap between pandas and Spark.**
- They are **fully compatible with Fabric Lakehouses and Delta tables** — both read and write Delta natively (Polars in one line via `write_delta`, DuckDB in two lines via `delta-rs`), producing files identical to what Spark writes.
- **DuckDB = SQL-first. Polars = DataFrame-first. Both = fast.**
- They interoperate through Arrow, so you don't have to choose one forever — common pattern is Polars for pipeline shape, DuckDB for gnarly SQL.
- **pandas** doesn't scale and is single-threaded; **`pyspark.pandas`** is still Spark and doesn't solve the small-data overhead problem. Polars/DuckDB occupy the "tier 2" that neither covers well.
- Reach for them whenever your data fits comfortably on one node — which, in 2025 on a Fabric Python Notebook, is most pipelines.
- When data genuinely exceeds single-node capacity, or you need streaming/MLlib/concurrent Delta writers, *then* fall back to PySpark (see `data-engineering-size-guidance.md`).
- **Getting started:** pick one tool, build 2–3 real pipelines including a MERGE and a scheduled Pipeline run, then add the other when a specific step calls for it.
