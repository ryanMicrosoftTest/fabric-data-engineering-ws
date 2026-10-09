# 04 — T-SQL to Spark Pattern Library

Translation patterns for the constructs that appear most often in Fabric Warehouse ETL.
All examples use generic object names.

Target engine notation:
- **Spark SQL** — runs in a Fabric notebook (`%%sql`) or Spark job definition.
- **PySpark** — runs in a Fabric notebook.
- **MLV** — materialized lake view, declarative.

---

## 1. Full reload of a table

### Warehouse (T-SQL)

```sql
CREATE OR ALTER PROCEDURE silver.usp_load_dim_customer
AS
BEGIN
    TRUNCATE TABLE silver.dim_customer;

    INSERT INTO silver.dim_customer (customer_id, customer_name, region, effective_from)
    SELECT  src.customer_id,
            UPPER(LTRIM(RTRIM(src.customer_name))),
            src.region,
            SYSUTCDATETIME()
    FROM    bronze.raw_customer AS src
    WHERE   src.is_deleted = 0;
END;
```

### Lakehouse (Spark SQL)

```sql
CREATE OR REPLACE TABLE silver.dim_customer
USING DELTA
AS
SELECT  src.customer_id,
        UPPER(TRIM(src.customer_name))  AS customer_name,
        src.region,
        CURRENT_TIMESTAMP()             AS effective_from
FROM    bronze.raw_customer AS src
WHERE   src.is_deleted = 0;
```

### Lakehouse (PySpark)

```python
from pyspark.sql import functions as F

(
    spark.table("bronze.raw_customer")
         .filter(F.col("is_deleted") == 0)
         .select(
             "customer_id",
             F.upper(F.trim("customer_name")).alias("customer_name"),
             "region",
             F.current_timestamp().alias("effective_from"),
         )
         .write
         .mode("overwrite")
         .option("overwriteSchema", "true")
         .saveAsTable("silver.dim_customer")
)
```

**Notes**
- `TRUNCATE` + `INSERT` in one transaction becomes a single atomic Delta overwrite.
  That is actually *safer* — the table is never empty to a concurrent reader.
- `LTRIM(RTRIM(x))` → `TRIM(x)`.
- `SYSUTCDATETIME()` → `CURRENT_TIMESTAMP()` (verify session time zone; Spark defaults
  are configurable — set `spark.sql.session.timeZone` explicitly to `UTC` if you relied
  on UTC semantics).

---

## 2. Incremental upsert (`MERGE`)

### Warehouse (T-SQL)

```sql
MERGE INTO silver.dim_product AS tgt
USING staging.product_delta AS src
    ON tgt.product_id = src.product_id
WHEN MATCHED AND tgt.row_hash <> src.row_hash THEN
    UPDATE SET tgt.product_name = src.product_name,
               tgt.category     = src.category,
               tgt.row_hash     = src.row_hash,
               tgt.updated_at   = SYSUTCDATETIME()
WHEN NOT MATCHED BY TARGET THEN
    INSERT (product_id, product_name, category, row_hash, updated_at)
    VALUES (src.product_id, src.product_name, src.category, src.row_hash, SYSUTCDATETIME());
```

### Lakehouse (Spark SQL)

```sql
MERGE INTO silver.dim_product AS tgt
USING staging.product_delta AS src
    ON tgt.product_id = src.product_id
WHEN MATCHED AND tgt.row_hash <> src.row_hash THEN
    UPDATE SET tgt.product_name = src.product_name,
               tgt.category     = src.category,
               tgt.row_hash     = src.row_hash,
               tgt.updated_at   = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN
    INSERT (product_id, product_name, category, row_hash, updated_at)
    VALUES (src.product_id, src.product_name, src.category, src.row_hash, CURRENT_TIMESTAMP());
```

**Notes**
- Delta `MERGE INTO` is the closest analogue and the syntax is largely compatible.
- `WHEN NOT MATCHED BY TARGET` → `WHEN NOT MATCHED`.
- `WHEN NOT MATCHED BY SOURCE` is supported in Delta Lake; verify behaviour on your
  runtime version before relying on it.
- The merge key column should be a good clustering candidate — see
  [06-operations-and-performance.md](06-operations-and-performance.md).

---

## 3. `IDENTITY` surrogate keys

`IDENTITY` has **no Lakehouse equivalent.** Pick one of three strategies.

### Strategy A — deterministic hash key (recommended)

Stable, reproducible, parallel-safe, no coordination needed.

```sql
SELECT
    xxhash64(CAST(customer_id AS STRING), '|', CAST(source_system AS STRING)) AS customer_sk,
    customer_id,
    source_system
FROM bronze.raw_customer;
```

```python
from pyspark.sql import functions as F

df = df.withColumn(
    "customer_sk",
    F.xxhash64(F.concat_ws("|", F.col("customer_id").cast("string"),
                                F.col("source_system").cast("string")))
)
```

**Trade-off:** keys are 64-bit and non-sequential. Collision probability is negligible
for typical dimension cardinalities but is not zero — validate uniqueness in tests.

### Strategy B — monotonic key with a high-water mark

Preserves "integer-looking" keys for downstream systems that expect them.

```python
from pyspark.sql import functions as F
from pyspark.sql.window import Window

max_sk = spark.sql("SELECT COALESCE(MAX(customer_sk), 0) AS m FROM silver.dim_customer").first()["m"]

w = Window.orderBy("customer_id")
new_rows = new_rows.withColumn("customer_sk", F.row_number().over(w) + F.lit(max_sk))
```

**Trade-off:** requires a full ordering, so it does not parallelise well and is not safe
under concurrent writers. Use only for single-writer dimension loads.

### Strategy C — keep the natural key

Often the right answer. If the natural key is stable and reasonably compact, the
surrogate key may simply be unnecessary overhead in a columnar, Direct Lake world.

---

## 4. Slowly Changing Dimension Type 2

### Lakehouse (Spark SQL, two-step)

```sql
-- Step 1: close out changed current rows
MERGE INTO silver.dim_customer AS tgt
USING staging.customer_delta AS src
    ON  tgt.customer_id = src.customer_id
    AND tgt.is_current   = TRUE
WHEN MATCHED AND tgt.row_hash <> src.row_hash THEN
    UPDATE SET tgt.is_current   = FALSE,
               tgt.valid_to     = CURRENT_TIMESTAMP();

-- Step 2: insert new current rows for new and changed keys
INSERT INTO silver.dim_customer
SELECT  src.customer_id,
        src.customer_name,
        src.region,
        src.row_hash,
        CURRENT_TIMESTAMP()  AS valid_from,
        CAST(NULL AS TIMESTAMP) AS valid_to,
        TRUE                 AS is_current
FROM    staging.customer_delta AS src
LEFT JOIN silver.dim_customer AS tgt
       ON  tgt.customer_id = src.customer_id
       AND tgt.is_current   = TRUE
WHERE   tgt.customer_id IS NULL
   OR   tgt.row_hash <> src.row_hash;
```

> ⚠️ These are **two separate Delta commits**. In the Warehouse they would have been one
> transaction. Make the whole notebook idempotent and re-runnable: if step 2 fails after
> step 1 succeeded, a rerun must not double-close or double-insert. The `is_current` +
> `row_hash` predicates above are designed for exactly that.

---

## 5. Temp tables and intermediate results

| T-SQL | Lakehouse equivalent | When to use |
|---|---|---|
| `#tmp` used once, small | `df.createOrReplaceTempView("tmp")` | Default |
| `#tmp` reused many times | `df.cache()` then `createOrReplaceTempView` | Reused DataFrames |
| `#tmp` large and reused across notebooks | Write a real Delta table in a `staging` schema | Cross-notebook handoff |
| `@table` variable | Spark temp view | Default |
| `SELECT ... INTO #tmp` | `CREATE OR REPLACE TEMP VIEW tmp AS SELECT ...` | Default |

```sql
CREATE OR REPLACE TEMP VIEW tmp_order_agg AS
SELECT customer_id, SUM(order_amount) AS total_amount
FROM   silver.fact_order
GROUP BY customer_id;
```

---

## 6. Cursors and procedural loops

Cursors rarely survive translation. Convert to set-based logic.

### T-SQL (anti-pattern)

```sql
DECLARE cur CURSOR FOR SELECT region FROM dim_region;
OPEN cur; FETCH NEXT FROM cur INTO @region;
WHILE @@FETCH_STATUS = 0
BEGIN
    INSERT INTO gold.region_summary
    SELECT @region, SUM(order_amount) FROM silver.fact_order WHERE region = @region;
    FETCH NEXT FROM cur INTO @region;
END
```

### Lakehouse (set-based)

```sql
CREATE OR REPLACE TABLE gold.region_summary AS
SELECT region, SUM(order_amount) AS total_amount
FROM   silver.fact_order
GROUP BY region;
```

If genuine per-partition iteration is required (for example, per-tenant processing with
different rules), drive it from Python over a list, not a cursor:

```python
for region in [r.region for r in spark.table("silver.dim_region").select("region").collect()]:
    process_region(region)
```

Prefer `foreachBatch`, window functions, or `GROUPING SETS` before resorting to loops.

---

## 7. `COPY INTO` replacement

### Warehouse

```sql
COPY INTO bronze.raw_order
FROM 'https://<account>.blob.core.windows.net/landing/orders/*.parquet'
WITH (FILE_TYPE = 'PARQUET');
```

### Lakehouse — option A: shortcut + Spark

Create a OneLake shortcut to the landing container, then:

```python
(
    spark.read.parquet("Files/landing/orders/")
         .write.mode("append")
         .saveAsTable("bronze.raw_order")
)
```

### Lakehouse — option B: Spark Structured Streaming with Auto Loader semantics

For continuously arriving files, use a streaming read with a checkpoint so each file is
processed exactly once:

```python
(
    spark.readStream
         .format("cloudFiles")
         .option("cloudFiles.format", "parquet")
         .load("Files/landing/orders/")
         .writeStream
         .option("checkpointLocation", "Files/_checkpoints/raw_order")
         .trigger(availableNow=True)
         .toTable("bronze.raw_order")
)
```

### Lakehouse — option C: pipeline Copy activity

Lowest-code. Use when there is no transformation and the schedule is simple.

---

## 8. Stored procedures that only read

Read-only procedures **do** carry over. The SQL analytics endpoint supports creating
views, functions, and stored procedures.

```sql
-- Valid on the Lakehouse SQL analytics endpoint
CREATE OR ALTER PROCEDURE gold.usp_get_region_summary
    @region VARCHAR(50)
AS
BEGIN
    SELECT region, total_amount
    FROM   gold.region_summary
    WHERE  region = @region;
END;
```

Audit each procedure first. If it contains any `INSERT`, `UPDATE`, `DELETE`, `MERGE`,
`TRUNCATE`, `CREATE TABLE`, or `SELECT ... INTO <permanent>`, it must be rewritten.

---

## 9. Views

Views carry over unchanged in syntax, but their **Direct Lake consequences differ**:

| Scenario | Consequence |
|---|---|
| Report built on a SQL view, Direct Lake on SQL | Falls back to **DirectQuery** |
| Report built on a SQL view, Direct Lake on OneLake | **Not supported** |
| Equivalent logic as a **materialized lake view** | Produces a real Delta table — stays in Direct Lake mode |

So: keep views for ad-hoc SQL and semantic abstraction, but **materialize anything that
feeds a Direct Lake semantic model.**

---

## 10. Materialized lake views as a stored-procedure replacement

For a large class of straightforward medallion transformations, MLVs are a better
target than hand-written notebooks: you write the SQL, Fabric handles execution,
storage, refresh strategy (incremental / full / skip), dependency ordering between
views, and data-quality constraints.

```sql
CREATE MATERIALIZED LAKE VIEW gold.mlv_daily_sales AS
SELECT  o.order_date,
        p.category,
        SUM(o.order_amount) AS total_amount,
        COUNT(*)            AS order_count
FROM    silver.fact_order  AS o
JOIN    silver.dim_product AS p
     ON p.product_id = o.product_id
GROUP BY o.order_date, p.category;
```

**Good fit for**
- Frequently accessed aggregations.
- Complex joins across large tables queried often.
- Uniformly applied data-quality rules, declared rather than coded.
- Reporting datasets that should refresh when sources change.
- Bronze → silver → gold transformations expressible in SQL.

**Poor fit for**
- One-time or rarely accessed queries.
- Non-SQL logic: ML inference, API calls, complex Python.
- Sub-second streaming requirements.

PySpark authoring of MLVs is in preview and currently supports **full refresh only** —
confirm current status before designing around incremental PySpark MLVs.

---

## 11. Function mapping quick reference

| T-SQL | Spark SQL |
|---|---|
| `SYSUTCDATETIME()`, `GETUTCDATE()` | `CURRENT_TIMESTAMP()` (set `spark.sql.session.timeZone = 'UTC'`) |
| `GETDATE()` | `CURRENT_TIMESTAMP()` |
| `ISNULL(a, b)` | `COALESCE(a, b)`, `NVL(a, b)` |
| `LEN(x)` | `LENGTH(x)` |
| `LTRIM(RTRIM(x))` | `TRIM(x)` |
| `CHARINDEX(a, b)` | `INSTR(b, a)`, `LOCATE(a, b)` |
| `SUBSTRING(x, s, l)` | `SUBSTRING(x, s, l)` |
| `DATEADD(day, n, d)` | `DATE_ADD(d, n)`, `d + INTERVAL n DAYS` |
| `DATEDIFF(day, a, b)` | `DATEDIFF(b, a)` |
| `CONVERT(DATE, x)` | `CAST(x AS DATE)`, `TO_DATE(x)` |
| `FORMAT(d, 'yyyy-MM')` | `DATE_FORMAT(d, 'yyyy-MM')` |
| `TOP n` | `LIMIT n` |
| `IIF(c, a, b)` | `IF(c, a, b)` |
| `STRING_AGG(x, ',')` | `CONCAT_WS(',', COLLECT_LIST(x))` |
| `TRY_CAST(x AS INT)` | `TRY_CAST(x AS INT)` |
| `NEWID()` | `UUID()` |
| `HASHBYTES('SHA2_256', x)` | `SHA2(x, 256)` |
| `ROW_NUMBER() OVER (...)` | `ROW_NUMBER() OVER (...)` |
| `CROSS APPLY` | `LATERAL VIEW`, or a join with `EXPLODE` |
| `OPENJSON` | `FROM_JSON`, `GET_JSON_OBJECT` |
| `PIVOT` | `PIVOT` (supported in Spark SQL) |

Always validate type-coercion and rounding behaviour. Decimal precision and implicit
casts are the most common source of silent numeric drift between the two engines.

---

## 12. Idempotency checklist for every rewritten job

Because you lose multi-table transactions, every job must satisfy:

- [ ] Re-running the same batch produces an identical result (no duplicates, no
      double-counting).
- [ ] The job either completes fully or leaves the target in a previously valid state.
- [ ] A `batch_id` and `load_ts` are stamped on every written row.
- [ ] Failure mid-way is detectable from a control table, not only from logs.
- [ ] A documented recovery action exists (replay the batch, or Delta `RESTORE` to a
      known version).
- [ ] Downstream consumers read only batches marked complete.
