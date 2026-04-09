# Liquid Clustering vs. Partitioning in Fabric

  Liquid clustering has largely replaced Hive-style partitioning for most use cases in Microsoft Fabric / Delta Lake. Microsoft's official
  guidance is to prefer liquid clustering over partitioning for new tables. But there are nuances:

  When Liquid Clustering is the clear winner

   - Most analytical workloads — it handles multi-dimensional filtering without the rigid folder structure of partitioning
   - Evolving query patterns — you can change clustering keys without rewriting all data (ALTER TABLE ... CLUSTER BY)
   - High-cardinality columns — partitioning creates too many small files; liquid clustering handles this gracefully
   - Write-heavy workloads — no small-file problem from partition over-specification
   - Simplicity — no need to reason about partition pruning, OPTIMIZE, or Z-ORDER separately




# V-Order
V-Order is a write-time optimization to the Parquet file format developed by Microsoft that enables significantly
  faster reads across all Fabric compute engines (Power BI, SQL, Spark, and others).

  How It Works

  V-Order applies four key optimizations when writing Parquet files:

   1. Special sorting — Data within row groups is sorted to maximize compression and enable predicate pushdown.
   2. Row group distribution — Data is distributed across row groups in an optimized layout for analytical scan patterns.
   3. Dictionary encoding — Enhanced dictionary encoding reduces redundancy in column values.
   4. Compression — Improved compression yields up to 50% better compression ratios compared to standard Parquet.

  The result is that compute engines need less network, disk, and CPU resources to read the data.

  Performance Impact

  ┌─────────────────────────────────────┬────────────────────────────────────────────────────────┐
  │ Metric                              │ Impact                                                 │
  ├─────────────────────────────────────┼────────────────────────────────────────────────────────┤
  │ Write overhead                      │ ~15% slower writes on average                          │
  ├─────────────────────────────────────┼────────────────────────────────────────────────────────┤
  │ Compression improvement             │ Up to 50% better compression                           │
  ├─────────────────────────────────────┼────────────────────────────────────────────────────────┤
  │ Read speed (Verti-Scan engines)     │ In-memory-like access times (Power BI, SQL)            │
  ├─────────────────────────────────────┼────────────────────────────────────────────────────────┤
  │ Read speed (non-Verti-Scan engines) │ 10% faster on average, up to 50% in some cases (Spark) │
  └─────────────────────────────────────┴────────────────────────────────────────────────────────┘

  Power BI and SQL engines use Microsoft's Verti-Scan technology to get the most benefit from V-Ordered files, achieving   
  in-memory-like data access times.

  Key Properties

   - 100% open-source Parquet compliant — Any Parquet reader can read V-Ordered files as normal Parquet. There is no       
  vendor lock-in.
   - Orthogonal to Delta features — V-Order is applied at the Parquet file level and is fully compatible with Delta Lake   
  features like Z-Order, compaction, vacuum, and time travel.
   - Disabled by default in new Fabric workspaces (for Spark), to optimize for write-heavy ingestion workloads.

  Controlling V-Order in Spark

  ┌─────────────────┬────────────────────────────────────────────────┬─────────┐
  │ Scope           │ Configuration                                  │ Default │
  ├─────────────────┼────────────────────────────────────────────────┼─────────┤
  │ Session-level   │ spark.sql.parquet.vorder.default               │ false   │
  ├─────────────────┼────────────────────────────────────────────────┼─────────┤
  │ Table-level     │ TBLPROPERTIES("delta.parquet.vorder.enabled")  │ Unset   │
  ├─────────────────┼────────────────────────────────────────────────┼─────────┤
  │ Write operation │ DataFrame writer option parquet.vorder.enabled │ Unset   │
  └─────────────────┴────────────────────────────────────────────────┴─────────┘

  Enable it for read-heavy workloads (dashboards, interactive queries). Leave it disabled for write-heavy ETL/ingestion
  pipelines. You can also use the OPTIMIZE ... VORDER Spark SQL command to retroactively apply V-Order to existing tables
  without changing write-time settings.

  When to Use V-Order

   - ✅ Read-heavy: Interactive queries, Power BI dashboards, reporting — enable V-Order.
   - ❌ Write-heavy: High-throughput ingestion, streaming, ETL — leave V-Order disabled (default) to avoid the ~15% write
   overhead.
   - ✅ Mixed: Use OPTIMIZE ... VORDER as a maintenance step after ingestion completes to get the best of both worlds.



# Row Group distribution in detail
  Background: What Is a Row Group?

  A Parquet file organizes data in a columnar format. Within each file, rows are divided into horizontal slices called row groups. Each row group contains a set of column chunks — one per
  column — and each column chunk contains the actual column data for that slice of rows, organized into data pages.

   Parquet File
   ├── Row Group 0
   │   ├── Column Chunk: "CustomerID"  (pages of data)
   │   ├── Column Chunk: "OrderDate"   (pages of data)
   │   └── Column Chunk: "Amount"      (pages of data)
   ├── Row Group 1
   │   ├── Column Chunk: "CustomerID"
   │   ├── Column Chunk: "OrderDate"
   │   └── Column Chunk: "Amount"
   └── Footer (metadata, min/max stats, offsets)

  Row groups are the fundamental unit of parallelism — a query engine reads one row group at a time and can skip entire row groups using min/max statistics in the metadata.

  ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

  What "Row Group Distribution" Means in V-Order

  When Microsoft says V-Order applies "row group distribution", it refers to how V-Order intelligently decides which rows go into which row groups and how data is laid out within them.
  Specifically, this involves:

  1. Sizing Row Groups Optimally

  V-Order targets row group sizes that balance two competing concerns:

   - Too small (e.g., thousands of rows): Creates excessive overhead — more metadata, more dictionary merges, more I/O operations. This is the "small file/small row group" problem.
   - Too large (e.g., hundreds of millions of rows): Reduces the engine's ability to skip irrelevant data and increases memory pressure.

  V-Order targets row groups in the 1 million to 16 million row range (Fabric may go larger for highly compressible data), which is the sweet spot for Fabric's Verti-Scan and Direct Lake
  engines.

  2. Co-locating Related Data

  V-Order distributes rows across row groups so that rows with similar or related values end up in the same row group. This is done in concert with V-Order's sorting step. By sorting data
  before distributing it into row groups:

   - The min/max statistics stored in each row group's metadata become much tighter (narrower ranges).
   - Query engines can skip more row groups entirely when evaluating filter predicates (predicate pushdown / row group elimination).
   - Dictionary encoding becomes more efficient — each row group's local dictionary is smaller because the co-located data has lower cardinality within that slice.

  3. Uniform Row Group Sizes

  V-Order aims for uniform row group sizes across all row groups in a file. Non-uniform sizes (e.g., one row group with 50M rows and another with 10K rows) create:

   - Unbalanced parallelism (one task finishes instantly, another takes forever).
   - Suboptimal memory allocation.
   - Poor Direct Lake transcoding performance (each row group maps to a column segment in VertiPaq, and tiny segments are inefficient).

  ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------   

  Why It Matters for Fabric Engines

  ┌────────────────────────┬───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
  │ Engine                 │ How Row Group Distribution Helps                                                                                                                  │
  ├────────────────────────┼───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┤
  │ Power BI / Direct Lake │ Each row group maps 1:1 to a VertiPaq column segment. Fewer, larger, well-organized row groups → fewer segments → faster transcoding and queries. │
  ├────────────────────────┼───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┤
  │ SQL (Warehouse)        │ Better min/max stats from sorted distribution → more row group skipping → less data scanned.                                                      │
  ├────────────────────────┼───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┤
  │ Spark                  │ Uniform row group sizes → balanced task parallelism. Tighter stats → more effective predicate pushdown.                                           │
  └────────────────────────┴───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘

  ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------   

  Summary

  "Row group distribution" in V-Order is not just about deciding how many rows go in each group. It's a holistic layout strategy that sorts data, distributes it into uniformly-sized row groups   
  with tight value ranges, and aligns the layout with how Fabric's analytical engines actually consume data — resulting in better compression, faster predicate pushdown, reduced I/O, and more    
  efficient dictionary encoding.



  # Optimized Write
    Optimize Write is a Delta Lake feature that solves the "small file problem" by performing pre-write compaction (bin packing) — it shuffles in-memory data into optimally sized bins before
  Spark writes the Parquet files, producing fewer, larger files instead of many tiny ones.

  Why It Exists

  Apache Spark's parallelism model naturally produces one output file per task/partition. With many partitions or small data volumes per partition, this creates a flood of small files that:

   - Increase metadata overhead and scheduler pressure
   - Degrade read performance (more I/O operations, more file opens)
   - Require frequent post-write OPTIMIZE compaction to fix

  Optimize Write prevents the problem at the source rather than fixing it after the fact.

  How It Works

   Without Optimize Write:               With Optimize Write:
   ┌─────────┐                           ┌─────────┐
   │ Spark   │ → 200 tasks → 200 files   │ Spark   │ → 200 tasks
   │ Job     │    (5 MB each)            │ Job     │       ↓
   └─────────┘                           │         │   Shuffle/Bin-pack
                                         │         │       ↓
                                         └─────────┘ → 10 files (100 MB each)

  Before writing, Spark reshuffles data across partitions to create optimally sized output bins. The target file size scales adaptively from 128 MB (tables < 10 GB) up to **1 GB** (tables > 10
  TB).

  Configuration

  Optimize Write is enabled by default in Microsoft Fabric Spark Runtime.

  ┌─────────────────────────┬─────────────────────────────────────────────────────────┬──────────────────┐
  │ Scope                   │ Configuration                                           │ Default          │
  ├─────────────────────────┼─────────────────────────────────────────────────────────┼──────────────────┤
  │ Session-level           │ spark.databricks.delta.optimizeWrite.enabled            │ true             │
  ├─────────────────────────┼─────────────────────────────────────────────────────────┼──────────────────┤
  │ Table-level             │ TBLPROPERTIES('delta.autoOptimize.optimizeWrite')       │ Inherits session │
  ├─────────────────────────┼─────────────────────────────────────────────────────────┼──────────────────┤
  │ Partitioned tables only │ spark.microsoft.delta.optimizeWrite.partitioned.enabled │ Unset            │
  └─────────────────────────┴─────────────────────────────────────────────────────────┴──────────────────┘

  When It Helps

   - ✅ Partitioned tables — partitioning naturally fragments writes into many small files
   - ✅ Frequent small inserts — streaming or micro-batch ingestion
   - ✅ MERGE, UPDATE, DELETE — operations that touch many files

  When to Disable It

   - ❌ Write-latency-sensitive pipelines — the shuffle adds computational cost
   - ❌ Large batch writes that already produce well-sized files — the shuffle is wasted work

  Disable per-table:

   ALTER TABLE my_table SET TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'false')

  Optimize Write vs. OPTIMIZE Command

  ┌──────────┬────────────────────────────────────┬──────────────────────────────────────────────────┐
  │          │ Optimize Write                     │ OPTIMIZE Command                                 │
  ├──────────┼────────────────────────────────────┼──────────────────────────────────────────────────┤
  │ When     │ Pre-write (at ingestion time)      │ Post-write (maintenance)                         │
  ├──────────┼────────────────────────────────────┼──────────────────────────────────────────────────┤
  │ How      │ Shuffles in-memory before writing  │ Reads + rewrites existing files                  │
  ├──────────┼────────────────────────────────────┼──────────────────────────────────────────────────┤
  │ Cost     │ Adds shuffle overhead to writes    │ Rewrites data on disk                            │
  ├──────────┼────────────────────────────────────┼──────────────────────────────────────────────────┤
  │ Best for │ Preventing small files proactively │ Compacting accumulated small files retroactively │
  └──────────┴────────────────────────────────────┴──────────────────────────────────────────────────┘

  Best practice: Use Optimize Write as a first line of defense, and schedule OPTIMIZE during quiet windows as a safety net for any files that still need compaction.


  # Liquid Clustering
   What It Is

  Liquid clustering is a data layout optimization technique for Delta Lake tables that replaces both traditional partitioning and Z-Order. It organizes data within files based on clustering
  keys you define, so that rows with similar key values are co-located — enabling the query engine to skip irrelevant files entirely.

  The key innovation is the word "liquid": clustering keys can be changed at any time without rewriting existing data. The layout adapts over time as your query patterns evolve.

  ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

  How It Works

  1. Define Clustering Keys at Table Creation (or Later)

   CREATE TABLE sales (
       id INT,
       region STRING,
       sale_date DATE,
       amount DECIMAL
   ) CLUSTER BY (region, sale_date);

  Or on an existing unpartitioned table:

   ALTER TABLE sales CLUSTER BY (region, sale_date);

  2. Data Is Clustered Incrementally via OPTIMIZE

  Liquid clustering does not rewrite your data on every write. Instead, when you run OPTIMIZE, it:

   - Identifies unclustered or poorly clustered files
   - Rewrites them so rows with similar clustering key values are grouped into the same files
   - Updates the Delta log with tight min/max statistics per file

   OPTIMIZE sales;

  New writes land as unclustered files. The next OPTIMIZE folds them into the clustered layout.

  3. Queries Benefit from File Skipping

  When a query filters on clustering keys, the engine reads the file-level min/max stats and skips files that can't contain matching rows:

   -- Only reads files where region could be 'West' and sale_date could be in range
   SELECT * FROM sales WHERE region = 'West' AND sale_date >= '2025-01-01';

  4. Clustering Keys Can Change Without Rewrites

   -- Shift from region-based to product-based clustering
   ALTER TABLE sales CLUSTER BY (product_id, sale_date);

  Existing data stays as-is. Future OPTIMIZE runs progressively recluster data under the new keys. No expensive full-table rewrite is needed.

  ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------   

  How It Compares

  ┌─────────────────────────────┬─────────────────────────────────────┬────────────────────────────────────────────────┬───────────────────────────────────────┐
  │ Feature                     │ Partitioning                        │ Z-Order                                        │ Liquid Clustering                     │
  ├─────────────────────────────┼─────────────────────────────────────┼────────────────────────────────────────────────┼───────────────────────────────────────┤
  │ Layout mechanism            │ Filesystem directories              │ File-level co-location via space-filling curve │ File-level co-location via clustering │
  ├─────────────────────────────┼─────────────────────────────────────┼────────────────────────────────────────────────┼───────────────────────────────────────┤
  │ Can change keys?            │ ❌ Requires full rewrite            │ ✅ Per OPTIMIZE run                            │ ✅ ALTER TABLE — no rewrite needed    │
  ├─────────────────────────────┼─────────────────────────────────────┼────────────────────────────────────────────────┼───────────────────────────────────────┤
  │ High-cardinality columns    │ ❌ Creates too many tiny partitions │ ✅ Works well                                  │ ✅ Works well                         │
  ├─────────────────────────────┼─────────────────────────────────────┼────────────────────────────────────────────────┼───────────────────────────────────────┤
  │ Low-cardinality columns     │ ✅ Works well                       │ ✅ Works                                       │ ✅ Works                              │
  ├─────────────────────────────┼─────────────────────────────────────┼────────────────────────────────────────────────┼───────────────────────────────────────┤
  │ Compatible with partitions? │ N/A                                 │ ✅ Yes                                         │ ❌ No — replaces partitioning         │
  ├─────────────────────────────┼─────────────────────────────────────┼────────────────────────────────────────────────┼───────────────────────────────────────┤
  │ Concurrent writes           │ Conflict-prone                      │ Conflict-prone                                 │ ✅ Row-level concurrency support      │
  ├─────────────────────────────┼─────────────────────────────────────┼────────────────────────────────────────────────┼───────────────────────────────────────┤
  │ Maintenance complexity      │ Medium                              │ High (must re-run per partition)               │ Low (just run OPTIMIZE)               │
  └─────────────────────────────┴─────────────────────────────────────┴────────────────────────────────────────────────┴───────────────────────────────────────┘

  ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------   

  When to Use It

   - ✅ All new Delta tables — this is the recommended default
   - ✅ Tables filtered on high-cardinality columns (e.g., customer_id, product_id)
   - ✅ Tables with skewed data — clustering handles it better than partitioning
   - ✅ Fast-growing tables that need ongoing tuning
   - ✅ Tables with concurrent writers — supports row-level concurrency
   - ✅ Evolving query patterns — change keys without rewriting data

  When NOT to Use It

   - ❌ Already-partitioned tables — liquid clustering is incompatible with partitioned tables (use Z-Order instead, or migrate to an unpartitioned table)
   - ❌ Append-only tables that are never filtered — clustering adds OPTIMIZE cost with no query benefit
   - ❌ Tables that must be readable by very old Delta clients — liquid clustering requires Delta writer v7 / reader v3 (cannot be downgraded)

  ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------   

  Fabric-Specific Notes

  ┌────────────────────────────┬───────────────────────────┐
  │ Fabric Runtime             │ Liquid Clustering Support │
  ├────────────────────────────┼───────────────────────────┤
  │ Spark Runtime 1.3+         │ Read and write            │
  ├────────────────────────────┼───────────────────────────┤
  │ Spark Runtime 1.2          │ Read only                 │
  ├────────────────────────────┼───────────────────────────┤
  │ SQL Analytics Endpoint     │ Read only                 │
  ├────────────────────────────┼───────────────────────────┤
  │ Power BI Direct Lake       │ Read only                 │
  ├────────────────────────────┼───────────────────────────┤
  │ Pipelines / Dataflows Gen2 │ Read only                 │
  └────────────────────────────┴───────────────────────────┘

  In Fabric, you typically create and cluster tables in Spark notebooks, then consume them from SQL, Power BI, or other engines that support reading clustered tables.


# Optimize - bin compaction
Automatic options so you don't have to manually run OPTIMIZE

 1. Auto Compaction (post-write, automatic)

  This is the closest to "set a property and forget it." After every write operation, Spark automatically evaluates whether the partition/table has too many small files, and if so, triggers a
  synchronous OPTIMIZE immediately.

  Enable at table level (persists across sessions):

   ALTER TABLE my_table
   SET TBLPROPERTIES ('delta.autoOptimize.autoCompact' = 'true');

  Or at session level:

   spark.conf.set('spark.databricks.delta.autoCompact.enabled', 'true')

  Trade-off: Each write may take slightly longer because it sometimes triggers compaction inline. This is fine for streaming/micro-batch ingestion but can add latency spikes to time-sensitive
  pipelines.

  ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

  2. Optimize Write (pre-write, automatic)

  This doesn't run OPTIMIZE at all — instead it prevents small files from being created in the first place by shuffling data into well-sized bins before writing. Enabled by default in Fabric.

   ALTER TABLE my_table
   SET TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true');

  ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

  3. Scheduled OPTIMIZE (manual but scriptable)

  For full control, you can schedule OPTIMIZE in a notebook or Spark job definition and orchestrate it via a Fabric pipeline on a cadence (e.g., nightly). This is the only way to apply Z-Order
  or liquid clustering layout — auto compaction does bin-packing only, not data reordering.

  ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

  How They Work Together

  ┌─────────────────┬──────────────────────────────────┬─────────────────────────────────────────────────────┬───────────────────────┐
  │ Mechanism       │ When it runs                     │ What it does                                        │ Reorders data?        │
  ├─────────────────┼──────────────────────────────────┼─────────────────────────────────────────────────────┼───────────────────────┤
  │ Optimize Write  │ During write (pre-write shuffle) │ Prevents small files                                │ ❌ No                 │
  ├─────────────────┼──────────────────────────────────┼─────────────────────────────────────────────────────┼───────────────────────┤
  │ Auto Compaction │ After write (post-write)         │ Merges small files into larger ones                 │ ❌ No (bin-pack only) │
  ├─────────────────┼──────────────────────────────────┼─────────────────────────────────────────────────────┼───────────────────────┤
  │ OPTIMIZE        │ On-demand / scheduled            │ Merges files + applies Z-Order or liquid clustering │ ✅ Yes                │
  └─────────────────┴──────────────────────────────────┴─────────────────────────────────────────────────────┴───────────────────────┘

  Recommended Combination

   ALTER TABLE my_table SET TBLPROPERTIES (
       'delta.autoOptimize.optimizeWrite' = 'true',
       'delta.autoOptimize.autoCompact' = 'true'
   );

  Use both together as your first line of defense, then schedule explicit OPTIMIZE runs when you need clustering or Z-Order applied. Auto compaction handles the day-to-day small file problem;    
  scheduled OPTIMIZE handles the data layout optimization.


# VACUUM however, must always be explicitly triggered
Options for triggering
Option 1: Lakehouse UI (Ad-hoc)

  Right-click a table in the Lakehouse explorer → Table maintenance → enable "Remove unreferenced old files" and set a retention period. This is manual and one-off.

  Option 2: Notebook / Spark Job Definition (Schedulable)

   VACUUM schema_name.table_name RETAIN 168 HOURS; -- 7 days (default)

  You can put this in a notebook, then orchestrate it on a schedule via a Fabric Pipeline (e.g., nightly or weekly).

  Option 3: Lakehouse REST API (Programmable)

  Use the Table Maintenance API to trigger VACUUM programmatically — useful for automating across many tables:

   POST https://api.fabric.microsoft.com/v1/workspaces/{workspaceId}/lakehouses/{lakehouseId}/jobs/instances?jobType=TableMaintenance

  The request body includes the vacuum retention period alongside any other maintenance operations (OPTIMIZE, V-Order, etc.).

  ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

  Important Considerations

  ┌───────────────────┬───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
  │ Concern           │ Detail                                                                                                                                │
  ├───────────────────┼───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┤
  │ Default retention │ 7 days — files older than this and not in the current Delta version are deleted                                                       │
  ├───────────────────┼───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┤
  │ Minimum retention │ 7 days enforced by default. Going lower requires setting spark.databricks.delta.retentionDurationCheck.enabled = false — risky        │
  ├───────────────────┼───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┤
  │ Time travel       │ VACUUM permanently deletes old Parquet files. After vacuuming, you cannot time-travel to versions that referenced those files         │
  ├───────────────────┼───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┤
  │ Direct Lake       │ If a Power BI semantic model is framed on an older Delta version, vacuuming can break it — always re-frame (refresh) before vacuuming │
  └───────────────────┴───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘

  ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

  Typical Pattern

  Most teams combine OPTIMIZE and VACUUM into a single scheduled maintenance notebook:

   tables = ["bronze.raw_events", "silver.clean_events", "gold.daily_summary"]

   for table in tables:
       spark.sql(f"OPTIMIZE {table}")
       spark.sql(f"VACUUM {table} RETAIN 168 HOURS")

  Then schedule that notebook in a pipeline to run nightly or weekly during a quiet window. There's no "set and forget" table property for VACUUM — it always requires an explicit trigger. 


# Best Practices
- It is best to define the schema prior to reading the data
    - This is because spark's inferred schema is not guaranteed to have a 100% match to the source datatypes and the datatypes can lead to performance issues when reading large sets of data
- Avoid UDFs
    - This is because UDFs operate serially one by one
- Avoid Expensive Operations
    - These are things like:
        - collect(): brings all data to the driver, possibly causing out of memory errors
        - count(): Forces Spark to iterate one by one over the dataset
        - distinctCount(): Gives count of all distinct items
        - distinct():  Use dropDuplicates() instead
        - repartition() helpful when needing to resolve skew, but requires a full shuffle.  Use coalesce() instead
        - groupByKey():  use reduceByKey() to perform the same combining of data before sending over network




# Spark Monitoring
Spark Application Hierarchy:
Spark Application
   ├── Job
   │   ├── Stage
   │   │   ├── Task
   │   │   └── Task
   │   └── Stage
   │       └── Task
   └── Job
       └── Stage
           ├── Task
           └── Task

APIs and Tools that can assist with Spark Monitoring

  1. Job Insight Library (In-Notebook, Scala)

  The most direct way to get structured execution data (jobs, stages, tasks, executors) inside a notebook:

   // Scala — requires Fabric Runtime 1.3+
   val insight = spark.jobInsight.analyze()

   // Get structured datasets
   val jobs = insight.jobs
   val stages = insight.stages
   val tasks = insight.tasks
   val executors = insight.executors

   jobs.show()
   stages.show()

  You can persist these to lakehouse tables for historical reporting. PySpark is not supported yet — Scala only.

  https://learn.microsoft.com/en-us/fabric/data-engineering/job-insight-library


  ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

  2. Apache Livy REST API (Remote Submission & Status)

  Submit and monitor Spark sessions/batches programmatically from outside Fabric:

   # Get session status
   GET https://{workspace-url}/livyApi/versions/2023-12-01/sparkPools/default/sessions/{sessionId}

   # Get batch job status
   GET https://{workspace-url}/livyApi/versions/2023-12-01/sparkPools/default/batches/{batchId}

  Returns job state, app ID, and log URLs. Useful for CI/CD pipelines or external orchestrators.

  ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

  3. Fabric REST APIs (Item-Level Monitoring)

  Monitor notebook/Spark job definition runs at the Fabric platform level:

   # Get item job instances (runs)
   GET https://api.fabric.microsoft.com/v1/workspaces/{workspaceId}/items/{itemId}/jobs/instances

  Returns run status, start/end times, and failure details.

  ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------   

  4. Azure Log Analytics Integration (Historical Telemetry)

  For persistent, queryable monitoring data across all runs, configure the Fabric diagnostic emitter to send Spark metrics to a Log Analytics workspace:

   - Spark application metrics (job/stage/task durations, data read/written)
   - Executor metrics (memory, CPU, shuffle)
   - Driver and executor logs

  Query with KQL in Log Analytics for dashboards and alerting.

  ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------   

  5. Spark UI / Spark History Server (Built-In)

  Not an API per se, but accessible programmatically — the Spark History Server link is available from the monitoring detail page and provides the full Spark UI (DAG visualization, event
  timeline, SQL tab, etc.).

  ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------   

  Summary

  ┌──────────────────────┬─────────────────────────────────────────────────────────┬────────────────────────────────────┐
  │ Tool                 │ Use Case                                                │ Access Method                      │
  ├──────────────────────┼─────────────────────────────────────────────────────────┼────────────────────────────────────┤
  │ Job Insight Library  │ Interactive analysis of jobs/stages/tasks in a notebook │ Scala API in notebook              │
  ├──────────────────────┼─────────────────────────────────────────────────────────┼────────────────────────────────────┤
  │ Livy REST API        │ Remote job submission & status polling                  │ HTTP REST                          │
  ├──────────────────────┼─────────────────────────────────────────────────────────┼────────────────────────────────────┤
  │ Fabric REST API      │ Run history & status for notebooks/Spark jobs           │ HTTP REST                          │
  ├──────────────────────┼─────────────────────────────────────────────────────────┼────────────────────────────────────┤
  │ Log Analytics        │ Historical metrics, alerting, dashboards                │ KQL queries                        │
  ├──────────────────────┼─────────────────────────────────────────────────────────┼────────────────────────────────────┤
  │ Spark History Server │ Full Spark UI with DAG, SQL plans, event timeline       │ Browser / link from Monitoring Hub │
  └──────────────────────┴─────────────────────────────────────────────────────────┴────────────────────────────────────┘
  6. Profiling Spark Notebooks with Sparklens
  https://blog.fabric.microsoft.com/en-us/blog/profiling-microsoft-fabric-spark-notebooks-with-sparklens?ft=Anu%20Venkataraman:author

    Sparklens is an open-source Spark profiling tool (by Qubole) with a built-in Spark scheduler simulator. Its primary purpose is to help you understand the scalability limits of your Spark
  application — specifically, whether adding more executors will actually make it faster.

  What It Does

  Yes, it gives executor sizing recommendations — but no, it doesn't suggest code changes.

  Here's specifically what it reports:

  ✅ Executor Sizing Simulation

  This is its killer feature. From a single run, it simulates how your app would perform with different executor counts:

   Executor count    31  ( 10%) estimated time 87m 29s  cluster utilization 92.73%
   Executor count    62  ( 20%) estimated time 47m 03s  cluster utilization 86.19%
   Executor count   155  ( 50%) estimated time 22m 51s  cluster utilization 71.01%
   Executor count   310  (100%) estimated time 14m 49s  cluster utilization 54.73%

  This tells you the point of diminishing returns — e.g., going from 62 to 155 executors cuts time in half, but going from 155 to 310 barely helps.

  ✅ Bottleneck Identification

   - Identifies stages that limit scalability (e.g., a single stage with massive skew blocks everything)
   - Shows per-stage compute utilization (available vs. used core-hours)
   - Highlights driver time vs. compute time
   - Detects data skew at the stage level

  ✅ Job/Stage Timeline Visualization

  Shows how parallel stages were scheduled and where gaps exist.

  ❌ Does NOT Suggest Code Changes

  Sparklens tells you where the problem is (which stage, what kind of bottleneck) but not how to fix your code. You still need to interpret the results — for example:

   - Low utilization on a stage → maybe you need to repartition
   - One stage dominates → look for skew or expensive shuffles
   - More executors don't help → the bottleneck is likely sequential (driver-side or single-partition)

  Fabric Compatibility

  Sparklens is a JVM-based tool that hooks into Spark via spark.extraListeners. It works with any Spark environment including Fabric, but:

   - It's a Scala/JVM library — you configure it via Spark conf, not PySpark code
   - You'd add it as a JAR or package to your Fabric Spark environment
   - It can also run offline against Spark event history logs

   # Configure in PySpark session (the tool itself is JVM-based)
   spark.conf.set("spark.extraListeners", "com.qubole.sparklens.QuboleJobListener")

  Summary

  ┌─────────────────────────────────┬─────────────────────────────────────────────────────────────────┐
  │ Capability                      │ Sparklens                                                       │
  ├─────────────────────────────────┼─────────────────────────────────────────────────────────────────┤
  │ Executor count recommendations  │ ✅ Yes — simulates different counts from a single run           │
  ├─────────────────────────────────┼─────────────────────────────────────────────────────────────────┤
  │ Cluster utilization analysis    │ ✅ Yes — per-stage and overall                                  │
  ├─────────────────────────────────┼─────────────────────────────────────────────────────────────────┤
  │ Bottleneck stage identification │ ✅ Yes — pinpoints limiting stages                              │
  ├─────────────────────────────────┼─────────────────────────────────────────────────────────────────┤
  │ Skew detection                  │ ✅ Yes — at stage level                                         │
  ├─────────────────────────────────┼─────────────────────────────────────────────────────────────────┤
  │ Code change suggestions         │ ❌ No — tells you where the problem is, not how to fix the code │
  ├─────────────────────────────────┼─────────────────────────────────────────────────────────────────┤
  │ Driver vs. compute time split   │ ✅ Yes                                                          │
  └─────────────────────────────────┴─────────────────────────────────────────────────────────────────┘

  For code-level suggestions, Fabric's built-in Spark Advisor (visible in the notebook UI) is closer to what you're looking for — it detects skew, suggests configuration changes, and flags       
  common anti-patterns directly in the notebook.

  # Sparklens Modes
  1. Live (Configured Before the Run)

  Attach it as a listener before your application starts. It collects metrics during execution and reports at the end:

   --conf spark.extraListeners=com.qubole.sparklens.QuboleJobListener

  2. Offline Against Event History Logs (Fully Ad-Hoc)

  You can run Sparklens after the fact against any Spark application's event history file — no prior configuration needed:

   spark-submit --packages qubole:sparklens:0.3.2-s_2.11 \
     --class com.qubole.sparklens.app.ReporterApp \
     qubole-dummy-arg <event-history-file> source=history

  This is the true ad-hoc mode. As long as you have the Spark event history log (which Fabric retains via the Spark History Server), you can analyze any past run without having configured
  anything beforehand.

  3. Offline Against Sparklens JSON (Deferred Reporting)

  Run with the listener but disable reporting at runtime, then analyze the saved JSON file later:

   --conf spark.sparklens.reporting.disabled=true
   --conf spark.sparklens.data.dir=/path/to/output/

  ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

  Bottom line: If you just want to analyze a job that already ran, use mode 2 with the event history file — no advance setup required.

  # How to get the Event History File
  Download Event Log via REST API

   GET https://api.fabric.microsoft.com/v1/workspaces/{workspaceId}/notebooks/{itemId}/livySessions/{livyId}/applications/{appId}/{attemptId}/logs

  Step-by-step:

   1. Find your Spark application — use the workspace-level API to list Livy sessions: GET https://api.fabric.microsoft.com/v1/workspaces/{workspaceId}/notebooks/{itemId}/livySessions

   This gives you the livyId and appId (e.g., application_1742369571479_0001).
   2. Download the event log: GET .../livySessions/{livyId}/applications/{appId}/1/logs

   This returns the Spark event history file that you can feed directly into Sparklens.
   3. Run Sparklens against it: spark-submit --packages qubole:sparklens:0.3.2-s_2.11 \
      --class com.qubole.sparklens.app.ReporterApp \
      qubole-dummy-arg <downloaded-event-log-file> source=history

  Alternative: Download from Spark UI

  If you prefer a manual approach:

   1. Open the Monitoring Hub → find your notebook/Spark job run
   2. Click into the run → click Spark History Server
   3. From the Spark UI, download the event log directly

  This is also the approach Microsoft recommends for production troubleshooting — production support engineers download logs from Spark UI and share them with developers who can analyze locally
  with tools like Sparklens or a local Spark History Server.