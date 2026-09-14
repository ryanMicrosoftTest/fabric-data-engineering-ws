Practical Polars vs PySpark Boundaries by Medallion Layer
Bottom line — my recommended default boundaries

There is no universal “Polars until X GB, Spark after Y GB” rule. The right boundary depends on working-set size, RAM, I/O layout, and whether the job is a narrow append/filter job or a wide shuffle-heavy job. The best evidence I found points to this practical rule: use Polars aggressively for single-node, small-to-medium, mostly narrow workloads; move to Spark/PySpark when the job needs distributed execution, fault tolerance, streaming checkpoints, large joins/shuffles, or lakehouse-native operational features. Polars is explicitly optimized for single-machine parallel/vectorized execution and out-of-core streaming, but some operations fall back to in-memory execution; Spark is built around driver/executor processes on a cluster and task distribution across executors.

Recommended thresholds below are for compressed Parquet/Delta-equivalent data processed by one job or one load window, not the total historical table size. This distinction matters because one benchmark mapped TPC-DS scale factors to much smaller compressed working sets: 1 GB scale → 140 MB compressed, 10 GB scale → 1.2 GB compressed, 100 GB scale → 12.7 GB compressed.

Medallion layer	Default to Polars	Gray zone: benchmark and profile	Default to PySpark/Spark	Why the boundary differs by layerBronze	<10 GB per ingest batch/window	10–50 GB	>50 GB, or continuous/high-frequency ingestion	Bronze is mostly append/raw landing, so Polars can go farther when transformations are narrow. Spark becomes preferable when ingestion is continuous, checkpointed, multi-source, file-management-heavy, or operationally critical.
Silver	<5–10 GB working set	10–25 GB	>25 GB	Silver adds cleansing, dedupe, joins, type casting, late-arrival handling, and normalization; these create memory pressure and shuffle-like costs earlier.
Gold	<25 GB working set or output mart	25–100 GB	>100 GB scan/shuffle, or high-concurrency/BI production serving	Gold often reduces data via aggregations, so Polars can regain advantage even if upstream Bronze/Silver used Spark. Spark remains better when the gold build scans large Silver history or serves enterprise lakehouse/warehouse workloads.

These are conservative production boundaries for a typical cloud/dev node around 8–16 vCPU and 32–64 GB RAM. If you run on a very large single node, the Polars boundary moves upward: the Polars team’s 2026 PDS-H benchmark used a single m8id.32xlarge with 128 vCPUs, 512 GB RAM, and 2.7 TB SSD and reported Polars about 6.4× faster than PySpark on total runtime for roughly terabyte-scale uncompressed CSV-equivalent data; on a distributed setup with the same total vCPU/RAM spread across 32 workers, Polars Cloud reported about 3.2× faster than PySpark. That result is important, but it does not mean ordinary Python Polars on a 32 GB workstation should be treated as a Spark replacement at hundreds of GB.

Evidence that sets the size bands

The strongest size-specific benchmark I found is Miles Cole’s 2025 Fabric-focused “Small Data Showdown.” It tested compressed working sets of 140 MB, 1.2 GB, 12.7 GB, and an extrapolated larger run at 127 GB. In that benchmark, single-node engines beat Spark at 140 MB; Polars still had a large advantage at 1.2 GB; at 12.7 GB, Spark with Fabric Native Execution Engine became the most reliable across compute sizes while Polars could be fastest only on larger compute and hit OOM at lower compute; at 127 GB, Spark was the only engine to complete all compute sizes, while Polars failed early with OOM.

A second ETL-oriented benchmark from DataCoolie used an 8-core, 32 GB RAM, NVMe SSD machine and identical append, merge-upsert, and SCD2 workloads. It found Polars 5–10× faster for small jobs under 100K rows, 2–3× faster for 100K–10M rows, “depends” for 10M–100M rows / ~10 GB, and Spark as the default for 100M+ rows or ~40 GB+ workloads or cluster environments.

Coiled’s TPC-H benchmarks add a useful caution: for local small data, 10 GB or less, Polars/DuckDB are good choices; for larger cloud-scale work, scalable systems such as Spark/Dask become necessary because single-machine engines hit stability, I/O, or scale limits. Coiled also noted Polars performed very well locally at modest scale but struggled with large multi-table joins and cloud readers in that benchmark, while Spark scales out and completes across large scales, albeit with configuration complexity.

That is why I set Bronze >50 GB, Silver >25 GB, and Gold >100 GB as the default Spark boundaries. They are not “laws”; they are evidence-backed operating points: Bronze can tolerate larger Polars batches because work is often narrow; Silver should shift earlier because joins/dedupe/state inflate memory and shuffle cost; Gold can shift later because the curated working set is often smaller than the raw input.

Assumptions behind the boundaries

Data size means per-job working set. For Bronze, that means one ingest batch/window; for Silver, the bytes scanned plus the data retained for joins, dedupe, and merge logic; for Gold, the bytes scanned and shuffled to produce the final aggregate, not just the final table size. This matters because compressed lakehouse bytes can be far smaller than benchmark scale-factor names imply.

Storage format is Parquet/Delta where possible. Fabric guidance says Bronze may keep original formats, while Silver and Gold typically use Delta tables; it also notes Delta brings performance and extra capabilities over generic files. Delta Lake itself provides ACID transactions, scalable metadata handling, and unifies streaming and batch processing on data lakes.

Hardware is a normal single node unless stated otherwise. Polars uses all available CPU cores on a machine, optimizes queries, and supports out-of-core streaming so results don’t all have to be in memory at once; however, Polars also says some operations are inherently non-streaming or not implemented in streaming mode and will fall back to the in-memory engine. Spark, by contrast, can acquire executors across nodes and send tasks to those executors, which is why it becomes the safer default once the workload no longer fits one machine comfortably.

Spark’s advantage is not free. Spark shuffles for joins and aggregations, and its own SQL tuning docs expose spark.sql.shuffle.partitions for joins/aggregations, spark.sql.files.maxPartitionBytes for file reads, broadcast thresholds, and AQE features for coalescing shuffle partitions and skew joins. In other words, Spark is not automatically faster; it is more scalable and operationally robust when the workload requires distributed execution.

Bronze layer boundary
Bronze characteristics

Bronze is the raw landing layer. Microsoft’s medallion guidance says Bronze stores data in original form, Silver fixes/standardizes/deduplicates, and Gold organizes data for reports and dashboards; Azure Databricks similarly describes Bronze as raw ingestion, Silver as cleaning/validation, and Gold as dimensional modeling/aggregation. Fabric guidance also says Bronze can store original formats or Parquet/Delta, while Silver and Gold should generally be Delta tables.

Recommended Bronze boundary
Bronze workload	Recommended engine<10 GB per ingest window; file landing, schema capture, light projection/filtering	Polars
10–50 GB per window; append-only, few transformations, streaming-compatible Polars plan, enough local SSD/RAM	Polars possible; profile
>50 GB per window, continuous ingestion, many source feeds/tables, checkpointing, MERGE/upsert, compaction, or production SLA	Spark/PySpark

Why 50 GB? Bronze transformations are often narrow and append-heavy, so Polars can process more data than it can in Silver. But at production scale Bronze problems are often not CPU math problems; they are incremental ingestion, file discovery, small-file handling, metadata growth, checkpointing, and exactly-once correctness problems. Spark Structured Streaming is explicitly a scalable, fault-tolerant stream-processing engine with checkpointing and write-ahead logs, and it supports streaming aggregations, event-time windows, and stream-to-batch joins through the same DataFrame API. Fabric guidance also recommends aiming for approximately 1 GB data files for query performance and notes Bronze may tolerate smaller files, while Silver/Gold should optimize for larger files and larger row groups.

My Bronze rule: if the Bronze job is a simple batch landing/conversion job, keep Polars until roughly 50 GB per load window if it profiles cleanly. If it is continuous ingestion into Delta, many tables, high-frequency appends, or requires checkpoint/retry semantics, use Spark much earlier—even below 10 GB—because the operational model matters more than raw speed. Fabric guidance says partitioning is usually good for high-frequency Bronze ingestion, while Silver/Gold should use query-performance-oriented optimization such as liquid clustering rather than simple partitioning.

Silver layer boundary
Silver characteristics

Silver is where the workload gets expensive. Azure Databricks describes Silver as data cleanup and validation, including dropping nulls, quarantining invalid records, joining datasets such as customer and transactions, and creating cleaned/enhanced datasets; the same documentation lists Silver operations such as schema enforcement, null handling, deduplication, late/out-of-order handling, quality checks, type casting, and joins.

Recommended Silver boundary
Silver workload	Recommended engine<5–10 GB working set; cleansing, type casting, light dedupe, small dimension joins	Polars
10–25 GB; joins/dedupe with moderate cardinality, SCD-like logic, some repartition-like behavior	Profile; choose by memory peak and skew
>25 GB, large joins, high-cardinality groupby, dedupe over history, MERGE/upsert, skew, late-arriving records, stateful processing	Spark/PySpark

Why the Silver boundary is lower than Bronze: Silver creates intermediate state. Joins, aggregations, dedupe, sorts, and window-like logic can require large hash tables or sorted intermediates. Polars can be extremely fast when the plan fits memory or streams well, but its own docs warn that not all operations stream and fallback to in-memory execution can happen. Spark has shuffle overhead, but that overhead buys distributed memory, partition-level parallelism, AQE, broadcast join options, and skew handling knobs for wide transformations.

This is the layer where I would be most conservative. Miles Cole’s benchmark found that at 12.7 GB compressed, Polars could still be fastest at higher compute sizes but hit OOM below 16 vCores, while Spark completed across compute sizes and was the fastest reliable option overall. DataCoolie similarly found that around 10 GB the winner depends on whether the data fits memory and how shuffle-heavy the workload is, while 40 GB+ / 100M+ rows should default to Spark or cluster execution.

My Silver rule: use Polars for developer iteration, unit/CI transformations, and small/medium conformance jobs. In production, once a Silver job crosses ~25 GB working set or includes large joins/dedupes over historical data, treat Spark as the default unless a Polars benchmark proves otherwise.

Gold layer boundary
Gold characteristics

Gold is the curated serving layer. Azure Databricks describes Gold as business-user-oriented datasets such as customer spending, account performance, sales pipeline summaries, and highly aggregated executive summaries; it also says Gold represents refined views that drive dashboards, ML, applications, and reporting, often aggregated or filtered for specific periods or regions.

Recommended Gold boundary
Gold workload	Recommended engine<25 GB working set or output mart; dimensional marts, feature extracts, ad-hoc aggregations, dashboard-ready extracts	Polars
25–100 GB; mostly aggregations, manageable group cardinality, large single node available	Polars possible; Spark if scheduled enterprise pipeline
>100 GB scan/shuffle, large fact-to-dimension rebuilds, high-concurrency BI, semantic model/warehouse serving, enterprise governance	Spark/PySpark

Why Polars can “come back” in Gold: Gold output is often much smaller and more aggregated than Silver input. If Spark already produced clean Silver tables, a Gold data mart or feature table may fit comfortably on one node; in that situation Spark’s JVM/session/distributed overhead can dominate, and Polars’ vectorized single-node engine often wins. This aligns with the small-data benchmark results where single-machine engines were strongest at 140 MB and 1.2 GB compressed workloads.

Why Spark still owns large Gold builds: Gold jobs often scan large Silver history even if the final output is small. If the build needs to aggregate >100 GB of detailed fact data, Spark’s distributed execution, fault tolerance, and shuffle controls become more valuable than Polars’ single-node speed. The 127 GB benchmark is the clearest datapoint: Spark was the only engine to complete all compute sizes, while Polars failed early.

Workload modifiers that move the boundary
Workload trait	Move boundary toward Polars when…	Move boundary toward Spark when…Filters/projections	Mostly narrow, column-pruned scans; few intermediates	Many downstream writes, file compaction, or object-store metadata dominates
Joins	One side is small; join cardinality is controlled	Large fact-to-fact joins, skew, many-to-many joins, or join state exceeds RAM
Aggregations	Low/medium cardinality groupby; output shrinks sharply	High-cardinality groupby, windows, stateful streaming, or large shuffle
Streaming	Micro-batches are small and operational semantics are not critical	Exactly-once semantics, checkpointing, late-arriving data, retries, or continuous jobs are required
I/O	Few large local Parquet files on fast SSD	Many small files, ADLS/S3 listing overhead, Delta table maintenance, or catalog/governance integration
Platform operations	Local dev, CI, lightweight feature engineering	Production lakehouse, Unity Catalog/Hive/Fabric integration, monitoring, retries, security, lineage

The key technical difference is this: Polars is fast because it avoids distribution overhead and uses local vectorized execution; Spark is useful because it accepts distribution overhead to gain scale, fault tolerance, and operational visibility. Polars’ docs emphasize all-core execution, query optimization, and out-of-core streaming; Spark’s docs describe driver/executor cluster execution and task scheduling across nodes.

Practical decision rule you can use with customers

Measure the per-run compressed working set, not total table size. For Bronze, use the ingest window; for Silver, use input plus expected join/dedupe state; for Gold, use the scan/shuffle size, not just final output.

Start with Polars if the job is single-node, batch, narrow, and below the layer threshold: Bronze <10 GB, Silver <5–10 GB, Gold <25 GB. This is especially attractive for local development, CI, exploratory transforms, and small production extracts.

Switch to Spark when you exceed the layer’s Spark boundary, or earlier if you need distributed execution, Structured Streaming fault tolerance, checkpointing, large joins/shuffles, Delta maintenance, platform governance, or production observability.

Treat the gray zones as benchmark zones. If Polars completes with peak memory below a safe operational limit and stable runtime, keep it. If you see OOM, spilling, high-cardinality joins, skew, or long-running opaque jobs, move to Spark. This mirrors the benchmark evidence: Polars can be fastest where it completes, but Spark is more reliable as workloads approach tens to hundreds of GB and require distributed execution.

In short: for a Medallion Architecture, I would not use one universal cutoff. I would use Bronze: Spark after ~50 GB, Silver: Spark after ~25 GB, and Gold: Spark after ~100 GB, with Polars strongly favored below those ranges when the job is batch, narrow, and single-node-friendly.
