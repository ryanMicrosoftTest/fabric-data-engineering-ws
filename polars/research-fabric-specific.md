Fabric F64 baseline: updated Polars vs PySpark boundaries by Medallion layer

Short answer: with a full Fabric F64 available to the data engineering team, the decision boundary does not simply move upward for Polars. It splits by workload shape. Polars gets a higher ceiling for narrow, single-node-friendly batch work because Fabric Python notebooks can scale vertically up to 64 vCores and remain single-node/no-distributed-overhead. Spark becomes the safer default earlier for Silver-style joins, dedupes, merges, streaming, and production Delta workflows because F64 can provide 128 Spark vCores base and up to 384 Spark vCores with burst when the pool is configured for it, while Fabric Spark also has native Delta support and the Native Execution Engine for batch workloads.

Updated practical thresholds under F64

Use these ranges as compressed Parquet/Delta working-set size per job, not raw CSV size. Miles Cole’s Fabric benchmark explicitly shows why this matters: a “100GB scale factor” represented 12.7GB compressed in his subset benchmark, roughly an 8x difference for that data shape.

Medallion layer	Polars default	Benchmark / gray zone	PySpark default	Spark mandatory triggersBronze ingestion / append / file normalization	0–10GB compressed per batch	10–50GB if batch-only, simple schema/casts/projections, low shuffle	50GB+ or high file count	Any continuous streaming, checkpointed ingestion, many-source fan-in, Delta merge/upsert, production retry requirements
Silver cleansing / joins / dedupe / conforming	0–5GB; up to 10GB only if narrow	5–15GB benchmark both	15GB+ for joins/dedupe/shuffle; 25GB+ strongly Spark	Multi-way joins, skew, CDC/merge, update/delete, row tracking, Change Data Feed writes, Liquid Clustering, operational monitoring
Gold aggregates / curated marts / serving tables	0–25GB scan if aggregation reduces heavily	25–75GB if output is small and logic is simple	75–100GB+ scan or shuffle	Large fact scans, windowing, heavy group-by, semantic-model optimization, V-Order/Liquid/OPTIMIZE/VACUUM governance

How this differs from the earlier non-cluster baseline: Bronze simple batch can tolerate a larger Polars zone than before because the single-node Fabric Python kernel can be scaled vertically; Silver does not move much upward because joins, dedupe, and shuffles are exactly where distributed execution and Spark AQE matter; Gold is the most forgiving layer for Polars because curated outputs are often smaller than the raw scan, but Spark still dominates once the scan or shuffle is around the 100GB compressed class.

1. What F64 really gives Spark vs Polars

A Fabric F64 has 64 Capacity Units, and for Spark, Microsoft documents 1 capacity unit = 2 Spark vCores. That gives 128 Spark vCores base. Fabric Spark bursting can allow up to 3x the purchased Spark vCores, so an F64 can reach 384 Spark vCores, and Microsoft’s current documentation says a single Spark job can consume all 384 vCores if the pool is configured large enough and the capacity is otherwise free.

The practical Spark pool shapes are important: Fabric Spark node sizes range from Small 4 vCore/32GB through XX-Large 64 vCore/512GB, and an F64 example allows up to 96 small nodes, 48 medium nodes, 24 large nodes, 12 X-Large nodes, or 6 XX-Large nodes at the 384-vCore burst envelope. Spark also uses a driver/head node and executor workers; Fabric documents a 1:1 node-to-executor model, with one node dedicated to the driver and remaining nodes used for executors, except in single-node mode where driver/executor resources are shared or split depending on configuration.

By contrast, Fabric’s Python kernel is a single-node environment: Microsoft’s notebook kernel guide says it defaults to 2 vCores / 1 CU, can start up to 64 vCores / 32 CU, and has no distributed execution. The Python notebook docs also state the default is 2 vCores / 16GB, include preinstalled DuckDB, Polars, and Scikit-learn, and support %%configure values of 4, 8, 16, 32, and 64 vCores, with memory allocated to match the vCore selection.

Implication: under F64, Spark has a much larger parallel envelope than Polars: 384 distributed Spark vCores vs 64 single-node Python vCores under the assumptions here. Polars still wins when it avoids distributed overhead and the working set fits; Spark wins when the data must be partitioned, shuffled, checkpointed, retried, or managed as production Delta tables.

2. Benchmark evidence that changes the boundary

The most directly applicable Fabric evidence is Miles Cole’s 2025 “Small Data Showdown,” which used Fabric engines and explicitly moved from scale-factor labels to compressed data size. At 140MB compressed, single-machine engines beat Spark; at 1.2GB compressed, Polars still had a large advantage; at 12.7GB compressed, Fabric Spark with the Native Execution Engine “started to flex,” Spark completed all compute scales without OOM, while Polars OOM’d below 16 vCores but was very fast when it completed.

The same benchmark then extrapolated to a ~127GB compressed workload and found that Spark was the only engine to complete on all tested compute sizes, while Polars failed within minutes; Spark was about 3.5x faster than DuckDB at 32 vCores and about 6x faster at 64 vCores. Microsoft’s own notebook-selection guide now aligns with this pattern: Python engines win at ultra-small scale, remain attractive around 1–2GB, Spark becomes competitive or faster around 10–13GB compressed, and Spark is the fastest/reliable choice around 100GB+.

There is also counter-evidence worth preserving: an Endjin Fabric benchmark found DuckDB/Polars-style in-process engines strongly competitive on a realistic Fabric workload and argued they can be faster and cheaper for many medium-scale workloads; however, that published workload was around 30 million rows / ~5GB raw CSV, and the conclusion is strongest for single-node-friendly analytics rather than heavy Silver-style joins or production Delta operations. Polars’ own 2026 benchmark also shows strong single-node and distributed performance, but its distributed result uses Polars Cloud, not the standard Fabric Python notebook, so I would not treat it as applicable to “Polars in Fabric Python notebook” unless you add Polars Cloud to the architecture.

3. Medallion layer mapping

Microsoft’s Fabric medallion guidance defines Bronze as storing data as it arrives, Silver as fixing errors/standardizing/removing duplicates, and Gold as organizing data for reports and dashboards. It also recommends keeping Bronze data in original format where possible, while Silver and Gold typically use Delta tables, and notes that Delta provides ACID-backed reliability plus batch and streaming support.

Bronze: ingestion, append-heavy workloads, streaming vs batch

For batch-only Bronze ingestion, Polars remains attractive up to about 10GB compressed and can be benchmarked up to 50GB compressed if the work is narrow: file reads, schema application, column projection, casting, light enrichment, and append-only writes. This is the place where Polars’ no-cluster overhead and Fabric Python’s vertical scaling help most.

For streaming Bronze, Spark should be the default even at small sizes. Fabric’s streaming guidance describes Spark Structured Streaming as scalable and fault-tolerant, able to write directly to Delta, use checkpoint locations, optimize streaming writes, and run production streams as Spark job definitions with retry policy. Apache Spark’s own Structured Streaming guide describes end-to-end exactly-once fault tolerance through checkpointing and write-ahead logs in micro-batch mode.

Updated Bronze boundary under F64:

Polars: 0–10GB compressed batch; possibly 10–50GB if simple and benchmarked.
Spark: 50GB+ compressed batch, high file counts, or any continuous/checkpointed ingestion.
Key shift: F64 lets Spark handle very large ingestion, but it also lets Polars survive larger narrow batches than a laptop/non-cluster baseline.
Silver: joins, dedupe, conformance, shuffle-heavy transforms

Silver is where the F64 baseline most strongly favors Spark. The reason is not just data size; it is shuffle shape. Spark’s own tuning docs show spark.sql.shuffle.partitions controls partitions for joins/aggregations, spark.sql.files.maxPartitionBytes defaults to 128MB for file reads, and AQE can coalesce shuffle partitions, convert join strategies, and optimize skew at runtime. Fabric-specific tuning guidance likewise treats shuffles as expensive because wide joins/aggregations move data across the network, materialize shuffle files, and can spill under memory pressure.

Under F64, Spark can spread a Silver job over many executors. That delays executor OOM and gives AQE more room to rebalance work, while Polars is still constrained to one machine’s memory and one process space. The 2025 Fabric benchmark supports this: at 12.7GB compressed, Spark completed across all compute sizes, while Polars needed at least 16 vCores and still had OOM sensitivity; at 127GB compressed, Polars failed and Spark completed.

Updated Silver boundary under F64:

Polars: 0–5GB compressed production default; up to 10GB only for narrow transforms.
Gray zone: 5–15GB compressed; benchmark both.
Spark: 15GB+ compressed for joins/dedupes; 25GB+ strongly Spark.
Key shift: compared with a non-cluster baseline, Silver’s Spark switch point moves earlier, not later, because the full F64 makes distributed shuffles cheap enough to use and operationally safer.
Gold: aggregations, curated datasets, BI-ready tables

Gold is more nuanced. If Gold jobs read a moderate dataset and produce a much smaller curated aggregate, Polars can remain viable longer than in Silver because the working set may shrink quickly. Fabric’s medallion guidance says Gold is organized for reports/dashboards and that Silver/Gold should optimize larger files and row groups for consumption-engine query performance.

However, when Gold scans large facts, performs window functions, joins multiple conformed dimensions, or prepares large semantic-model tables, Spark becomes the safer default. Microsoft’s Fabric medallion guidance recommends Liquid Clustering instead of partitioning for Silver and Gold query performance, and the notebook selection guide shows Spark becoming the fastest/reliable choice around 100GB+ compressed.

Updated Gold boundary under F64:

Polars: 0–25GB compressed scan/output if aggregation is simple and output is much smaller.
Gray zone: 25–75GB compressed if the operation is mostly group-by/filter/projection.
Spark: 75–100GB+ compressed scan, or any Gold workload needing V-Order/Liquid/table maintenance/large semantic-model serving.
Key shift: Polars can remain useful at higher Gold sizes than Silver, but Spark becomes the “standard operating model” once Gold tables are part of governed, optimized Fabric serving paths.
4. Spark scaling model on F64: how to think about partitions and memory

For a full F64 batch job, I would model Spark in two envelopes: base envelope = 128 Spark vCores and burst envelope = 384 Spark vCores, assuming job-level bursting is enabled and the capacity is not shared with other workloads. A medium-node burst pool could be configured as 48 medium nodes of 8 vCores each; because one node is the driver, executor parallelism is effectively the worker-node portion, not every vCore in the pool.

For scan-heavy stages, start from Spark’s default file partitioning: spark.sql.files.maxPartitionBytes is 128MB, so a 100GB compressed Delta/Parquet scan can produce roughly hundreds to thousands of input partitions depending on files and layout; this is enough to keep a large F64 pool busy if files are well-sized. Fabric’s medallion guidance also warns that big data platforms perform better with fewer large files than many small files and recommends aiming around 1GB data files for query performance, while allowing smaller Bronze files when Spark handles downstream prep.

For shuffle-heavy stages, do not leave spark.sql.shuffle.partitions=200 blindly on a 128–384-core pool. Spark’s default is 200, but with F64 that can under-parallelize joins/aggregations; use AQE and set a higher initial shuffle partition count, then let AQE coalesce, split skewed partitions, and adjust join strategies based on runtime statistics.Heuristic: start around 2–4x active executor cores for shuffle partitions, or target 128–256MB post-shuffle partitions, then validate in Spark UI using task duration, shuffle read/write, spill, and skew metrics.

5. Final practitioner heuristics

Use Polars when the whole working set fits comfortably on one Fabric Python node, the job is batch-only, and the transform is narrow. Fabric Python notebooks are fast to start, include Polars, and can scale to 64 vCores, but they do not provide distributed execution.

Use Spark when data growth, operational maturity, or Delta feature coverage matter. Microsoft’s kernel guide explicitly calls out Spark for full Delta compatibility, production features, live monitoring, Microsoft support, and scaling from single-node to multi-node without rewriting code.

Silver is the Spark-first layer under F64. Joins, dedupes, CDC, merges, and skew are distributed-system problems; F64 gives Spark enough cores and memory to make distributed execution the practical default around the 10–25GB compressed class, depending on complexity.

Bronze and Gold are where Polars still earns a place. Bronze simple batch ingestion and Gold small-to-medium aggregations can avoid Spark overhead, but streaming Bronze and governed/optimized Gold serving should generally stay Spark.

Treat all boundaries as benchmark gates, not laws. At every gray-zone boundary, run the same job in Polars and Spark with representative data, record wall-clock duration, CU consumption, memory peak/OOM behavior, and output table quality. The public Fabric benchmark evidence is strong directionally, but your file sizes, cardinality, skew, Delta features, and SLA shape will determine the exact cutoff.