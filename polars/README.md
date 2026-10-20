# Polars vs PySpark — Fabric Medallion Benchmark

A reproducible Microsoft Fabric demo that benchmarks **Polars (single‑node Python
kernel)** against **PySpark** across a **bronze → silver → gold medallion
pipeline** at three *compressed* data sizes (**1 / 10 / 50 GB**). The goal is to
surface **where Polars wins and where it breaks down** on an F64 capacity — for
learning and for customer conversations. Methodology mirrors Miles Cole's Fabric
TPC‑DS benchmarks.

- **Capacity:** `uswest3capacity` (F64)
- **Tenant:** `35acf02c-4b87-4ae6-9221-ff5cafd430b4` (MngEnvMCAP372892)
- **Dataset:** TPC‑DS (DuckDB `tpcds` generator), one fact + an 8‑table dimension star
- **Write format:** Delta for *both* engines (Polars via `deltalake`/delta‑rs)
- **Scope:** All items are provisioned; **you run the benchmarks.** The notebooks
  here define the workload — nothing is timed automatically at deploy time.

> **Outputs are isolated per engine + size** using the schema naming
> `<engine>_<size>` (e.g. `polars_10gb.*`, `pyspark_10gb.*`), so runs never
> collide and can be compared side‑by‑side.

---

## Documentation index

| Section | Contents |
|---------|----------|
| [1. Item inventory](#1-item-inventory) | Workspaces, lakehouses, environments, notebooks, pipeline (with IDs) |
| [2. Architecture](#2-architecture) | Medallion layout, default‑lakehouse design, read/write paths |
| [3. Notebook catalog](#3-notebook-catalog) | What each of the 9 notebooks does |
| [4. Environments](#4-environments-pyspark-only) | Per‑layer Spark tuning for PySpark |
| [5. Benchmark matrix](#5-benchmark-matrix) | The 18 timed runs |
| [6. How to run](#6-how-to-run) | Manual run + `bench_full_run` pipeline |
| [7. Reading results](#7-reading-results) | `bench.results` schema, report notebook, CU attribution |
| [8. Deliverable analyses](#8-deliverable-analyses) | Polars‑vs‑PySpark + layer‑separation evaluation |
| [9. Deploying / editing](#9-deploying--editing) | Regenerating the items from source |
| [10. Known risks & notes](#10-known-risks--notes) | OneLake write, SF400 cost, fairness |

---

## 1. Item inventory

Two workspaces: one holds **data**, one holds **code + benchmarking**.

### `polars-benchmark-data-ws` — data
`f1e17fe0-3706-425e-ae65-9fda78946327`

| Item | Type | ID | Role |
|------|------|----|------|
| `lh_bench_source` | Lakehouse (schema) | `e219f8d3-f681-4a46-8cd0-855ab2b9e650` | Raw TPC‑DS ZSTD Parquet at 3 sizes (`Files/tpcds/<size>/<table>`) |
| `lh_bench_bronze` | Lakehouse (schema) | `52711a64-ef06-4608-b3b4-1330984d3c98` | Bronze Delta, schema `<engine>_<size>` |
| `lh_bench_silver` | Lakehouse (schema) | `b7280cc4-8842-443b-b9f3-2d643ff07cf3` | Silver `sales_conformed`, schema `<engine>_<size>` |
| `lh_bench_gold` | Lakehouse (schema) | `42643c02-ba40-4f37-ba7c-4ca0d68a3e4a` | Gold marts, schema `<engine>_<size>` |

### `polars-benchmark-engineering-ws` — code + benchmarking
`6dc1aae2-1e5d-4ebb-82b9-faf1a63ed5d8`

| Item | Type | ID | Role |
|------|------|----|------|
| `lh_bench_results` | Lakehouse | `44150c61-a3db-4e45-9b2b-4bdfc58a00b3` | Per‑stage JSON results + consolidated `bench.results` Delta |
| `pyspark-bronze` | Environment | `4944670a-141e-4385-bed7-9e473232ac60` | Spark tuning for the bronze layer |
| `pyspark-silver` | Environment | `ecb51557-d241-4172-8b4a-8cf820bea110` | Spark tuning for the silver layer |
| `pyspark-gold` | Environment | `15fa38d3-bcfb-4fc9-b188-ca8408f986eb` | Spark tuning for the gold layer |
| `00_generate_tpcds` | Notebook (Python) | `f4e9bc23-8ba9-4483-b352-599dca32fb29` | Generate TPC‑DS source |
| `01_calibrate_sizes` | Notebook (Python) | `20558adb-c1d8-4581-86c4-97390b8e2c7d` | Measure compressed size, suggest scale factors |
| `10_polars_bronze` | Notebook (Python) | `92d95b03-ef03-4f21-90b0-8cffbd45c718` | Polars bronze ingest |
| `11_polars_silver` | Notebook (Python) | `56f7208b-8521-4d38-b6ad-21030742bc0e` | Polars silver join/dedupe |
| `12_polars_gold` | Notebook (Python) | `d9215f86-3a5f-449c-a0b3-8d246b8ff4c1` | Polars gold aggregates/windows |
| `20_pyspark_bronze` | Notebook (Spark) | `7ee66ff5-b317-47bb-886d-d1bd828b0b87` | PySpark bronze ingest |
| `21_pyspark_silver` | Notebook (Spark) | `13237c5e-79c0-45ce-944c-788d117ce1bb` | PySpark silver join/dedupe |
| `22_pyspark_gold` | Notebook (Spark) | `ec355c6a-4a2b-440c-9e10-c0a4915f9adc` | PySpark gold aggregates/windows |
| `90_results_report` | Notebook (Python) | `543aefe4-19fe-4ba0-be60-8c49de544ee7` | Consolidate JSON → `bench.results`, print comparisons |
| `bench_full_run` | Data Pipeline | `e7696c43-9d17-4441-a757-38e2692a01f2` | Sequentially runs the whole matrix |

---

## 2. Architecture

**Medallion flow (per engine, per size):**

```
lh_bench_source (Parquet)
   → bronze  (ingest + metadata)        → lh_bench_bronze.<engine>_<size>.*
   → silver  (join fact+dims, dedupe)   → lh_bench_silver.<engine>_<size>.sales_conformed
   → gold    (aggregates, windows)      → lh_bench_gold.<engine>_<size>.*
```

**Default‑lakehouse design (key to fair, fast reads).** Each layer notebook's
*default lakehouse* is set to the lakehouse it **reads**, so reads use the fast,
reliable local mount `/lakehouse/default/...` on both kernels:

| Notebook layer | Default lakehouse (reads) | Writes to |
|----------------|---------------------------|-----------|
| bronze | `lh_bench_source` | `lh_bench_bronze` |
| silver | `lh_bench_bronze` | `lh_bench_silver` |
| gold | `lh_bench_silver` | `lh_bench_gold` |

**Writes go cross‑workspace via `abfss://`** (Spark) / **delta‑rs
`storage_options`** (Polars) to the target layer's lakehouse:

```
abfss://<data_ws>@onelake.dfs.fabric.microsoft.com/<lakehouseId>/Tables/<schema>/<table>
```

Polars authenticates the OneLake write with a storage bearer token:

```python
{"bearer_token": notebookutils.credentials.getToken("storage"),
 "use_fabric_endpoint": "true"}
```

**TPC‑DS tables used:** fact `store_sales` + dims `date_dim`, `item`, `store`,
`customer`, `customer_address`, `customer_demographics`,
`household_demographics`, `promotion`.

---

## 3. Notebook catalog

Every benchmark notebook is built from the same source and shares two
auto‑injected cells at the top:

- **Config cell** — item IDs, `abfss()`/`tbl()`/`results_dir()` path helpers,
  `onelake_opts()`, the `FACT`/`DIMS`/`SIZES` constants.
- **Metrics cell** — `run_stage(engine, size, layer, stage, fn)` which times a
  stage, samples **peak RSS** in a background thread, classifies the outcome as
  `success` / `failed` / `oom` (**never raises** — an OOM is a *result*), and
  writes one JSON row to `lh_bench_results/Files/bench_results/`.

Then a **`parameters`‑tagged cell** (`engine`, `size`, and for Polars `VCORES`),
immediately followed by a derived `schema_out = f"{engine}_{size}"` cell so that
**pipeline / `notebookutils.run` parameter injection** (which inserts overrides
*after* the parameters cell) is picked up correctly.

| Notebook | Kernel | Work |
|----------|--------|------|
| `00_generate_tpcds` | Python | DuckDB `dsdgen(sf=…)` → ZSTD Parquet into `lh_bench_source`. Generates one size at a time (`sizes_to_generate`). |
| `01_calibrate_sizes` | Python | Walks each `tpcds/<size>` folder, reports actual compressed GB and the **suggested scale factor** to hit the 1/10/50 GB targets. |
| `10_polars_bronze` | Python | `pl.scan_parquet(...).collect(streaming=True)` + ingest metadata → Delta. Narrow, scan‑heavy → Polars' sweet spot. **vCores 16.** |
| `11_polars_silver` | Python | Join fact→dims, dedupe on `(ss_ticket_number, ss_item_sk)`, derive amounts. Shuffle‑heavy → where Spark starts to win. **vCores 32.** |
| `12_polars_gold` | Python | Three marts: sales‑by‑date/store/category, monthly running revenue (window), top‑20 items per category (rank). **vCores 16.** |
| `20_pyspark_bronze` | Spark | Same bronze logic in Spark; env `pyspark-bronze`. |
| `21_pyspark_silver` | Spark | Same silver logic (row‑number dedupe); env `pyspark-silver`. |
| `22_pyspark_gold` | Spark | Same three marts; env `pyspark-gold`. |
| `90_results_report` | Python | Loads every JSON result, writes consolidated `bench.results` Delta, prints engine comparison + layer‑separation view. |

> **Set Polars vCores in the notebook** *Settings → Compute* to match the
> `VCORES` value (or sweep 4/8/16/32/64). `VCORES` is only *recorded* in results
> for context; it does not change compute by itself.

---

## 4. Environments (PySpark only)

Polars runs on the plain Python kernel (no Environment item). The three
PySpark environments are layer‑tuned for F64 and **published**. Common:
runtime **1.3**, driver/executor **8 cores / 56 GB**.

| Environment | Autoscale | Shuffle partitions | Key flags |
|-------------|-----------|--------------------|-----------|
| `pyspark-bronze` | min 1 / max 4 | 128 | `maxPartitionBytes=256m`, Native Execution Engine on |
| `pyspark-silver` | min 2 / max 8 | 400 | AQE + skew‑join on |
| `pyspark-gold` | min 1 / max 6 | 256 | AQE on (coalesce) |

---

## 5. Benchmark matrix

**2 engines × 3 sizes × 3 layers = 18 timed runs** (plus generation/calibration).
Each `run_stage` call appends one JSON row; `90_results_report` consolidates them.

| | 1 GB | 10 GB | 50 GB |
|--|------|-------|-------|
| **Polars** | bronze/silver/gold | bronze/silver/gold | bronze/silver/gold |
| **PySpark** | bronze/silver/gold | bronze/silver/gold | bronze/silver/gold |

> Polars at 50 GB is *expected* to OOM below higher vCore counts. Capturing that
> failure cleanly is a **first‑class result**, not an error to hide.

---

## 6. How to run

### One‑time data prep
1. Open **`00_generate_tpcds`** (default lakehouse `lh_bench_source`). Set
   `sizes_to_generate` (start with `["1gb"]`) and run.
2. Run **`01_calibrate_sizes`**; adjust `scale_factors` in `00` until the
   reported compressed size matches 1/10/50 GB, regenerate as needed. SF400
   (~50 GB) is heavy single‑node — generate it only when ready.

### Manual per‑run (recommended for careful measurement)
For each engine/size/layer: open the notebook, set `size` (and Polars `VCORES` +
*Settings → Compute*), run it. Results land as JSON automatically. **Run layers
in order** (bronze → silver → gold) because each reads the previous layer.

### Automated — `bench_full_run` pipeline
The **`bench_full_run`** Data Pipeline runs the whole matrix **sequentially**
(never in parallel — parallel runs would contend for capacity and corrupt the
numbers). Order: for each size `1gb → 10gb → 50gb`, Polars bronze→silver→gold
then PySpark bronze→silver→gold; finally `results_report`. Each activity passes
`size`. Trigger it from the pipeline canvas (**Run**) once the source data for the
sizes you want exists.

> **Before running 10 GB / 50 GB via the pipeline**, make sure that source size
> is generated. The pipeline does not call `00_generate_tpcds`.

---

## 7. Reading results

Each stage writes a JSON file to
`lh_bench_results/Files/bench_results/` with:

| Field | Meaning |
|-------|---------|
| `run_id` | Per‑notebook‑execution id (groups a stage's calls) |
| `engine`, `size`, `layer`, `stage` | The cell of the matrix + the specific transform |
| `status` | `success` / `failed` / `oom` |
| `wall_seconds` | Wall‑clock for the stage |
| `peak_mem_mb` | Peak process RSS (Polars: real; **Spark: driver only** — executor pressure shows in CU / Spark UI) |
| `rows` | Output rows (Polars). Spark bronze returns `None` to avoid an extra count scan for timing fairness |
| `error` | Truncated exception text on failure/oom |
| `capacity_id`, `vcores` | Capacity + Polars vCore context |
| `start_utc`, `end_utc` | Run window — join to Capacity Metrics for CU |

Run **`90_results_report`** to consolidate everything into the Delta table
**`bench.results`** (in `lh_bench_results`) and print the comparisons.

**CU attribution.** There is no clean public REST for per‑run CU. The report
prints each run's `capacity_id` + `start_utc..end_utc` window; join those to the
**Microsoft Fabric Capacity Metrics** app to attribute CU(s). Capacity Metrics
data lags, so backfill CU after a delay.

---

## 8. Deliverable analyses

1. **Polars vs PySpark** — wall‑clock / peak memory / CU / OOM per layer per size.
   Expect Polars to win low‑shuffle **bronze** and small **gold**, and to lose
   shuffle‑heavy **silver** and the largest sizes; `90_results_report` pivots
   wall‑clock by `size × layer × engine` to surface the crossover boundary
   (aligns with [`research-fabric-specific.md`](research-fabric-specific.md)).
2. **Layer‑separation evaluation** — does splitting Spark environments by *layer*
   (`pyspark-bronze/silver/gold`) beat splitting only by *technology* (one Spark
   config)? The report groups engine deltas by layer: if Polars wins bronze/gold
   but loses silver, **per‑layer** technology choice pays off; if one engine
   dominates every layer at a size, per‑technology separation is enough there.

---

## 9. Deploying / editing

Everything is generated from source scripts — **edit the scripts, not the live
items or the `notebooks/*.py` copies.**

| Script | Produces |
|--------|----------|
| [`deploy_benchmark_notebooks.py`](deploy_benchmark_notebooks.py) | All 9 notebooks (create/update via REST, ipynb format) + human‑readable `notebooks/*.py` copies |
| [`deploy_pipeline.py`](deploy_pipeline.py) | The `bench_full_run` Data Pipeline (19 sequential activities) |

```powershell
cd polars
python deploy_benchmark_notebooks.py   # notebooks
python deploy_pipeline.py              # pipeline
```

Both authenticate to the F64 tenant with:

```powershell
az account get-access-token --tenant 35acf02c-4b87-4ae6-9221-ff5cafd430b4 `
  --resource https://api.fabric.microsoft.com
```

> The F64 capacity lives in tenant `35acf02c` (MngEnvMCAP372892), **not** the
> corp tenant. All Fabric REST calls for this project must use that tenant's
> token. The scripts do this via `az` automatically; run `az login --tenant
> 35acf02c-4b87-4ae6-9221-ff5cafd430b4` first if your CLI is pointed elsewhere.

Notebook kernel is chosen by metadata: `jupyter` (Polars/Python) vs
`synapse_pyspark` (Spark). Default‑lakehouse and environment bindings are set in
the ipynb `metadata.dependencies`.

---

## 10. Known risks & notes

- **Polars → OneLake delta‑rs write is the highest‑risk path.** The
  `bearer_token` + `use_fabric_endpoint` `storage_options` pattern is the
  documented approach but is **unverified at runtime** here — it is the first
  thing to check on the initial Polars run.
- **SF400 (~50 GB) generation is heavy on a single node.** Generate it only when
  you're ready to benchmark that size.
- **Fairness:** both engines read the same source, write Delta, and run the same
  logical transforms. Unavoidable differences (Spark's distributed shuffle vs
  Polars' single‑node streaming; driver‑only memory sampling for Spark) are
  documented above so results are interpreted correctly.
- **Runtime:** Fabric Spark runtime **1.3** driver is Java 11.
