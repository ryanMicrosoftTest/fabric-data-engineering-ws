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
| [4. Environments & compute parity](#4-environments--compute-parity-the-fairness-contract) | The fairness contract: equal worker vCores per layer + Spark tuning |
| [5. Benchmark matrix](#5-benchmark-matrix) | The 18 timed runs |
| [6. How to run](#6-how-to-run) | `init_pipeline` setup + `bench_full_run` matrix |
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
| `02_polars_vcore_advisor` | Notebook (Python) | `f6c0ed2c-1791-4e43-b42a-ab68d9763d02` | Recommend Polars vCores for a `(size, layer)` |
| `10_polars_bronze` | Notebook (Python) | `92d95b03-ef03-4f21-90b0-8cffbd45c718` | Polars bronze ingest |
| `11_polars_silver` | Notebook (Python) | `56f7208b-8521-4d38-b6ad-21030742bc0e` | Polars silver join/dedupe |
| `12_polars_gold` | Notebook (Python) | `d9215f86-3a5f-449c-a0b3-8d246b8ff4c1` | Polars gold aggregates/windows |
| `20_pyspark_bronze` | Notebook (Spark) | `7ee66ff5-b317-47bb-886d-d1bd828b0b87` | PySpark bronze ingest |
| `21_pyspark_silver` | Notebook (Spark) | `13237c5e-79c0-45ce-944c-788d117ce1bb` | PySpark silver join/dedupe |
| `22_pyspark_gold` | Notebook (Spark) | `ec355c6a-4a2b-440c-9e10-c0a4915f9adc` | PySpark gold aggregates/windows |
| `90_results_report` | Notebook (Python) | `543aefe4-19fe-4ba0-be60-8c49de544ee7` | Consolidate JSON → `bench.results`, print comparisons |
| `init_pipeline` | Data Pipeline | `836f0cc1-f7b5-4c93-82bd-37aee6bf797e` | One‑time setup: generate all sizes → calibrate |
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

Every benchmark notebook is built from the same source and shares auto‑injected
configuration and metrics cells at the top. The three Polars layer notebooks
also install and verify the pinned runtime before importing Polars:

- **Polars runtime cells** — pin Polars 1.42.1 and delta‑rs 1.6.2 instead of
  Fabric's bundled Polars 1.6.0 legacy streaming engine. Fabric's already-loaded
  PyArrow runtime remains in place.
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
| `00_generate_tpcds` | Python | DuckDB `dsdgen(sf=…)` → ZSTD Parquet into `lh_bench_source`. Generates **one size per run** via the `size` parameter (pipeline‑injectable). |
| `01_calibrate_sizes` | Python | Walks each `tpcds/<size>` folder, reports actual compressed GB and the **suggested scale factor** to hit the 1/10/50 GB targets. |
| `02_polars_vcore_advisor` | Python | Inputs `size` + `layer`; measures that layer's on-disk input in OneLake and applies a **streaming** memory model — peak is driven by the largest blocking op on the **column-pruned** dominant input (not the sum of inputs), then decompress × op-factor × headroom + baseline. Falls back to a **source proxy** (silver→source, gold→source `store_sales`) when the true upstream isn't produced yet. Recommends vCores from the 4/8/16/32/64 ladder and states the value applies to **both engines** (Polars `CONFIGURE_VCORES` + PySpark `deploy_environments.py`). Emits the number via `notebook.exit`. |
| `10_polars_bronze` | Python | `pl.scan_parquet(...)` + ingest metadata → native **`sink_delta`** (streaming, no `collect()`). Narrow, scan‑heavy → Polars' sweet spot. **vCores 16.** |
| `11_polars_silver` | Python | Join fact→dims, dedupe on `(ss_ticket_number, ss_item_sk)`, derive amounts → native **`sink_delta`** (streaming). Shuffle‑heavy → where Spark starts to win. **vCores 32.** |
| `12_polars_gold` | Python | Three marts: sales‑by‑date/store/category, monthly running revenue (window), top‑20 items per category (rank) → native **`sink_delta`**. **vCores 16.** |
| `20_pyspark_bronze` | Spark | Same bronze logic in Spark; env `pyspark-bronze`. |
| `21_pyspark_silver` | Spark | Same silver logic (row‑number dedupe); env `pyspark-silver`. |
| `22_pyspark_gold` | Spark | Same three marts; env `pyspark-gold`. |
| `90_results_report` | Python | Loads every JSON result, writes consolidated `bench.results` Delta, prints engine comparison + layer‑separation view. |

> **Polars vCores are pinned automatically.** Each Polars layer notebook's first
> cell is `%%configure -f {"vCores": N}` (16/32/16), which sets the single‑node
> size in **both interactive and pipeline runs** — no manual *Settings → Compute*
> step, and no way to accidentally run Polars on the 2‑vCore default. `VCORES` in
> the parameters cell mirrors this and is recorded in results. The following
> bootstrap cells pin and verify the same Polars/delta‑rs versions for every
> layer run.

---

## 4. Environments & compute parity (the fairness contract)

The whole point of the benchmark is a **fair** fight, so each layer gives Polars
and PySpark the **same number of worker vCores**:

| Layer | Polars (single node, `%%configure`) | PySpark workers (env) | Equal? |
|-------|-------------------------------------|-----------------------|--------|
| Bronze | 16 vCores / 128 GB | 2 executors × 8 = **16** | ✅ |
| Silver | 32 vCores / 256 GB | 4 executors × 8 = **32** | ✅ |
| Gold | 16 vCores / 128 GB | 2 executors × 8 = **16** | ✅ |

**How parity is enforced**
- **Polars** is single‑node, so its budget = the `%%configure` vCores.
- **PySpark** executor count is *pinned* (dynamic allocation `min == max`) so it
  cannot burst past the budget: worker vCores = executors × 8.

**The one unavoidable asymmetry — disclosed:** PySpark also needs a **driver**
(coordinator) that Polars has no equivalent for. It's shrunk to the smallest node
(**4 cores / 28 GB**) and reported separately. It *handicaps* Spark (extra cores
Polars doesn't spend), so the bias is always in the safe direction — never a
hidden advantage for Spark. Per‑run CU (below) counts driver + executor cores, so
the overhead is fully visible and quantified.

Polars runs on the plain Python kernel (no Environment item). The three published
PySpark environments (runtime **1.3**, executor **8 cores / 56 GB**, driver
**4 cores / 28 GB**) keep the compute *chassis* identical and vary only the
layer‑appropriate Spark SQL tuning:

| Environment | Pinned executors | Shuffle partitions | Key flags |
|-------------|------------------|--------------------|-----------|
| `pyspark-bronze` | 2 (=16 vCores) | 128 | `maxPartitionBytes=256m`, Native Execution Engine on |
| `pyspark-silver` | 4 (=32 vCores) | 400 | AQE + skew‑join + coalesce on, `maxPartitionBytes=128m` |
| `pyspark-gold` | 2 (=16 vCores) | 256 | AQE + coalesce on, `maxPartitionBytes=128m` |

> To change the budget or run a vCore sweep, edit `CONFIGURE_VCORES` in
> `deploy_benchmark_notebooks.py` (Polars) **and** the `executors` counts in
> `deploy_environments.py` (PySpark) so they stay equal, then re‑run both scripts.

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
Run the **`init_pipeline`** Data Pipeline (**Run** on the canvas). It generates the
TPC‑DS source for **all three sizes sequentially, then calibrates**:

```
generate_1gb → generate_10gb → generate_50gb → calibrate
```

Each `generate_<size>` activity runs `00_generate_tpcds` with `size` injected,
writing ZSTD Parquet to `lh_bench_source/Files/tpcds/<size>/…`; `calibrate` runs
`01_calibrate_sizes` once to report actual compressed GB vs the 1/10/50 GB targets.
SF400 (~50 GB) is heavy single‑node — expect a long run.

- To generate only a subset (e.g. a 1 GB smoke test), open **`00_generate_tpcds`**
  manually, set `size`, and run — then run **`01_calibrate_sizes`**.
- If a size is off target, adjust `scale_factors` in `00_generate_tpcds` and
  regenerate that size.

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

> **Run `init_pipeline` first.** `bench_full_run` starts at bronze and does **not**
> call `00_generate_tpcds`; its bronze stages will fail if the source data for a
> size hasn't been generated.

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
| `capacity_id`, `vcores` | Capacity + **worker vCore budget** (recorded for both engines; equal per layer) |
| `driver_cores` | Extra coordinator cores: **4 for PySpark, 0 for Polars** |
| `total_cores` | `vcores + driver_cores` — the cores billed for CU (20/36/20 Spark; 16/32/16 Polars) |
| `cu_seconds` | **Auto‑computed CU‑seconds** for the stage (`total_cores × 0.5 × wall_seconds`) |
| `start_utc`, `end_utc` | Run window (also usable with Capacity Metrics) |

Run **`90_results_report`** to consolidate everything into the Delta table
**`bench.results`** (in `lh_bench_results`) and print the comparisons — including
a **CU‑seconds by engine** pivot alongside wall‑clock.

**CU attribution — computed and persisted automatically.** Fabric bills notebook
compute with a simple formula (startup time is *not* charged); `run_stage` applies
it per stage and writes `cu_seconds` into every result row:

```
cu_seconds = total_cores × 0.5 × active_seconds
```

- **Polars:** `total_cores = vcores` (16/32/16).
- **PySpark:** `total_cores = 4 (driver) + worker_vcores` (so 20/36/20) — the
  disclosed driver overhead is included here, keeping the comparison honest.

Because worker vCores are equal per layer, the CU comparison reduces almost
entirely to **active duration** (plus Spark's fixed 4‑core driver tax). You can
still cross‑check the stored totals against the **Fabric Capacity Metrics** app
using the recorded `capacity_id` + `start_utc..end_utc` window (its data lags, so
backfill later).

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
| [`deploy_environments.py`](deploy_environments.py) | The 3 PySpark environments with vCore‑matched compute (pinned executors) + layer Spark tuning |
| [`deploy_benchmark_notebooks.py`](deploy_benchmark_notebooks.py) | All 9 notebooks (create/update via REST, ipynb format) + human‑readable `notebooks/*.py` copies |
| [`deploy_init_pipeline.py`](deploy_init_pipeline.py) | The `init_pipeline` Data Pipeline (generate 1gb/10gb/50gb → calibrate) |
| [`deploy_pipeline.py`](deploy_pipeline.py) | The `bench_full_run` Data Pipeline (19 sequential activities) |

```powershell
cd polars
python deploy_environments.py          # spark environments (parity compute)
python deploy_benchmark_notebooks.py   # notebooks
python deploy_init_pipeline.py         # setup pipeline (data generation)
python deploy_pipeline.py              # benchmark pipeline
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

- **The results handoff tolerates OneLake metadata propagation.** Every stage
  ensures `Files/bench_results` exists before writing its JSON row, and
  `90_results_report` retries the directory listing for up to 50 seconds before
  failing. This prevents a successful matrix from being reported as a failed
  pipeline because the final notebook briefly cannot see the results directory.
- **Do not use Fabric's bundled Polars 1.6.0 for Silver.** Its legacy streaming
  `unique → joins → sink_parquet` graph can panic with a Rust `SendError`. The
  layer notebooks pin Polars 1.42.1 and use native `sink_delta`; the bootstrap
  version check fails early rather than silently falling back to 1.6.0.
- **OneLake storage tokens live ~1 hour.** The Polars write token
  (`getToken("storage")`) is fetched once per stage; a single stage that runs
  longer than ~60 min (possible for 50 GB silver on 32 vCores) can lose access
  mid‑write. If a long run fails late with an auth error, that's the cause —
  re‑fetch the token inside the write or split the work.
- **SF400 (~50 GB) generation is heavy on a single node.** Generate it only when
  you're ready to benchmark that size.
- **Compute parity is enforced, with one disclosed exception.** Worker vCores are
  equal per layer (16/32/16) for both engines — Polars via `%%configure`, Spark
  via pinned executors. The only asymmetry is Spark's required **4‑core driver**,
  which is counted in the CU formula (§7) and handicaps Spark, never Polars.
  Other differences (distributed shuffle vs single‑node streaming; driver‑only
  RSS sampling for Spark) are documented so results are read correctly.
- **A true Polars OOM kills the kernel (SIGKILL ‑9) and is NOT recorded.** The
  `run_stage` harness catches Python `MemoryError`, but an out‑of‑memory Polars
  process is killed by the OS before any exception is raised — so no `oom` result
  row is written and the pipeline activity hard‑fails (breaking the chain). Treat
  a failed Polars activity with kernel exit `‑9` / "Kernel died" as an OOM result.
  (A subprocess‑isolated harness would be needed to capture these as `oom` rows.)
- **Rust panics are recorded separately from OOMs.** `run_stage` explicitly
  catches Polars/PyO3 `PanicException` without broadly swallowing
  `KeyboardInterrupt`, `SystemExit`, or other `BaseException` types.
- **Polars writes every layer with a streaming `sink_delta`, never `collect()`.**
  A headless ETL pipeline never needs the frame in RAM; streaming `collect()`
  streams *intermediates* but still re‑materializes the **entire result** on the
  final step, so a passthrough/join output is held whole in memory. Bronze (the
  widest output — all 23 `store_sales` columns) OOM'd this way at 50 GB (~125 GB
  peak, killed at the 16‑vCore / 128 GB ceiling). `sink_delta` streams
  scan→transform→write so peak memory is **O(batch)**, not O(table). This is the
  fair counterpart to Spark's inherently spilling, out‑of‑core writes — a naive
  `collect()` was actually an artificial handicap on Polars. Silver still prunes
  `store_sales` to the 8 columns it needs (Catalyst does this for Spark
  automatically).
- **Runtime:** Fabric Spark runtime **1.3** driver is Java 11.
