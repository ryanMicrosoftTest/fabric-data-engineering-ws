#!/usr/bin/env python3
"""
Deploy the Polars-vs-PySpark medallion benchmark notebooks into Fabric.

Single source of truth for the 9 benchmark notebooks. Running this script:
  1. Writes a human-readable percent-format copy of each notebook to
     polars/notebooks/<name>.py
  2. Creates (or updates) the live Notebook item in the
     polars-benchmark-engineering-ws workspace via the Fabric REST API
     (ipynb definition format).

Auth: uses `az account get-access-token` for the MngEnvMCAP372892 tenant
(35acf02c-...), which is where capacity `uswest3capacity` (F64) and both
workspaces live. Run `az login --tenant 35acf02c-4b87-4ae6-9221-ff5cafd430b4`
first.

The benchmarks themselves are NOT run by this script — the user runs them.
"""
import base64
import json
import os
import subprocess
import sys
import time
import urllib.request
import urllib.error

TENANT = "35acf02c-4b87-4ae6-9221-ff5cafd430b4"
FABRIC = "https://api.fabric.microsoft.com"

# ---- Provisioned item IDs (live) ------------------------------------------
DATA_WS = "f1e17fe0-3706-425e-ae65-9fda78946327"   # polars-benchmark-data-ws
ENG_WS = "6dc1aae2-1e5d-4ebb-82b9-faf1a63ed5d8"    # polars-benchmark-engineering-ws
LH_SOURCE = "e219f8d3-f681-4a46-8cd0-855ab2b9e650"
LH_BRONZE = "52711a64-ef06-4608-b3b4-1330984d3c98"
LH_SILVER = "b7280cc4-8842-443b-b9f3-2d643ff07cf3"
LH_GOLD = "42643c02-ba40-4f37-ba7c-4ca0d68a3e4a"
LH_RESULTS = "44150c61-a3db-4e45-9b2b-4bdfc58a00b3"
CAPACITY_ID = "ad343e36-f335-4ba3-b261-b739f7e950b0"
ENV_BRONZE = "4944670a-141e-4385-bed7-9e473232ac60"
ENV_SILVER = "ecb51557-d241-4172-8b4a-8cf820bea110"
ENV_GOLD = "15fa38d3-bcfb-4fc9-b188-ca8408f986eb"

DEFAULT_LH = {
    "source": (DATA_WS, LH_SOURCE, "lh_bench_source"),
    "bronze": (DATA_WS, LH_BRONZE, "lh_bench_bronze"),
    "silver": (DATA_WS, LH_SILVER, "lh_bench_silver"),
    "gold": (DATA_WS, LH_GOLD, "lh_bench_gold"),
    "results": (ENG_WS, LH_RESULTS, "lh_bench_results"),
}

# ===========================================================================
# Shared cells (injected into every notebook). Plain Python — safe on both the
# Python (Polars) kernel and the Spark driver.
# ===========================================================================
CELL_CONFIG = '''\
# --- Shared benchmark configuration (auto-injected) ---
DATA_WS   = "f1e17fe0-3706-425e-ae65-9fda78946327"
ENG_WS    = "6dc1aae2-1e5d-4ebb-82b9-faf1a63ed5d8"
LH = {
    "source": "e219f8d3-f681-4a46-8cd0-855ab2b9e650",
    "bronze": "52711a64-ef06-4608-b3b4-1330984d3c98",
    "silver": "b7280cc4-8842-443b-b9f3-2d643ff07cf3",
    "gold":   "42643c02-ba40-4f37-ba7c-4ca0d68a3e4a",
}
RESULTS_LH  = "44150c61-a3db-4e45-9b2b-4bdfc58a00b3"
CAPACITY_ID = "ad343e36-f335-4ba3-b261-b739f7e950b0"
ONELAKE     = "onelake.dfs.fabric.microsoft.com"

def abfss(ws, item, sub):
    return f"abfss://{ws}@{ONELAKE}/{item}/{sub}"

def tbl(layer, schema, table):
    """abfss path to a Delta table in one of the data-workspace lakehouses."""
    return abfss(DATA_WS, LH[layer], f"Tables/{schema}/{table}")

def results_dir():
    return abfss(ENG_WS, RESULTS_LH, "Files/bench_results")

def onelake_opts():
    """delta-rs / object_store options for cross-workspace OneLake writes."""
    import notebookutils
    return {"bearer_token": notebookutils.credentials.getToken("storage"),
            "use_fabric_endpoint": "true"}

# TPC-DS subset used by the benchmark (one big fact + a realistic star of dims)
FACT = "store_sales"
DIMS = ["date_dim", "item", "store", "customer", "customer_address",
        "customer_demographics", "household_demographics", "promotion"]
SIZES = ["1gb", "10gb", "50gb"]
'''

CELL_METRICS = '''\
# --- Metrics harness (auto-injected): wall-clock + peak memory + OOM + CU ctx ---
import time, json, threading, datetime, os, uuid
try:
    import psutil
except Exception:
    psutil = None

def _uid():
    return datetime.datetime.utcnow().strftime("%Y%m%d%H%M%S") + "-" + uuid.uuid4().hex[:6]

RUN_ID = _uid()
print("RUN_ID =", RUN_ID)

class _PeakMem:
    """Samples process (+child) RSS in a background thread to find the peak.

    On Spark this only sees the driver; executor pressure shows up in CU and
    the Spark UI, which is why we also record capacity + time window below."""
    def __init__(self):
        self.peak = 0; self._run = False; self._t = None
    def _loop(self):
        p = psutil.Process(os.getpid()) if psutil else None
        while self._run and p is not None:
            try:
                rss = p.memory_info().rss
                for c in p.children(recursive=True):
                    try: rss += c.memory_info().rss
                    except Exception: pass
                self.peak = max(self.peak, rss)
            except Exception:
                pass
            time.sleep(0.25)
    def __enter__(self):
        if psutil is not None:
            self._run = True
            self._t = threading.Thread(target=self._loop, daemon=True); self._t.start()
        return self
    def __exit__(self, *a):
        self._run = False
        if self._t is not None:
            self._t.join(timeout=1)

def _write_result(row):
    """Append one JSON result file to lh_bench_results/Files/bench_results.
    JSON files avoid cross-engine Delta schema conflicts."""
    import notebookutils
    fn = f"{row['run_id']}_{row['engine']}_{row['size']}_{row['layer']}_{row['stage']}.json"
    fn = fn.replace(":", "-").replace("/", "-")
    notebookutils.fs.put(f"{results_dir()}/{fn}", json.dumps(row, default=str), True)

def run_stage(engine, size, layer, stage, fn, vcores=None):
    """Time `fn`, capture peak memory + status (success/failed/oom), persist a
    result row, and return the status string. Never raises."""
    start = datetime.datetime.utcnow()
    t0 = time.perf_counter(); status = "success"; err = None; rows = None
    with _PeakMem() as pm:
        try:
            rows = fn()
        except MemoryError as e:
            status, err = "oom", repr(e)
        except Exception as e:
            m = repr(e).lower()
            status = "oom" if ("outofmemory" in m or "out of memory" in m
                               or "memoryerror" in m or "oom" in m) else "failed"
            err = repr(e)
    wall = time.perf_counter() - t0
    end = datetime.datetime.utcnow()
    row = {
        "run_id": RUN_ID, "engine": engine, "size": size, "layer": layer,
        "stage": stage, "status": status, "wall_seconds": round(wall, 3),
        "peak_mem_mb": round(pm.peak / 1e6, 1) if pm.peak else None,
        "rows": int(rows) if rows is not None else None,
        "error": (str(err)[:900] if err else None),
        "capacity_id": CAPACITY_ID, "vcores": vcores,
        "start_utc": start.isoformat(), "end_utc": end.isoformat(),
        "recorded_utc": datetime.datetime.utcnow().isoformat(),
    }
    try:
        _write_result(row)
    except Exception as e:
        print("WARN: could not persist result:", repr(e))
    tag = "OK " if status == "success" else status.upper()
    print(f"[{tag}] {stage}: {wall:.2f}s  peak={row['peak_mem_mb']}MB  rows={row['rows']}")
    if err:
        print("      ->", str(err)[:300])
    return status
'''

# ===========================================================================
# Per-notebook transform cells
# ===========================================================================

NB_GENERATE = [
    ("md", "# 00 - Generate TPC-DS source data\n"
           "Generates the TPC-DS subset with DuckDB's built-in `tpcds` extension and "
           "writes ZSTD Parquet into `lh_bench_source/Files/tpcds/<size>/<table>`.\n\n"
           "**Default lakehouse must be `lh_bench_source`.** Scale factors are estimates "
           "(SF100 ~= 12.7 GB compressed per Miles Cole); calibrate with notebook 01. "
           "SF400 (~50 GB) is heavy on a single node — generate it only when ready."),
    ("code", '''\
# Parameters
scale_factors = {"1gb": 8, "10gb": 80, "50gb": 400}   # starting estimates -> calibrate
sizes_to_generate = ["1gb"]                            # add "10gb", "50gb" when ready
'''),
    ("code", '''\
import duckdb, os, time
TABLES = [FACT] + DIMS
out_root = "/lakehouse/default/Files/tpcds"   # default lakehouse = lh_bench_source

for size in sizes_to_generate:
    sf = scale_factors[size]
    print(f"=== generating {size} (sf={sf}) ===")
    con = duckdb.connect()
    con.execute("INSTALL tpcds; LOAD tpcds;")
    t0 = time.perf_counter()
    con.execute(f"CALL dsdgen(sf={sf})")
    print(f"  dsdgen done in {time.perf_counter()-t0:.1f}s; exporting {len(TABLES)} tables")
    for t in TABLES:
        dst = f"{out_root}/{size}/{t}"
        os.makedirs(dst, exist_ok=True)
        con.execute(
            f"COPY (SELECT * FROM {t}) TO '{dst}/{t}.parquet' "
            f"(FORMAT PARQUET, COMPRESSION ZSTD)")
    con.close()
    print(f"  {size} complete in {time.perf_counter()-t0:.1f}s")
'''),
]

NB_CALIBRATE = [
    ("md", "# 01 - Calibrate compressed sizes\n"
           "Measures the on-disk (ZSTD Parquet) size of each generated size folder and "
           "suggests the scale factor needed to hit the 1 / 10 / 50 GB **compressed** "
           "targets. Default lakehouse = `lh_bench_source`."),
    ("code", '''\
import os
root = "/lakehouse/default/Files/tpcds"
targets = {"1gb": 1.0, "10gb": 10.0, "50gb": 50.0}
sf_used = {"1gb": 8, "10gb": 80, "50gb": 400}   # keep in sync with notebook 00

def dir_gb(p):
    tot = 0
    for r, _, fs in os.walk(p):
        for f in fs:
            tot += os.path.getsize(os.path.join(r, f))
    return tot / 1e9

for size in ["1gb", "10gb", "50gb"]:
    p = f"{root}/{size}"
    if not os.path.exists(p):
        print(f"{size}: not generated yet"); continue
    gb = dir_gb(p)
    ratio = gb / sf_used[size]
    suggested = targets[size] / ratio if ratio else float("nan")
    print(f"{size}: actual={gb:.2f} GB compressed | sf_used={sf_used[size]} | "
          f"GB/sf={ratio:.3f} | suggested_sf_for_{targets[size]:.0f}GB={suggested:.1f}")
'''),
]

# ---- Polars notebooks -----------------------------------------------------
_POLARS_PARAMS = '''\
# Parameters -- change `size` per run; set the notebook's vCores in Settings > Compute.
engine = "polars"
size   = "1gb"          # one of: 1gb, 10gb, 50gb
VCORES = 16             # recorded for context; matches the vCores you set in Settings
'''

NB_POLARS_BRONZE = [
    ("md", "# 10 - Polars Bronze (ingest)\n"
           "Reads TPC-DS Parquet from the **default lakehouse (`lh_bench_source`)**, "
           "adds ingest metadata, and writes Delta to `lh_bench_bronze` under schema "
           "`polars_<size>`. Narrow, scan-heavy, low-shuffle work — Polars' sweet spot.\n\n"
           "**Recommended vCores (set in Settings > Compute): 16.**"),
    ("code", _POLARS_PARAMS),
    ("code", '''\
import polars as pl, datetime
src_base = f"/lakehouse/default/Files/tpcds/{size}"   # default lakehouse = source

def bronze_one(name):
    def _run():
        df = (pl.scan_parquet(f"{src_base}/{name}/*.parquet")
                .with_columns([
                    pl.lit(size).alias("_bench_size"),
                    pl.lit(engine).alias("_bench_engine"),
                    pl.lit(datetime.datetime.utcnow().isoformat()).alias("_bronze_ingest_ts"),
                ])
                .collect(streaming=True))
        df.write_delta(tbl("bronze", schema_out, name), mode="overwrite",
                       storage_options=onelake_opts())
        return df.height
    return _run

for t in [FACT] + DIMS:
    run_stage(engine, size, "bronze", f"bronze:{t}", bronze_one(t), vcores=VCORES)
'''),
]

NB_POLARS_SILVER = [
    ("md", "# 11 - Polars Silver (clean / join / dedupe)\n"
           "Reads Bronze Delta from the **default lakehouse (`lh_bench_bronze`)**, joins the "
           "fact to its dimensions, dedupes on the natural key, and writes "
           "`silver.sales_conformed`. Shuffle-heavy — where Spark starts to win.\n\n"
           "**Recommended vCores: 32.**"),
    ("code", _POLARS_PARAMS.replace('VCORES = 16', 'VCORES = 32')),
    ("code", '''\
import polars as pl
b = f"/lakehouse/default/Tables/{schema_out}"   # default lakehouse = bronze

def read_b(t, cols=None):
    df = pl.read_delta(f"{b}/{t}")
    return df.select(cols) if cols else df

def silver_conform():
    ss = read_b("store_sales")
    dd = read_b("date_dim", ["d_date_sk", "d_year", "d_moy", "d_date"])
    it = read_b("item", ["i_item_sk", "i_category", "i_brand", "i_current_price"])
    st = read_b("store", ["s_store_sk", "s_store_name", "s_state"])
    cu = read_b("customer", ["c_customer_sk", "c_current_addr_sk", "c_birth_year"])
    ca = read_b("customer_address", ["ca_address_sk", "ca_state", "ca_city"])
    df = (ss.join(dd, left_on="ss_sold_date_sk", right_on="d_date_sk", how="inner")
            .join(it, left_on="ss_item_sk", right_on="i_item_sk", how="inner")
            .join(st, left_on="ss_store_sk", right_on="s_store_sk", how="left")
            .join(cu, left_on="ss_customer_sk", right_on="c_customer_sk", how="left")
            .join(ca, left_on="c_current_addr_sk", right_on="ca_address_sk", how="left")
            .unique(subset=["ss_ticket_number", "ss_item_sk"], keep="first")
            .with_columns([
                (pl.col("ss_sales_price") * pl.col("ss_quantity")).alias("gross_amount"),
                pl.col("ss_net_paid").alias("net_amount"),
            ]))
    df.write_delta(tbl("silver", schema_out, "sales_conformed"), mode="overwrite",
                   storage_options=onelake_opts())
    return df.height

run_stage(engine, size, "silver", "silver:conform", silver_conform, vcores=VCORES)
'''),
]

NB_POLARS_GOLD = [
    ("md", "# 12 - Polars Gold (aggregates / windows)\n"
           "Reads `silver.sales_conformed` from the **default lakehouse (`lh_bench_silver`)** "
           "and builds curated marts (group-bys + window functions), written to "
           "`lh_bench_gold`. Curated output is usually much smaller than the scan.\n\n"
           "**Recommended vCores: 16.**"),
    ("code", _POLARS_PARAMS),
    ("code", '''\
import polars as pl
sc_path = f"/lakehouse/default/Tables/{schema_out}/sales_conformed"  # default = silver

def gold_sales_by_date_store_cat():
    sc = pl.read_delta(sc_path)
    g = (sc.group_by(["d_year", "d_moy", "s_store_sk", "i_category"])
           .agg([pl.col("net_amount").sum().alias("net_revenue"),
                 pl.len().alias("line_items"),
                 pl.col("net_amount").mean().alias("avg_line")]))
    g.write_delta(tbl("gold", schema_out, "sales_by_date_store_cat"),
                  mode="overwrite", storage_options=onelake_opts())
    return g.height

def gold_monthly_store_revenue():
    sc = pl.read_delta(sc_path)
    g = (sc.group_by(["s_store_sk", "d_year", "d_moy"])
           .agg(pl.col("net_amount").sum().alias("revenue"))
           .sort(["s_store_sk", "d_year", "d_moy"])
           .with_columns(pl.col("revenue").cum_sum().over("s_store_sk").alias("running_revenue")))
    g.write_delta(tbl("gold", schema_out, "monthly_store_revenue"),
                  mode="overwrite", storage_options=onelake_opts())
    return g.height

def gold_top_items():
    sc = pl.read_delta(sc_path)
    g = (sc.group_by(["i_category", "i_item_sk"])
           .agg(pl.col("net_amount").sum().alias("revenue"))
           .with_columns(pl.col("revenue").rank("dense", descending=True)
                           .over("i_category").alias("rank_in_cat"))
           .filter(pl.col("rank_in_cat") <= 20))
    g.write_delta(tbl("gold", schema_out, "top_items_per_category"),
                  mode="overwrite", storage_options=onelake_opts())
    return g.height

run_stage(engine, size, "gold", "gold:sales_by_date_store_cat", gold_sales_by_date_store_cat, vcores=VCORES)
run_stage(engine, size, "gold", "gold:monthly_store_revenue",  gold_monthly_store_revenue,  vcores=VCORES)
run_stage(engine, size, "gold", "gold:top_items_per_category",  gold_top_items,              vcores=VCORES)
'''),
]

# ---- PySpark notebooks ----------------------------------------------------
_SPARK_PARAMS = '''\
# Parameters -- change `size` per run. Spark compute comes from the attached
# environment (pyspark-<layer>); no need to set vCores manually.
engine = "pyspark"
size   = "1gb"          # one of: 1gb, 10gb, 50gb
'''

NB_PYSPARK_BRONZE = [
    ("md", "# 20 - PySpark Bronze (ingest)\n"
           "Reads TPC-DS Parquet from the **default lakehouse (`lh_bench_source`)**, adds "
           "ingest metadata, writes Delta to `lh_bench_bronze` (schema `pyspark_<size>`). "
           "Attached environment: **pyspark-bronze** (low-shuffle, NEE on)."),
    ("code", _SPARK_PARAMS),
    ("code", '''\
from pyspark.sql import functions as F

def bronze_one(name):
    def _run():
        df = (spark.read.parquet(f"Files/tpcds/{size}/{name}")   # default lakehouse = source
                .withColumn("_bench_size", F.lit(size))
                .withColumn("_bench_engine", F.lit(engine))
                .withColumn("_bronze_ingest_ts", F.current_timestamp()))
        (df.write.format("delta").mode("overwrite")
           .save(tbl("bronze", schema_out, name)))
        return None
    return _run

for t in [FACT] + DIMS:
    run_stage(engine, size, "bronze", f"bronze:{t}", bronze_one(t))
'''),
]

NB_PYSPARK_SILVER = [
    ("md", "# 21 - PySpark Silver (clean / join / dedupe)\n"
           "Joins the fact to its dimensions from **`lh_bench_bronze`** (default lakehouse), "
           "dedupes on the natural key, writes `silver.sales_conformed`. Attached "
           "environment: **pyspark-silver** (AQE + skew join, higher shuffle partitions)."),
    ("code", _SPARK_PARAMS),
    ("code", '''\
from pyspark.sql import functions as F, Window

def read_b(t):
    return spark.read.format("delta").load(tbl("bronze", schema_out, t))

def silver_conform():
    ss = read_b("store_sales")
    dd = read_b("date_dim").select("d_date_sk", "d_year", "d_moy", "d_date")
    it = read_b("item").select("i_item_sk", "i_category", "i_brand", "i_current_price")
    st = read_b("store").select("s_store_sk", "s_store_name", "s_state")
    cu = read_b("customer").select("c_customer_sk", "c_current_addr_sk", "c_birth_year")
    ca = read_b("customer_address").select("ca_address_sk", "ca_state", "ca_city")
    df = (ss.join(dd, ss.ss_sold_date_sk == dd.d_date_sk, "inner")
            .join(it, ss.ss_item_sk == it.i_item_sk, "inner")
            .join(st, ss.ss_store_sk == st.s_store_sk, "left")
            .join(cu, ss.ss_customer_sk == cu.c_customer_sk, "left")
            .join(ca, F.col("c_current_addr_sk") == ca.ca_address_sk, "left"))
    w = Window.partitionBy("ss_ticket_number", "ss_item_sk").orderBy(F.lit(1))
    df = (df.withColumn("_rn", F.row_number().over(w)).filter(F.col("_rn") == 1).drop("_rn")
            .withColumn("gross_amount", F.col("ss_sales_price") * F.col("ss_quantity"))
            .withColumn("net_amount", F.col("ss_net_paid")))
    (df.write.format("delta").mode("overwrite")
       .save(tbl("silver", schema_out, "sales_conformed")))
    return None

run_stage(engine, size, "silver", "silver:conform", silver_conform)
'''),
]

NB_PYSPARK_GOLD = [
    ("md", "# 22 - PySpark Gold (aggregates / windows)\n"
           "Reads `silver.sales_conformed` from **`lh_bench_silver`** (default lakehouse) and "
           "builds curated marts, written to `lh_bench_gold`. Attached environment: "
           "**pyspark-gold** (AQE, coalesce)."),
    ("code", _SPARK_PARAMS),
    ("code", '''\
from pyspark.sql import functions as F, Window

def _sc():
    return spark.read.format("delta").load(tbl("silver", schema_out, "sales_conformed"))

def gold_sales_by_date_store_cat():
    g = (_sc().groupBy("d_year", "d_moy", "s_store_sk", "i_category")
              .agg(F.sum("net_amount").alias("net_revenue"),
                   F.count(F.lit(1)).alias("line_items"),
                   F.avg("net_amount").alias("avg_line")))
    g.write.format("delta").mode("overwrite").save(tbl("gold", schema_out, "sales_by_date_store_cat"))
    return None

def gold_monthly_store_revenue():
    base = (_sc().groupBy("s_store_sk", "d_year", "d_moy")
                 .agg(F.sum("net_amount").alias("revenue")))
    w = Window.partitionBy("s_store_sk").orderBy("d_year", "d_moy")
    g = base.withColumn("running_revenue", F.sum("revenue").over(w))
    g.write.format("delta").mode("overwrite").save(tbl("gold", schema_out, "monthly_store_revenue"))
    return None

def gold_top_items():
    base = (_sc().groupBy("i_category", "i_item_sk")
                 .agg(F.sum("net_amount").alias("revenue")))
    w = Window.partitionBy("i_category").orderBy(F.col("revenue").desc())
    g = base.withColumn("rank_in_cat", F.dense_rank().over(w)).filter(F.col("rank_in_cat") <= 20)
    g.write.format("delta").mode("overwrite").save(tbl("gold", schema_out, "top_items_per_category"))
    return None

run_stage(engine, size, "gold", "gold:sales_by_date_store_cat", gold_sales_by_date_store_cat)
run_stage(engine, size, "gold", "gold:monthly_store_revenue",  gold_monthly_store_revenue)
run_stage(engine, size, "gold", "gold:top_items_per_category",  gold_top_items)
'''),
]

NB_RESULTS = [
    ("md", "# 90 - Results report\n"
           "Reads every per-stage JSON result from `lh_bench_results/Files/bench_results`, "
           "consolidates them into the Delta table `bench.results` (default lakehouse = "
           "`lh_bench_results`), and prints Polars-vs-PySpark comparisons plus the "
           "layer-separation view. Run after any batch of benchmark runs."),
    ("code", '''\
import notebookutils, json, polars as pl

infos = notebookutils.fs.ls("Files/bench_results")   # default lakehouse = results
rows = []
for f in infos:
    if f.name.endswith(".json"):
        try:
            rows.append(json.loads(notebookutils.fs.head(f.path, 1000000)))
        except Exception as e:
            print("skip", f.name, repr(e))
print(f"loaded {len(rows)} result rows")
df = pl.DataFrame(rows)
df.write_delta("Tables/bench/results", mode="overwrite")   # consolidated table
df.head()
'''),
    ("code", '''\
# Engine comparison: total wall-clock per size x layer
comp = (df.filter(pl.col("status") == "success")
          .group_by(["size", "layer", "engine"])
          .agg(pl.col("wall_seconds").sum().alias("wall_s"),
               pl.col("peak_mem_mb").max().alias("peak_mb")))
wide = comp.pivot(values="wall_s", index=["size", "layer"], columns="engine",
                  aggregate_function="sum").sort(["size", "layer"])
print("=== wall-clock seconds by engine ===")
print(wide)

# OOM / failures (decisive at 10-50 GB)
fails = df.filter(pl.col("status") != "success").select(
    ["engine", "size", "layer", "stage", "status", "error"])
print("=== non-success runs ===")
print(fails)
'''),
    ("code", '''\
# Layer-separation evaluation input: per-layer engine deltas.
# If Polars wins Bronze/Gold but loses Silver, per-LAYER technology choice pays
# off; if one engine dominates every layer at a given size, per-technology-only
# separation is sufficient there.
ev = (df.filter(pl.col("status") == "success")
        .group_by(["size", "layer", "engine"])
        .agg(pl.col("wall_seconds").sum().alias("wall_s"))
        .sort(["size", "layer", "engine"]))
print(ev)
print("\\nCU note: join these run windows (start_utc..end_utc, capacity_id) to the "
      "Microsoft Fabric Capacity Metrics app to attribute CU(s) per run.")
'''),
]

# ===========================================================================
# Notebook registry
# ===========================================================================
NOTEBOOKS = [
    ("00_generate_tpcds", "jupyter", "source", None, NB_GENERATE),
    ("01_calibrate_sizes", "jupyter", "source", None, NB_CALIBRATE),
    ("10_polars_bronze", "jupyter", "source", None, NB_POLARS_BRONZE),
    ("11_polars_silver", "jupyter", "bronze", None, NB_POLARS_SILVER),
    ("12_polars_gold", "jupyter", "silver", None, NB_POLARS_GOLD),
    ("20_pyspark_bronze", "synapse_pyspark", "source", ENV_BRONZE, NB_PYSPARK_BRONZE),
    ("21_pyspark_silver", "synapse_pyspark", "bronze", ENV_SILVER, NB_PYSPARK_SILVER),
    ("22_pyspark_gold", "synapse_pyspark", "silver", ENV_GOLD, NB_PYSPARK_GOLD),
    ("90_results_report", "jupyter", "results", None, NB_RESULTS),
]


def build_cells(kernel, transform_cells):
    """Config + metrics + transform cells -> ipynb cell list.

    The parameters cell (source starting with '# Parameters') is tagged
    `parameters` so Fabric pipeline / notebookutils.run parameter injection
    works. Because injection places overrides in a NEW cell *after* the
    parameters cell, any value derived from a parameter (e.g. schema_out) must
    be computed in a later cell -- so for the benchmark notebooks we insert a
    derived cell right after the parameters cell."""
    lang_group = "jupyter" if kernel == "jupyter" else "synapse_pyspark"
    cells = []
    # shared code first
    for src in (CELL_CONFIG, CELL_METRICS):
        cells.append(("code", src))
    cells.extend(transform_cells)

    def code_cell(src, tags=None):
        meta = {"microsoft": {"language": "python", "language_group": lang_group}}
        if tags:
            meta["tags"] = tags
        return {"cell_type": "code", "execution_count": None, "outputs": [],
                "source": _split(src), "metadata": meta}

    out = []
    for kind, src in cells:
        if kind == "md":
            out.append({"cell_type": "markdown",
                        "source": _split(src), "metadata": {}})
        elif src.startswith("# Parameters"):
            out.append(code_cell(src, tags=["parameters"]))
            # derived-from-parameters values must live AFTER the params cell so
            # injected overrides take effect; only when engine+size are defined.
            if ('engine = "polars"' in src) or ('engine = "pyspark"' in src):
                out.append(code_cell("schema_out = f\"{engine}_{size}\"\n"))
        else:
            out.append(code_cell(src))
    return out


def _split(text):
    lines = text.splitlines(keepends=True)
    return lines if lines else [""]


def build_ipynb(name, kernel, default_layer, env_id):
    ksp = ({"name": "jupyter", "display_name": "Python 3.11"} if kernel == "jupyter"
           else {"name": "synapse_pyspark", "display_name": "Synapse PySpark"})
    lang_group = "jupyter" if kernel == "jupyter" else "synapse_pyspark"
    ws, lh, lh_name = DEFAULT_LH[default_layer]
    deps = {"lakehouse": {"default_lakehouse": lh,
                          "default_lakehouse_name": lh_name,
                          "default_lakehouse_workspace_id": ws}}
    if env_id:
        deps["environment"] = {"environmentId": env_id, "workspaceId": ENG_WS}
    transform = dict(NOTEBOOKS_BY_NAME[name])
    return {
        "nbformat": 4, "nbformat_minor": 5,
        "cells": transform["cells"],
        "metadata": {
            "language_info": {"name": "python"},
            "kernelspec": ksp,
            "microsoft": {"language": "python", "language_group": lang_group},
            "dependencies": deps,
        },
    }


def to_percent_py(name, kernel, transform_cells):
    """Human-readable percent-format copy for the repo."""
    lines = [f"# Fabric benchmark notebook: {name}",
             f"# Kernel: {kernel}",
             "# NOTE: generated by deploy_benchmark_notebooks.py -- edit there, not here.",
             ""]
    for src in (CELL_CONFIG, CELL_METRICS):
        lines.append("# %%")
        lines.append(src.rstrip("\n"))
        lines.append("")
    for kind, src in transform_cells:
        lines.append("# %% [markdown]" if kind == "md" else "# %%")
        body = src.rstrip("\n")
        if kind == "md":
            body = "\n".join("# " + ln for ln in body.splitlines())
        lines.append(body)
        lines.append("")
        if kind == "code" and src.startswith("# Parameters") and (
                'engine = "polars"' in src or 'engine = "pyspark"' in src):
            lines.append("# %%")
            lines.append('schema_out = f"{engine}_{size}"')
            lines.append("")
    return "\n".join(lines)


NOTEBOOKS_BY_NAME = {}


def token():
    out = subprocess.run(
        ["az", "account", "get-access-token", "--tenant", TENANT,
         "--resource", FABRIC, "--query", "accessToken", "-o", "tsv"],
        capture_output=True, text=True, shell=(os.name == "nt"))
    if out.returncode != 0:
        raise SystemExit("az token failed: " + out.stderr)
    return out.stdout.strip()


def api(method, path, tok, body=None, expect_lro=False):
    url = f"{FABRIC}{path}"
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, method=method)
    req.add_header("Authorization", f"Bearer {tok}")
    if data:
        req.add_header("Content-Type", "application/json")
    try:
        resp = urllib.request.urlopen(req)
    except urllib.error.HTTPError as e:
        raise RuntimeError(f"{method} {path} -> {e.code}: {e.read().decode()[:500]}")
    if resp.status == 202 and expect_lro:
        loc = resp.headers.get("Location")
        while True:
            time.sleep(2)
            s = json.loads(urllib.request.urlopen(_auth(loc, tok)).read())
            if s.get("status") not in ("Running", "NotStarted"):
                return s
    body_txt = resp.read().decode()
    return json.loads(body_txt) if body_txt else {}


def _auth(url, tok):
    r = urllib.request.Request(url)
    r.add_header("Authorization", f"Bearer {tok}")
    return r


def list_notebooks(tok):
    r = api("GET", f"/v1/workspaces/{ENG_WS}/items?type=Notebook", tok)
    return {i["displayName"]: i["id"] for i in r.get("value", [])}


def deploy():
    tok = token()
    existing = list_notebooks(tok)
    repo_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "notebooks")
    os.makedirs(repo_dir, exist_ok=True)
    for name, kernel, default_layer, env_id, cells in NOTEBOOKS:
        NOTEBOOKS_BY_NAME[name] = {"cells": build_cells(kernel, cells)}
        # local human-readable copy
        with open(os.path.join(repo_dir, f"{name}.py"), "w", encoding="utf-8") as f:
            f.write(to_percent_py(name, kernel, cells))
        # ipynb definition
        nb = build_ipynb(name, kernel, default_layer, env_id)
        payload = base64.b64encode(json.dumps(nb).encode()).decode()
        parts = [{"path": "notebook-content.ipynb", "payload": payload,
                  "payloadType": "InlineBase64"}]
        definition = {"format": "ipynb", "parts": parts}
        if name in existing:
            nid = existing[name]
            api("POST", f"/v1/workspaces/{ENG_WS}/items/{nid}/updateDefinition",
                tok, {"definition": definition}, expect_lro=True)
            print(f"updated  {name}  ({nid})")
        else:
            body = {"displayName": name, "type": "Notebook", "definition": definition}
            api("POST", f"/v1/workspaces/{ENG_WS}/items", tok, body, expect_lro=True)
            print(f"created  {name}")
    print("done.")


if __name__ == "__main__":
    deploy()
