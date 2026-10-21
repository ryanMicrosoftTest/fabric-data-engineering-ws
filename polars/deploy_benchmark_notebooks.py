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

POLARS_VERSION = "1.42.1"
DELTALAKE_VERSION = "1.6.2"
POLARS_BENCHMARK_NOTEBOOKS = {
    "10_polars_bronze",
    "11_polars_silver",
    "12_polars_gold",
}

DEFAULT_LH = {
    "source": (DATA_WS, LH_SOURCE, "lh_bench_source"),
    "bronze": (DATA_WS, LH_BRONZE, "lh_bench_bronze"),
    "silver": (DATA_WS, LH_SILVER, "lh_bench_silver"),
    "gold": (DATA_WS, LH_GOLD, "lh_bench_gold"),
    "results": (ENG_WS, LH_RESULTS, "lh_bench_results"),
}

CELL_POLARS_INSTALL = f'''\
%pip install --quiet --disable-pip-version-check \
polars=={POLARS_VERSION} deltalake=={DELTALAKE_VERSION}
'''

CELL_POLARS_RUNTIME_CHECK = f'''\
import polars as pl
import deltalake
import pyarrow

_expected_runtime = {{
    "polars": "{POLARS_VERSION}",
    "deltalake": "{DELTALAKE_VERSION}",
}}
_actual_runtime = {{
    "polars": pl.__version__,
    "deltalake": deltalake.__version__,
}}
if _actual_runtime != _expected_runtime:
    raise RuntimeError(
        f"Unexpected Polars runtime: {{_actual_runtime}}; expected {{_expected_runtime}}")
print("Polars runtime:", {{**_actual_runtime, "pyarrow": pyarrow.__version__}})
'''

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

# PySpark needs a coordinator driver that Polars has no equivalent for. It is
# pinned to the smallest node in deploy_environments.py and counted in CU so the
# overhead is visible (it handicaps Spark, never Polars). Polars = 0 extra.
SPARK_DRIVER_CORES = 4

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

def _looks_streaming_unsupported(err):
    """True when Polars ran the plan on the in-memory STANDARD engine because the
    streaming engine can't execute some node (e.g. window / rank over-expressions).
    The tell-tale is the sink error 'not yet supported in standard engine. Use
    collect().write_parquet()'. Used ONLY to gate the small-output streaming
    collect() fallback (gold), so a genuine write error is never mistaken for an
    unsupported plan."""
    m = repr(err).lower()
    return ("standard engine" in m
            or "collect().write_parquet" in m
            or ("not supported" in m and "stream" in m)
            or ("not yet supported" in m and "stream" in m))


def scan_delta_stream(path):
    """Return a STREAMING LazyFrame over a Delta table's ACTIVE data files.

    Reading the Delta log's active file list and scanning those Parquet files
    keeps the source streaming while avoiding stale files left by earlier
    overwrites. A *.parquet glob could double-count those inactive files."""
    import polars as pl
    from deltalake import DeltaTable
    files = [f[7:] if f.startswith("file://") else f
             for f in DeltaTable(path).file_uris()]
    return pl.scan_parquet(files)


def _is_disk_full(err):
    """True when a write failed because the target filesystem ran out of space."""
    import errno
    if isinstance(err, OSError) and err.errno == errno.ENOSPC:
        return True
    m = repr(err).lower()
    return "no space left" in m or "enospc" in m or "disk quota" in m


def _spill_bases():
    """Ordered candidate directories for the streaming spill, FAST LOCAL DISK
    FIRST. `/tmp` is node-local NVMe; the `/lakehouse/default/Files` mount is
    OneLake-backed (network FUSE), so writing the spill there costs a full extra
    network round-trip (write it out, then read it back for the Delta load) and
    is ~10-20x slower. We therefore spill locally whenever the output fits, and
    fall back to the big network mount only for outputs too large for the node's
    local disk (e.g. the ~38GB 50gb fact, which does not fit in /tmp)."""
    import os, shutil
    order = []
    for c in ("/tmp", "/lakehouse/default/Files"):
        try:
            free = shutil.disk_usage(c).free
        except Exception:
            continue
        order.append((c, free))
    if not order:
        order = [("/tmp", 0)]
    return [os.path.join(c, "_polars_sink_tmp") for c, _ in order]


def _write_delta_from_dir(pdir, target):
    """Load a directory of Parquet row groups into Delta INCREMENTALLY via a
    pyarrow dataset (write_deltalake scans batch-by-batch, so it never holds the
    whole table in RAM). One fresh-token retry covers a transient OneLake blip."""
    import time as _time
    import pyarrow.dataset as pads
    from deltalake import write_deltalake
    ds = pads.dataset(pdir, format="parquet")
    for attempt in range(2):
        try:
            write_deltalake(target, ds, mode="overwrite",
                            schema_mode="overwrite",
                            storage_options=onelake_opts())
            return
        except Exception as e:
            if attempt == 0:
                print(f"  write_deltalake failed ({e!r}); retry with fresh token")
                _time.sleep(3)
            else:
                raise


def polars_sink_delta(lf, target, allow_collect_fallback=False):
    """Write a Polars LazyFrame to a Delta table WITHOUT ever materializing the
    full frame in RAM. Peak memory is O(batch), not O(table) -- a headless ETL
    never needs the whole result in memory.

    The benchmark notebooks pin a modern Polars release with native
    `LazyFrame.sink_delta`, so the normal path streams directly to Delta. The
    Parquet spill path remains as a compatibility fallback for older runtimes.

    The spill goes to FAST NODE-LOCAL DISK (/tmp) whenever the output fits there;
    only outputs too large for local disk fall back to the big OneLake-backed
    mount (see _spill_bases). Spilling to the network mount works but is ~10-20x
    slower, so we avoid it unless the local node disk can't hold the output.

    allow_collect_fallback (gold ONLY): if the streaming engine genuinely can't
    run the plan (some window / rank over-expressions), collect() the result with
    the in-memory engine and write that. Safe ONLY because gold output is small,
    aggregated data. Bronze and silver leave this False and FAIL LOUDLY instead --
    silently materializing their full-size output is exactly the OOM (SIGKILL -9)
    we are avoiding.

    Returns the row count from Delta's transaction-log metadata (no data re-scan),
    or None if it can't be read cheaply."""
    import os, shutil, uuid
    import polars as pl
    if hasattr(pl.LazyFrame, "sink_delta"):
        # Pinned benchmark runtime: native streaming Delta sink.
        lf.sink_delta(
            target,
            mode="overwrite",
            storage_options=onelake_opts(),
            delta_write_options={"schema_mode": "overwrite"},
        )
    else:
        bases = _spill_bases()
        last = len(bases) - 1
        disk_err = None
        for i, base in enumerate(bases):
            pdir = os.path.join(base, uuid.uuid4().hex)
            os.makedirs(pdir, exist_ok=True)
            pfile = os.path.join(pdir, "data.parquet")
            try:
                try:
                    # maintain_order=False lets the STREAMING engine run
                    # order-insensitive plans (joins, dedupe). With the default
                    # maintain_order=True Polars 1.6 falls back to the standard
                    # (in-memory) engine for such plans -- which cannot sink and
                    # would otherwise force a full-frame collect() (the OOM path).
                    lf.sink_parquet(pfile, maintain_order=False)
                except Exception as e:
                    if allow_collect_fallback and _looks_streaming_unsupported(e):
                        # gold window/rank: the streaming engine can't fuse the
                        # over()/rank() into the sink, so collect WITH THE
                        # STREAMING ENGINE -- it streams the group_by (bounded)
                        # and only materializes the small aggregated result, then
                        # runs the window on that. Safe because gold output is
                        # tiny; bronze/silver never set this (their result is
                        # full-size, so any collect would be the OOM we avoid).
                        print(f"  streaming sink can't fuse this plan ({e!r}); "
                              f"streaming collect() of the small aggregated output")
                        lf.collect(engine="streaming").write_parquet(pfile)
                    elif _is_disk_full(e) and i < last:
                        # Local spill dir is too small for this output -- fall back
                        # to the next (larger, network-backed) mount.
                        print(f"  spill dir {base} out of space ({e!r}); "
                              f"falling back to {bases[i + 1]}")
                        disk_err = e
                        continue
                    else:
                        raise
                _write_delta_from_dir(pdir, target)
                break
            finally:
                shutil.rmtree(pdir, ignore_errors=True)
        else:
            raise disk_err or RuntimeError("no usable spill location for sink_parquet")
    try:
        from deltalake import DeltaTable
        add = DeltaTable(target, storage_options=onelake_opts()).get_add_actions(flatten=True)
        vals = add.column("num_records").to_pylist()
        return int(sum(v for v in vals if v is not None))
    except Exception:
        return None

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
    result_dir = results_dir()
    notebookutils.fs.mkdirs(result_dir)
    fn = f"{row['run_id']}_{row['engine']}_{row['size']}_{row['layer']}_{row['stage']}.json"
    fn = fn.replace(":", "-").replace("/", "-")
    notebookutils.fs.put(f"{result_dir}/{fn}", json.dumps(row, default=str), True)

def run_stage(engine, size, layer, stage, fn, vcores=None):
    """Time `fn`, capture peak memory + status (success/failed/oom), persist a
    result row, and return the status string. Never raises."""
    try:
        from polars.exceptions import PanicException
        panic_types = (PanicException,)
    except ImportError:
        panic_types = ()
    start = datetime.datetime.utcnow()
    t0 = time.perf_counter(); status = "success"; err = None; rows = None
    with _PeakMem() as pm:
        try:
            rows = fn()
        except MemoryError as e:
            status, err = "oom", repr(e)
        except panic_types as e:
            status, err = "failed", repr(e)
        except Exception as e:
            m = repr(e).lower()
            status = "oom" if ("outofmemory" in m or "out of memory" in m
                               or "memoryerror" in m or "oom" in m) else "failed"
            err = repr(e)
    wall = time.perf_counter() - t0
    end = datetime.datetime.utcnow()
    # CU-seconds (Fabric: cores * 0.5 * active_seconds; startup not billed).
    # Spark counts driver + worker cores; Polars is single-node (no driver).
    driver_cores = SPARK_DRIVER_CORES if engine == "pyspark" else 0
    total_cores = (vcores + driver_cores) if vcores else None
    cu_seconds = round(total_cores * 0.5 * wall, 3) if total_cores else None
    row = {
        "run_id": RUN_ID, "engine": engine, "size": size, "layer": layer,
        "stage": stage, "status": status, "wall_seconds": round(wall, 3),
        "peak_mem_mb": round(pm.peak / 1e6, 1) if pm.peak else None,
        "rows": int(rows) if rows is not None else None,
        "error": (str(err)[:900] if err else None),
        "capacity_id": CAPACITY_ID, "vcores": vcores,
        "driver_cores": driver_cores, "total_cores": total_cores,
        "cu_seconds": cu_seconds,
        "start_utc": start.isoformat(), "end_utc": end.isoformat(),
        "recorded_utc": datetime.datetime.utcnow().isoformat(),
    }
    try:
        _write_result(row)
    except Exception as e:
        print("WARN: could not persist result:", repr(e))
    tag = "OK " if status == "success" else status.upper()
    print(f"[{tag}] {stage}: {wall:.2f}s  peak={row['peak_mem_mb']}MB  "
          f"rows={row['rows']}  cu_s={cu_seconds}")
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
size = "1gb"                                          # one of: 1gb / 10gb / 50gb (pipeline overrides)
scale_factors = {"1gb": 8, "10gb": 80, "50gb": 400}   # starting estimates -> calibrate
'''),
    ("code", '''\
import duckdb, os, time
TABLES = [FACT] + DIMS
out_root = "/lakehouse/default/Files/tpcds"   # default lakehouse = lh_bench_source

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

# ---- Polars vCore advisor -------------------------------------------------
NB_ADVISOR = [
    ("md", "# 02 - Polars vCore advisor\n"
           "Given a `size` and `layer`, measures the **actual on-disk (ZSTD Parquet) "
           "size of that layer's input** in OneLake, then applies a **streaming memory "
           "model** (Polars lazy + `collect(streaming=True)`): peak memory is driven by "
           "the largest blocking op on the **column-pruned** dominant input, not the sum "
           "of all inputs. Recommends a vCore count from the 4/8/16/32/64 ladder "
           "(8 GB RAM per vCore).\n\n"
           "If the true upstream hasn't been produced yet for a size, it falls back to a "
           "**source proxy** (silver->source, gold->source `store_sales`) so you can "
           "right-size *before* the first run, with a printed caveat.\n\n"
           "The recommendation applies to **both engines**: set the same worker vCores "
           "for Polars (`CONFIGURE_VCORES`) and PySpark (`deploy_environments.py`) to keep "
           "the comparison like-for-like. It **reads no full dataset into memory** -- it "
           "only lists file sizes."),
    ("code", '''\
# Parameters
size  = "10gb"    # one of: 1gb, 10gb, 50gb
layer = "silver"  # one of: bronze, silver, gold
'''),
    ("code", '''\
import notebookutils

# ---- Streaming memory model (Polars lazy + collect(streaming=True)) -----------
# A streaming pipeline's peak memory is driven by the largest BLOCKING operation
# on the PRUNED working set (dedupe hash / join build / aggregation table), NOT by
# the sum of all inputs x a big factor. Projection pushdown means unused columns
# are never loaded, so we size from the dominant, column-pruned input. Constants
# are tunable -- refine them against real peak_mem_mb in bench.results.
DECOMPRESS  = 5.0    # ZSTD Parquet on disk -> Arrow in-memory
HEADROOM    = 1.3    # safety margin on the working set
BASELINE_GB = 3.0    # interpreter + Arrow/Polars fixed overhead
VCORE_GB    = 8.0    # Fabric Python kernel: 8 GB RAM per vCore
LADDER      = [4, 8, 16, 32, 64]

# Column-count ratios model projection pushdown (Parquet is columnar => ~linear).
SS_TOTAL_COLS  = 23   # store_sales columns
SILVER_SS_COLS = 8    # columns silver actually reads from store_sales
SC_TOTAL_COLS  = 22   # sales_conformed columns
GOLD_MART_COLS = 6    # widest single gold mart's column set

schema = f"polars_{size}"
LAYER = {
    "bronze": {"primary": ("source", f"Files/tpcds/{size}"),
               "tables": [FACT] + DIMS, "driver": "largest",
               "prune": {}, "op_factor": 1.5,
               "note": "streamed scan->write, one table at a time"},
    "silver": {"primary": ("bronze", f"Tables/{schema}"),
               "fallback": ("source", f"Files/tpcds/{size}"),  # bronze ~= source passthrough
               "tables": [FACT, "date_dim", "item", "store", "customer",
                          "customer_address"], "driver": "sum",
               "prune": {FACT: SILVER_SS_COLS / SS_TOTAL_COLS}, "op_factor": 2.5,
               "note": f"5 joins + dedupe; fact pruned to {SILVER_SS_COLS}/{SS_TOTAL_COLS} cols"},
    "gold":   {"primary": ("silver", f"Tables/{schema}"),
               "tables": ["sales_conformed"], "driver": "sum",
               "prune": {"sales_conformed": GOLD_MART_COLS / SC_TOTAL_COLS}, "op_factor": 1.5,
               "note": f"group-by/window reads ~{GOLD_MART_COLS}/{SC_TOTAL_COLS} cols (spillable)"},
}
if layer not in LAYER:
    raise ValueError(f"layer must be one of {list(LAYER)}; got {layer!r}")
spec = LAYER[layer]

def parquet_bytes(path):
    total = 0
    try:
        entries = notebookutils.fs.ls(path)
    except Exception:
        return 0
    for it in entries:
        is_dir = getattr(it, "isDir", None)
        if is_dir is None:
            is_dir = (it.size == 0 and not it.name.endswith(".parquet"))
        if is_dir:
            total += parquet_bytes(it.path)
        elif it.name.endswith(".parquet"):
            total += it.size
    return total

def measure(lh_key, base, tables):
    uri = abfss(DATA_WS, LH[lh_key], base)
    return uri, {t: parquet_bytes(f"{uri}/{t}") for t in tables}

# Measure the true upstream; fall back to a proxy if it hasn't been produced yet
# so the advisor is usable BEFORE a fresh size has been run end-to-end.
lh_key, base = spec["primary"]
uri, sizes = measure(lh_key, base, spec["tables"])
proxy = None
if sum(sizes.values()) == 0 and spec.get("fallback"):
    lh_key, base = spec["fallback"]
    uri, sizes = measure(lh_key, base, spec["tables"])
    proxy = "source (bronze not produced yet; bronze ~= source passthrough)"
if sum(sizes.values()) == 0 and layer == "gold":
    uri, ss = measure("source", f"Files/tpcds/{size}", [FACT])
    sizes = {"sales_conformed": ss[FACT]}   # ~same rows/width as the conformed fact
    proxy = "source store_sales (silver not produced yet; conformed ~= fact)"
if sum(sizes.values()) == 0:
    raise RuntimeError(
        f"No Parquet found for layer={layer} size={size}. Generate the upstream "
        f"first (bronze<-init_pipeline; silver<-bronze; gold<-silver).")

print(f"measuring {layer} input under {uri}" + (f"  [proxy: {proxy}]" if proxy else ""))
pruned = {}
for t, b in sizes.items():
    pr = spec["prune"].get(t, 1.0)
    pruned[t] = b * pr
    tag = f"  (x{pr:.2f} projection prune)" if pr != 1.0 else ""
    print(f"  {t:20s} {b/1e9:8.3f} GB -> {pruned[t]/1e9:7.3f} GB effective{tag}")

dom = sum(pruned.values()) if spec["driver"] == "sum" else max(pruned.values() or [0])
decompressed = dom / 1e9 * DECOMPRESS
working_set  = decompressed * spec["op_factor"]
required_gb  = working_set * HEADROOM + BASELINE_GB
rec = next((v for v in LADDER if v * VCORE_GB >= required_gb), LADDER[-1])
capped = required_gb > LADDER[-1] * VCORE_GB

bar = "=" * 70
print("\\n" + bar)
print(f"Polars vCore recommendation (streaming model)  |  size={size} layer={layer}")
print(bar)
print(f"  dominant input ({spec['driver']}, pruned): {dom/1e9:8.3f} GB on disk")
print(f"  x decompress {DECOMPRESS} -> {decompressed:.1f} GB in memory")
print(f"  x op_factor {spec['op_factor']} ({spec['note']}) -> {working_set:.1f} GB working set")
print(f"  x headroom {HEADROOM} + {BASELINE_GB} GB baseline -> required {required_gb:.1f} GB")
print(f"  / {VCORE_GB:.0f} GB per vCore -> need >= {required_gb/VCORE_GB:.1f} vCores")
print("-" * 70)
print(f"  RECOMMENDED vCores: {rec}   ({int(rec*VCORE_GB)} GB RAM)")
if capped:
    print(f"  WARNING: estimate ({required_gb:.0f} GB) exceeds the 64-vCore / "
          f"{int(LADDER[-1]*VCORE_GB)} GB max node -- expect a heavily spill-bound")
    print(f"  run or a legitimate OOM result at {size}/{layer}.")
print(bar)
print("\\nFAIRNESS - apply the SAME vCores to BOTH engines (like-for-like):")
print(f"  * Polars : set this layer to {rec} vCores via CONFIGURE_VCORES in")
print(f"             deploy_benchmark_notebooks.py (the %%configure cell).")
print(f"  * PySpark: set the pyspark-{layer} environment to the SAME {rec} worker")
print(f"             vCores (executors x executor-cores) in deploy_environments.py")
print(f"             and re-publish. Spark also runs a fixed 4-core driver, which")
print(f"             is disclosed and counted in CU -- workers stay matched at {rec}.")
print("  Set each layer's budget to the MAX across sizes so both engines share")
print("  one fixed per-layer vCore count (the fairness contract).")

notebookutils.notebook.exit(str(rec))
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
           "`polars_<size>` with the pinned runtime's native streaming `sink_delta`; "
           "the full frame is never materialized. Narrow, scan-heavy, low-shuffle work — "
           "Polars' sweet spot.\n\n"
           "**Recommended vCores (set in Settings > Compute): 16.**"),
    ("code", _POLARS_PARAMS),
    ("code", '''\
import polars as pl, datetime
src_base = f"/lakehouse/default/Files/tpcds/{size}"   # default lakehouse = source

def bronze_one(name):
    def _run():
        lf = (pl.scan_parquet(f"{src_base}/{name}/*.parquet")
                .with_columns([
                    pl.lit(size).alias("_bench_size"),
                    pl.lit(engine).alias("_bench_engine"),
                    pl.lit(datetime.datetime.utcnow().isoformat()).alias("_bronze_ingest_ts"),
                ]))
        # Stream straight to Delta -- never materialize the full table (bronze is a
        # passthrough of the widest fact, so collect() here is what OOM'd at 50gb).
        return polars_sink_delta(lf, tbl("bronze", schema_out, name))
    return _run

for t in [FACT] + DIMS:
    run_stage(engine, size, "bronze", f"bronze:{t}", bronze_one(t), vcores=VCORES)
'''),
]

NB_POLARS_SILVER = [
    ("md", "# 11 - Polars Silver (clean / join / dedupe)\n"
           "Reads Bronze Delta from the **default lakehouse (`lh_bench_bronze`)**, dedupes "
           "the narrow fact on its natural key **before** joining it to its dimensions, and "
           "writes `silver.sales_conformed` to Delta. dedupe-then-join keeps the dedupe "
           "hash table on the small projected fact and every join build side dim-sized; the "
           "plan runs through the pinned runtime's native streaming `sink_delta`. "
           "Shuffle-heavy — where Spark starts to win.\n\n"
           "**Recommended vCores: 32.**"),
    ("code", _POLARS_PARAMS.replace('VCORES = 16', 'VCORES = 32')),
    ("code", '''\
import polars as pl
b = f"/lakehouse/default/Tables/{schema_out}"   # default lakehouse = bronze

def read_b(t, cols=None):
    # Stream from the Delta table's ACTIVE parquet files so projection pushdown
    # and bounded execution apply without reading stale files from old versions.
    lf = scan_delta_stream(f"{b}/{t}")
    return lf.select(cols) if cols else lf

def silver_conform():
    # Prune the fact to only the columns used downstream (Spark's Catalyst does
    # this automatically; doing it here keeps the comparison fair). Dedupe the
    # NARROW fact on its natural key BEFORE the wide joins: the dedupe hash table
    # is then built on the small projected fact (not the full conformed row), and
    # each join's build side stays dim-sized. Result is identical because the
    # dedupe key is fact-only and every dim is joined on its unique PK.
    ss = (read_b("store_sales", [
              "ss_sold_date_sk", "ss_item_sk", "ss_store_sk", "ss_customer_sk",
              "ss_ticket_number", "ss_sales_price", "ss_quantity", "ss_net_paid"])
          .unique(subset=["ss_ticket_number", "ss_item_sk"], keep="first"))
    dd = read_b("date_dim", ["d_date_sk", "d_year", "d_moy", "d_date"])
    it = read_b("item", ["i_item_sk", "i_category", "i_brand", "i_current_price"])
    st = read_b("store", ["s_store_sk", "s_store_name", "s_state"])
    cu = read_b("customer", ["c_customer_sk", "c_current_addr_sk", "c_birth_year"])
    ca = read_b("customer_address", ["ca_address_sk", "ca_state", "ca_city"])
    # Keep both fact and dimension keys to match the PySpark Silver contract and
    # provide the s_store_sk/i_item_sk columns consumed by Gold.
    lf = (ss.join(dd, left_on="ss_sold_date_sk", right_on="d_date_sk",
                  how="inner", coalesce=False)
            .join(it, left_on="ss_item_sk", right_on="i_item_sk",
                  how="inner", coalesce=False)
            .join(st, left_on="ss_store_sk", right_on="s_store_sk",
                  how="left", coalesce=False)
            .join(cu, left_on="ss_customer_sk", right_on="c_customer_sk",
                  how="left", coalesce=False)
            .join(ca, left_on="c_current_addr_sk", right_on="ca_address_sk",
                  how="left", coalesce=False)
            .with_columns([
                (pl.col("ss_sales_price") * pl.col("ss_quantity")).alias("gross_amount"),
                pl.col("ss_net_paid").alias("net_amount"),
            ]))
    # Stream the joined result straight to Delta -- joins are streaming-supported,
    # so peak memory stays bounded instead of materializing the full conformed
    # fact. allow_collect_fallback stays False: silver output is full-size, so a
    # silent collect() here would be an OOM, not a safe fallback.
    return polars_sink_delta(lf, tbl("silver", schema_out, "sales_conformed"))

run_stage(engine, size, "silver", "silver:conform", silver_conform, vcores=VCORES)
'''),
]

NB_POLARS_GOLD = [
    ("md", "# 12 - Polars Gold (aggregates / windows)\n"
           "Reads `silver.sales_conformed` from the **default lakehouse (`lh_bench_silver`)** "
           "and builds curated marts (group-bys + window functions), written to "
           "`lh_bench_gold` with the pinned runtime's native streaming `sink_delta`.\n\n"
           "**Recommended vCores: 16.**"),
    ("code", _POLARS_PARAMS),
    ("code", '''\
import polars as pl
sc_path = f"/lakehouse/default/Tables/{schema_out}/sales_conformed"  # default = silver

def scan_sc():
    # Stream from the conformed table's ACTIVE parquet files so inactive files
    # from older Delta versions are never read.
    return scan_delta_stream(sc_path)

def gold_sales_by_date_store_cat():
    g = (scan_sc().group_by(["d_year", "d_moy", "s_store_sk", "i_category"])
           .agg([pl.col("net_amount").sum().alias("net_revenue"),
                 pl.len().alias("line_items"),
                 pl.col("net_amount").mean().alias("avg_line")]))
    return polars_sink_delta(g, tbl("gold", schema_out, "sales_by_date_store_cat"),
                             allow_collect_fallback=True)

def gold_monthly_store_revenue():
    g = (scan_sc().group_by(["s_store_sk", "d_year", "d_moy"])
           .agg(pl.col("net_amount").sum().alias("revenue"))
           .sort(["s_store_sk", "d_year", "d_moy"])
           .with_columns(pl.col("revenue").cum_sum().over("s_store_sk").alias("running_revenue")))
    return polars_sink_delta(g, tbl("gold", schema_out, "monthly_store_revenue"),
                             allow_collect_fallback=True)

def gold_top_items():
    g = (scan_sc().group_by(["i_category", "i_item_sk"])
           .agg(pl.col("net_amount").sum().alias("revenue"))
           .with_columns(pl.col("revenue").rank("dense", descending=True)
                           .over("i_category").alias("rank_in_cat"))
           .filter(pl.col("rank_in_cat") <= 20))
    return polars_sink_delta(g, tbl("gold", schema_out, "top_items_per_category"),
                             allow_collect_fallback=True)

run_stage(engine, size, "gold", "gold:sales_by_date_store_cat", gold_sales_by_date_store_cat, vcores=VCORES)
run_stage(engine, size, "gold", "gold:monthly_store_revenue",  gold_monthly_store_revenue,  vcores=VCORES)
run_stage(engine, size, "gold", "gold:top_items_per_category",  gold_top_items,              vcores=VCORES)
'''),
]

# ---- PySpark notebooks ----------------------------------------------------
_SPARK_PARAMS = '''\
# Parameters -- change `size` per run. Spark compute comes from the attached
# environment (pyspark-<layer>), which pins executors so worker vCores == VCORES.
engine = "pyspark"
size   = "1gb"          # one of: 1gb, 10gb, 50gb
VCORES = 16             # fair worker-vCore budget (matches Polars %%configure);
                        # enforced by the environment (executors x 8 cores).
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
    run_stage(engine, size, "bronze", f"bronze:{t}", bronze_one(t), vcores=VCORES)
'''),
]

NB_PYSPARK_SILVER = [
    ("md", "# 21 - PySpark Silver (clean / join / dedupe)\n"
           "Joins the fact to its dimensions from **`lh_bench_bronze`** (default lakehouse), "
           "dedupes on the natural key, writes `silver.sales_conformed`. Attached "
           "environment: **pyspark-silver** (AQE + skew join, higher shuffle partitions)."),
    ("code", _SPARK_PARAMS.replace('VCORES = 16', 'VCORES = 32')),
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

run_stage(engine, size, "silver", "silver:conform", silver_conform, vcores=VCORES)
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

run_stage(engine, size, "gold", "gold:sales_by_date_store_cat", gold_sales_by_date_store_cat, vcores=VCORES)
run_stage(engine, size, "gold", "gold:monthly_store_revenue",  gold_monthly_store_revenue,  vcores=VCORES)
run_stage(engine, size, "gold", "gold:top_items_per_category",  gold_top_items,              vcores=VCORES)
'''),
]

NB_RESULTS = [
    ("md", "# 90 - Results report\n"
           "Reads every per-stage JSON result from `lh_bench_results/Files/bench_results`, "
           "consolidates them into the Delta table `bench.results` (default lakehouse = "
           "`lh_bench_results`), and prints Polars-vs-PySpark comparisons plus the "
           "layer-separation view. Run after any batch of benchmark runs."),
    ("code", '''\
import notebookutils, json, polars as pl, time

result_dir = results_dir()
infos = None
for attempt in range(6):
    notebookutils.fs.mkdirs(result_dir)
    try:
        infos = notebookutils.fs.ls(result_dir)
        break
    except FileNotFoundError:
        if attempt == 5:
            raise
        print(f"results directory not visible yet; retry {attempt + 1}/5")
        time.sleep(10)
rows = []
required = {"run_id", "engine", "size", "layer", "stage", "status"}
for f in infos:
    if f.name.endswith(".json"):
        try:
            row = json.loads(notebookutils.fs.head(f.path, 1000000))
            if required.issubset(row):
                rows.append(row)
            else:
                print("skip non-result JSON", f.name)
        except Exception as e:
            print("skip", f.name, repr(e))
print(f"loaded {len(rows)} result rows")
if not rows:
    raise RuntimeError(f"No benchmark result rows found under {result_dir}")
df = pl.DataFrame(rows)
df.write_delta(
    abfss(ENG_WS, RESULTS_LH, "Tables/bench/results"),
    mode="overwrite",
    storage_options=onelake_opts(),
)
df.head()
'''),
    ("code", '''\
# Engine comparison: total wall-clock + CU-seconds per size x layer
comp = (df.filter(pl.col("status") == "success")
          .group_by(["size", "layer", "engine"])
          .agg(pl.col("wall_seconds").sum().alias("wall_s"),
               pl.col("cu_seconds").sum().alias("cu_s"),
               pl.col("peak_mem_mb").max().alias("peak_mb")))
wide = comp.pivot(values="wall_s", index=["size", "layer"], columns="engine",
                  aggregate_function="sum").sort(["size", "layer"])
print("=== wall-clock seconds by engine ===")
print(wide)

cu_wide = comp.pivot(values="cu_s", index=["size", "layer"], columns="engine",
                     aggregate_function="sum").sort(["size", "layer"])
print("=== CU-seconds by engine (cores x 0.5 x active_seconds; Spark incl. driver) ===")
print(cu_wide)

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
print("\\nCU: `cu_seconds` is computed per stage and stored in bench.results "
      "(cores x 0.5 x active_seconds; Spark includes its 4-core driver). "
      "Cross-check totals against the Fabric Capacity Metrics app using "
      "capacity_id + start_utc..end_utc if desired.")
'''),
]

# ===========================================================================
# Notebook registry
# ===========================================================================
NOTEBOOKS = [
    ("00_generate_tpcds", "jupyter", "source", None, NB_GENERATE),
    ("01_calibrate_sizes", "jupyter", "source", None, NB_CALIBRATE),
    ("02_polars_vcore_advisor", "jupyter", "source", None, NB_ADVISOR),
    ("10_polars_bronze", "jupyter", "source", None, NB_POLARS_BRONZE),
    ("11_polars_silver", "jupyter", "bronze", None, NB_POLARS_SILVER),
    ("12_polars_gold", "jupyter", "silver", None, NB_POLARS_GOLD),
    ("20_pyspark_bronze", "synapse_pyspark", "source", ENV_BRONZE, NB_PYSPARK_BRONZE),
    ("21_pyspark_silver", "synapse_pyspark", "bronze", ENV_SILVER, NB_PYSPARK_SILVER),
    ("22_pyspark_gold", "synapse_pyspark", "silver", ENV_GOLD, NB_PYSPARK_GOLD),
    ("90_results_report", "jupyter", "results", None, NB_RESULTS),
]

# Polars (Python-kernel) single-node vCore budget per notebook. Pinned via a
# `%%configure` cell so Polars gets EXACTLY the same worker vCores as the matched
# PySpark environment (see deploy_environments.py) -- the fairness guarantee.
# Static values apply in BOTH interactive and pipeline runs. Valid: 4/8/16/32/64.
CONFIGURE_VCORES = {
    "00_generate_tpcds": 16,   # heavy single-node generation; not part of the comparison
    "10_polars_bronze": 16,
    "11_polars_silver": 32,
    "12_polars_gold": 16,
}


def _configure_cell_src(vcores):
    return ('%%configure -f\n'
            '{\n'
            f'    "vCores": {vcores}\n'
            '}\n')


def build_cells(name, kernel, transform_cells, vcores=None):
    """Config + metrics + transform cells -> ipynb cell list.

    If `vcores` is set (Polars/Python notebooks), a `%%configure` cell is placed
    FIRST to pin the single-node vCore budget -- this is what makes Polars use
    the same worker vCores as the matched PySpark environment.

    The parameters cell (source starting with '# Parameters') is tagged
    `parameters` so Fabric pipeline / notebookutils.run parameter injection
    works. Because injection places overrides in a NEW cell *after* the
    parameters cell, any value derived from a parameter (e.g. schema_out) must
    be computed in a later cell -- so for the benchmark notebooks we insert a
    derived cell right after the parameters cell."""
    lang_group = "jupyter" if kernel == "jupyter" else "synapse_pyspark"
    cells = []
    if vcores and kernel == "jupyter":
        cells.append(("code", _configure_cell_src(vcores)))
    if name in POLARS_BENCHMARK_NOTEBOOKS:
        cells.extend([
            ("code", CELL_POLARS_INSTALL),
            ("code", CELL_POLARS_RUNTIME_CHECK),
        ])
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


def to_percent_py(name, kernel, transform_cells, vcores=None):
    """Human-readable percent-format copy for the repo."""
    lines = [f"# Fabric benchmark notebook: {name}",
             f"# Kernel: {kernel}",
             "# NOTE: generated by deploy_benchmark_notebooks.py -- edit there, not here.",
             ""]
    if vcores and kernel == "jupyter":
        lines.append("# %%")
        lines.append(_configure_cell_src(vcores).rstrip("\n"))
        lines.append("")
    if name in POLARS_BENCHMARK_NOTEBOOKS:
        for src in (CELL_POLARS_INSTALL, CELL_POLARS_RUNTIME_CHECK):
            lines.append("# %%")
            lines.append(src.rstrip("\n"))
            lines.append("")
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
        vcores = CONFIGURE_VCORES.get(name)
        NOTEBOOKS_BY_NAME[name] = {"cells": build_cells(name, kernel, cells, vcores)}
        # local human-readable copy
        with open(os.path.join(repo_dir, f"{name}.py"), "w", encoding="utf-8") as f:
            f.write(to_percent_py(name, kernel, cells, vcores))
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
