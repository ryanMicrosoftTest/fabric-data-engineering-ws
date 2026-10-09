# Fabric benchmark notebook: 99_diag_polars
# Kernel: jupyter
# NOTE: authored directly in the Fabric workspace (not emitted by
#       deploy_benchmark_notebooks.py). Mirrored here so the ad-hoc diagnostics
#       live with the rest of the benchmark. Default lakehouse binding:
#       lh_bench_results (44150c61-a3db-4e45-9b2b-4bdfc58a00b3)
#       in workspace 6dc1aae2-1e5d-4ebb-82b9-faf1a63ed5d8.

# %%
import json, inspect, os, shutil, uuid, notebookutils
info = {}
import polars as pl
info["polars_version"] = pl.__version__
# disk capacity for candidate scratch locations
for p in ["/tmp", "/", "/lakehouse/default/Files"]:
    try:
        du = shutil.disk_usage(p)
        info[f"disk {p}"] = {"total_gb": round(du.total/1e9,1),
                             "free_gb": round(du.free/1e9,1)}
    except Exception as e:
        info[f"disk {p}"] = repr(e)

# --- real round-trip test of the proposed memory-bounded write path ----------
opts = {"bearer_token": notebookutils.credentials.getToken("storage"),
        "use_fabric_endpoint": "true"}
target = ("abfss://6dc1aae2-1e5d-4ebb-82b9-faf1a63ed5d8@onelake.dfs.fabric.microsoft.com/"
          "44150c61-a3db-4e45-9b2b-4bdfc58a00b3/Tables/_diag/sink_test")
try:
    lf = pl.LazyFrame({"a": list(range(100000)), "b": ["x"]*100000})
    import pyarrow.dataset as pads
    from deltalake import write_deltalake
    tmp = f"/tmp/_sink_{uuid.uuid4().hex}"
    os.makedirs(tmp, exist_ok=True)
    pq = f"{tmp}/data.parquet"
    lf.sink_parquet(pq)                                  # streaming write
    ds = pads.dataset(tmp, format="parquet")             # lazy dataset
    write_deltalake(target, ds, mode="overwrite", storage_options=opts)
    shutil.rmtree(tmp, ignore_errors=True)
    from deltalake import DeltaTable
    add = DeltaTable(target, storage_options=opts).get_add_actions(flatten=True)
    info["roundtrip_rows"] = int(sum(v for v in add.column("num_records").to_pylist() if v))
    info["roundtrip"] = "OK"
except Exception as e:
    info["roundtrip"] = repr(e)

path = ("abfss://6dc1aae2-1e5d-4ebb-82b9-faf1a63ed5d8@onelake.dfs.fabric.microsoft.com/"
        "44150c61-a3db-4e45-9b2b-4bdfc58a00b3/Files/bench_results/_diag_polars.json")
notebookutils.fs.put(path, json.dumps(info, indent=2), True)
print(json.dumps(info, indent=2))
