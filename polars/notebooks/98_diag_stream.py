# Fabric benchmark notebook: 98_diag_stream
# Kernel: jupyter
# NOTE: authored directly in the Fabric workspace (not emitted by
#       deploy_benchmark_notebooks.py). Mirrored here so the ad-hoc diagnostics
#       live with the rest of the benchmark. Default lakehouse binding:
#       lh_bench_bronze (52711a64-ef06-4608-b3b4-1330984d3c98)
#       in workspace f1e17fe0-3706-425e-ae65-9fda78946327.

# %%
import json, os, notebookutils
import polars as pl
info = {"polars_version": pl.__version__}
b = "/lakehouse/default/Tables/polars_10gb"   # bronze mounted as default

from deltalake import DeltaTable
def sp(t):
    files = [f[7:] if f.startswith("file://") else f
             for f in DeltaTable(f"{b}/{t}").file_uris()]
    return pl.scan_parquet(files)

def probe(name, lf):
    """Record the streaming physical plan (instant, no execution). If the plan
    can't stream, explain(streaming=True) shows little/no STREAMING section."""
    try:
        info[name] = lf.explain(streaming=True)
    except Exception as e:
        info[name] = "EXPLAIN_ERR: " + repr(e)[:300]

cols = ["ss_sold_date_sk","ss_item_sk","ss_store_sk","ss_customer_sk",
        "ss_ticket_number","ss_sales_price","ss_quantity","ss_net_paid"]

probe("A_select", sp("store_sales").select(cols))
probe("B_unique", sp("store_sales").select(cols)
        .unique(subset=["ss_ticket_number","ss_item_sk"], keep="first"))
probe("C_groupby_first", sp("store_sales").select(cols)
        .group_by(["ss_ticket_number","ss_item_sk"]).agg(pl.all().first()))
dd = sp("date_dim").select(["d_date_sk","d_year","d_moy","d_date"])
probe("D_join", sp("store_sales").select(cols)
        .join(dd, left_on="ss_sold_date_sk", right_on="d_date_sk", how="inner"))
probe("E_unique_then_join", sp("store_sales").select(cols)
        .unique(subset=["ss_ticket_number","ss_item_sk"], keep="first")
        .join(dd, left_on="ss_sold_date_sk", right_on="d_date_sk", how="inner"))
probe("F_groupby_then_join", sp("store_sales").select(cols)
        .group_by(["ss_ticket_number","ss_item_sk"]).agg(pl.all().first())
        .join(dd, left_on="ss_sold_date_sk", right_on="d_date_sk", how="inner"))
probe("G_agg", sp("store_sales").select(cols)
        .group_by(["ss_store_sk"]).agg(pl.col("ss_net_paid").sum()))

out = ("abfss://6dc1aae2-1e5d-4ebb-82b9-faf1a63ed5d8@onelake.dfs.fabric.microsoft.com/"
       "44150c61-a3db-4e45-9b2b-4bdfc58a00b3/Files/bench_results/_diag_stream.json")
notebookutils.fs.put(out, json.dumps(info, indent=2), True)
print(json.dumps(info, indent=2))
