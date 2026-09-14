"""Stage + publish the three PySpark environments with FAIR, vCore-matched compute.

Fairness rule (agreed): each layer must give Polars and PySpark the SAME number
of *worker* vCores. Polars is single-node, so its budget = the notebook's
`%%configure` vCores (16 / 32 / 16 for bronze / silver / gold). PySpark is
distributed, so we match **executor (worker) vCores** to that budget by pinning
the executor count (dynamic allocation min == max) at an 8-core executor size:

    bronze  2 x 8 = 16 executor vCores   (== Polars 16)
    silver  4 x 8 = 32 executor vCores   (== Polars 32)
    gold    2 x 8 = 16 executor vCores   (== Polars 16)

The Spark **driver** is a required coordinator that Polars has no equivalent for,
so we shrink it to the smallest node (4 cores) and DISCLOSE it as fixed overhead
(it handicaps Spark, never Polars -- the safe direction). Per-run CU is reported
directly with the Fabric formula `cores * 0.5 * active_seconds`, counting driver
+ executor cores for Spark and node vCores for Polars, so nothing is hidden.

Layer-specific Spark SQL tuning (shuffle partitions, AQE, NEE, split size) is
unchanged -- it shapes *how* the fixed vCore budget is used, not how much.

Run:  python deploy_environments.py
"""
import json
import os
import subprocess
import time
import urllib.error
import urllib.request

TENANT = "35acf02c-4b87-4ae6-9221-ff5cafd430b4"
FABRIC = "https://api.fabric.microsoft.com"
ENG_WS = "6dc1aae2-1e5d-4ebb-82b9-faf1a63ed5d8"

# Driver = smallest node (disclosed overhead); executor = 8-core worker.
DRIVER_CORES = 4
DRIVER_MEM = "28g"
EXEC_CORES = 8
EXEC_MEM = "56g"

STARTER = {"name": "Starter Pool", "type": "Workspace",
           "id": "00000000-0000-0000-0000-000000000000"}

# executors pinned so executor vCores == the Polars %%configure budget
ENVS = {
    "4944670a-141e-4385-bed7-9e473232ac60": {  # pyspark-bronze  -> 16 vCores
        "executors": 2,
        "props": {
            "spark.native.enabled": "true",
            "spark.sql.shuffle.partitions": "128",
            "spark.sql.files.maxPartitionBytes": "256m",
        },
    },
    "ecb51557-d241-4172-8b4a-8cf820bea110": {  # pyspark-silver  -> 32 vCores
        "executors": 4,
        "props": {
            "spark.native.enabled": "true",
            "spark.sql.adaptive.enabled": "true",
            "spark.sql.adaptive.coalescePartitions.enabled": "true",
            "spark.sql.adaptive.skewJoin.enabled": "true",
            "spark.sql.shuffle.partitions": "400",
            "spark.sql.files.maxPartitionBytes": "128m",
        },
    },
    "15fa38d3-bcfb-4fc9-b188-ca8408f986eb": {  # pyspark-gold  -> 16 vCores
        "executors": 2,
        "props": {
            "spark.native.enabled": "true",
            "spark.sql.adaptive.enabled": "true",
            "spark.sql.adaptive.coalescePartitions.enabled": "true",
            "spark.sql.shuffle.partitions": "256",
            "spark.sql.files.maxPartitionBytes": "128m",
        },
    },
}


def sparkcompute(cfg):
    n = cfg["executors"]
    return {
        "instancePool": STARTER,
        "driverCores": DRIVER_CORES, "driverMemory": DRIVER_MEM,
        "executorCores": EXEC_CORES, "executorMemory": EXEC_MEM,
        "dynamicExecutorAllocation": {"enabled": True,
                                      "minExecutors": n, "maxExecutors": n},
        "sparkProperties": cfg["props"],
        "runtimeVersion": "1.3",
    }


def token():
    out = subprocess.run(
        ["az", "account", "get-access-token", "--tenant", TENANT,
         "--resource", FABRIC, "--query", "accessToken", "-o", "tsv"],
        capture_output=True, text=True, shell=(os.name == "nt"))
    if out.returncode != 0:
        raise SystemExit("az token failed: " + out.stderr)
    return out.stdout.strip()


def req(method, url, tok, body=None):
    data = json.dumps(body).encode() if body is not None else None
    r = urllib.request.Request(url, data=data, method=method)
    r.add_header("Authorization", f"Bearer {tok}")
    if data:
        r.add_header("Content-Type", "application/json")
    try:
        resp = urllib.request.urlopen(r)
    except urllib.error.HTTPError as e:
        raise RuntimeError(f"{method} {url} -> {e.code}: {e.read().decode()[:600]}")
    txt = resp.read().decode()
    return resp.status, (json.loads(txt) if txt else {})


def deploy():
    tok = token()
    for env_id, cfg in ENVS.items():
        base = f"{FABRIC}/v1/workspaces/{ENG_WS}/environments/{env_id}"
        # 1) update staging spark compute
        req("PATCH", f"{base}/staging/sparkcompute", tok, sparkcompute(cfg))
        # 2) publish staging
        req("POST", f"{base}/staging/publish", tok)
        # 3) poll publish state
        while True:
            time.sleep(5)
            _, e = req("GET", base, tok)
            st = e.get("properties", {}).get("publishDetails", {}).get("state")
            if st not in ("Running", "Waiting", None):
                break
        print(f"{env_id}: executors={cfg['executors']} "
              f"(x{EXEC_CORES}c = {cfg['executors']*EXEC_CORES} worker vCores) "
              f"driver={DRIVER_CORES}c -> publish {st}")


if __name__ == "__main__":
    deploy()
