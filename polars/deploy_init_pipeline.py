"""Create/update the `init_pipeline` Data Pipeline in polars-benchmark-engineering-ws.

This is the ONE-TIME SETUP pipeline you run before `bench_full_run`. It produces
the TPC-DS source data that the benchmark's bronze stages read -- something
`bench_full_run` does NOT do (that pipeline starts at bronze).

Order (all sequential, never parallel -- single-node generation is heavy):
    generate 1gb -> generate 10gb -> generate 50gb -> calibrate

- Each `generate_<size>` activity runs `00_generate_tpcds` with `size` injected;
  the notebook writes ZSTD Parquet to `lh_bench_source/Files/tpcds/<size>/...`.
- `calibrate` runs `01_calibrate_sizes` once at the end to report the actual
  compressed size per folder vs the 1/10/50 GB targets (adjust scale_factors in
  00_generate_tpcds and re-run if a size is off target).

50 GB (SF400) generation on a single node is slow/heavy -- expect a long run. To
generate only a subset, run `00_generate_tpcds` manually with the size you want.

Run:  python deploy_init_pipeline.py
Uses the tenant-35acf02c Fabric token via `az` (same as deploy_pipeline.py).
"""
import base64
import json
import os
import subprocess
import time
import urllib.error
import urllib.request

TENANT = "35acf02c-4b87-4ae6-9221-ff5cafd430b4"
FABRIC = "https://api.fabric.microsoft.com"
ENG_WS = "6dc1aae2-1e5d-4ebb-82b9-faf1a63ed5d8"
PIPELINE_NAME = "init_pipeline"

SIZES = ["1gb", "10gb", "50gb"]

GENERATE_NB = "f4e9bc23-8ba9-4483-b352-599dca32fb29"   # 00_generate_tpcds
CALIBRATE_NB = "20558adb-c1d8-4581-86c4-97390b8e2c7d"  # 01_calibrate_sizes


def _nb_activity(name, notebook_id, size, depends_on):
    act = {
        "name": name,
        "type": "TridentNotebook",
        "dependsOn": ([{"activity": depends_on,
                        "dependencyConditions": ["Succeeded"]}]
                      if depends_on else []),
        "policy": {"timeout": "0.12:00:00", "retry": 0,
                   "retryIntervalInSeconds": 30},
        "typeProperties": {
            "notebookId": notebook_id,
            "workspaceId": ENG_WS,
        },
    }
    if size is not None:
        act["typeProperties"]["parameters"] = {
            "size": {"value": size, "type": "string"}}
    return act


def build_pipeline():
    activities = []
    prev = None
    for size in SIZES:
        name = f"generate_{size}"
        activities.append(_nb_activity(name, GENERATE_NB, size, prev))
        prev = name
    # calibrate once, after every size is generated (no size parameter)
    activities.append(_nb_activity("calibrate", CALIBRATE_NB, None, prev))
    return {"properties": {"activities": activities}}


def token():
    out = subprocess.run(
        ["az", "account", "get-access-token", "--tenant", TENANT,
         "--resource", FABRIC, "--query", "accessToken", "-o", "tsv"],
        capture_output=True, text=True, shell=(os.name == "nt"))
    if out.returncode != 0:
        raise SystemExit("az token failed: " + out.stderr)
    return out.stdout.strip()


def _auth(url, tok):
    r = urllib.request.Request(url)
    r.add_header("Authorization", f"Bearer {tok}")
    return r


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
        raise RuntimeError(f"{method} {path} -> {e.code}: {e.read().decode()[:600]}")
    if resp.status == 202 and expect_lro:
        loc = resp.headers.get("Location")
        while True:
            time.sleep(2)
            s = json.loads(urllib.request.urlopen(_auth(loc, tok)).read())
            if s.get("status") not in ("Running", "NotStarted"):
                return s
    body_txt = resp.read().decode()
    return json.loads(body_txt) if body_txt else {}


def deploy():
    tok = token()
    definition = build_pipeline()
    payload = base64.b64encode(
        json.dumps(definition).encode()).decode()
    parts = [{"path": "pipeline-content.json",
              "payload": payload, "payloadType": "InlineBase64"}]

    existing = api("GET", f"/v1/workspaces/{ENG_WS}/items?type=DataPipeline", tok)
    match = next((i for i in existing.get("value", [])
                  if i["displayName"] == PIPELINE_NAME), None)
    if match:
        api("POST",
            f"/v1/workspaces/{ENG_WS}/items/{match['id']}/updateDefinition",
            tok, {"definition": {"parts": parts}}, expect_lro=True)
        print(f"updated  {PIPELINE_NAME}  ({match['id']})")
    else:
        body = {"displayName": PIPELINE_NAME, "type": "DataPipeline",
                "definition": {"parts": parts}}
        res = api("POST", f"/v1/workspaces/{ENG_WS}/items", tok, body,
                  expect_lro=True)
        print(f"created  {PIPELINE_NAME}  ({json.dumps(res)[:200]})")
    print(f"activities: {len(definition['properties']['activities'])}")


if __name__ == "__main__":
    deploy()
