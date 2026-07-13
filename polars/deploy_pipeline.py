"""Create/update the `bench_full_run` Data Pipeline in polars-benchmark-engineering-ws.

The pipeline chains every benchmark notebook SEQUENTIALLY (never in parallel) so
that runs never contend for capacity -- essential for fair wall-clock / CU
numbers -- and so each layer sees the previous layer's output. Ordering per size:
polars bronze -> silver -> gold, then pyspark bronze -> silver -> gold. All three
sizes run back-to-back, then 90_results_report consolidates the JSON results into
the `bench.results` Delta table.

Each notebook activity passes `size`; `engine` and `layer` are fixed per notebook.
Because run_stage() swallows OOM/failure (an OOM is a *result*, not a pipeline
error), the chain keeps going; only an unhandled notebook crash breaks it.

Run:  python deploy_pipeline.py
Uses the tenant-35acf02c Fabric token via `az` (same as deploy_benchmark_notebooks.py).
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
PIPELINE_NAME = "bench_full_run"

SIZES = ["1gb", "10gb", "50gb"]

# (activity-suffix, notebookId) in execution order within one size
POLARS = [
    ("polars_bronze", "92d95b03-ef03-4f21-90b0-8cffbd45c718"),
    ("polars_silver", "56f7208b-8521-4d38-b6ad-21030742bc0e"),
    ("polars_gold",   "d9215f86-3a5f-449c-a0b3-8d246b8ff4c1"),
]
PYSPARK = [
    ("pyspark_bronze", "7ee66ff5-b317-47bb-886d-d1bd828b0b87"),
    ("pyspark_silver", "13237c5e-79c0-45ce-944c-788d117ce1bb"),
    ("pyspark_gold",   "ec355c6a-4a2b-440c-9e10-c0a4915f9adc"),
]
RESULTS_NB = "543aefe4-19fe-4ba0-be60-8c49de544ee7"  # 90_results_report


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
        for suffix, nb in POLARS + PYSPARK:
            name = f"{suffix}_{size}"
            activities.append(_nb_activity(name, nb, size, prev))
            prev = name
    # final consolidation (no size parameter)
    activities.append(_nb_activity("results_report", RESULTS_NB, None, prev))
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
