"""
synthea_full_run — Airflow port of the Fabric ``synthea_full_run`` Data Pipeline
================================================================================
Option B (pool-per-job): the **entire** Synthea medallion pipeline is ONE
end-to-end job that runs on ONE Custom Live Pool sized to 60% of an F64 capacity.

This DAG only *orchestrates*; the heavy lifting stays in the five Fabric
notebooks (run via ``FabricRunItemOperator``). It reproduces the source stages:

    set_run_id_root (once)
        -> ForEach Generate        (8 cohorts, batch 4)
        -> ForEach BronzeToSilver  (8 cohorts, batch 4)
        -> silver_to_gold          (once)

with two light Option-B enhancements:
  * ``00_run_log_init`` is wired in as a guarded ``init_run_log`` first task.
  * Per-cohort skip/restart is driven by ``gold.control.run_state`` (the
    notebooks self-record state and self-skip; see include/run_state_helpers.py
    and README §"Idempotency").

Concurrency is governed by TWO independent ceilings (see README):
  * Custom Live Pool ``synthea_pool_60`` — the capacity ceiling (~60% of F64).
  * Airflow Pool ``synthea_60`` (4 slots) — caps concurrent cohort tasks,
    mirroring the source ``ForEach`` batchCount=4.

Prerequisites (one-time) — see README §"Prerequisites":
  1. Tenant setting "Service principals can call Fabric public APIs" = enabled.
  2. A service principal added as *Contributor* on synthea-data-airflow-ws.
  3. An Airflow connection ``fabric_conn`` holding the SP Tenant/Client ID + secret.
  4. Custom Live Pool ``synthea_pool_60`` created + bound to the Environment the
     notebooks target.
  5. Airflow Pool ``synthea_60`` created with 4 slots.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task
from apache_airflow_microsoft_fabric_plugin.operators.fabric import (
    FabricRunItemOperator,
)

# ---- Configuration -------------------------------------------------------
# Replace the placeholder GUIDs with the item IDs from the
# synthea-data-airflow-ws workspace (Notebook -> ... -> Properties -> Item ID).
WORKSPACE_ID = "b45f2319-b9cc-49f2-abd8-f19d2e2a078b"
FABRIC_CONN_ID = "fabric_conn"

RUN_LOG_INIT_NB = "7cdb1993-c8a9-42c0-a93a-1907ff20cac1"
GENERATE_NB = "f12561b8-21e2-4ff0-b1e4-720202c6ccb5"
BRONZE_TO_SILVER_NB = "04b7f54b-e248-4c80-99bf-55d1ce560291"
SILVER_TO_GOLD_NB = "593ad7db-4b36-44b5-acc8-0d3cb8ed24df"

# Airflow Pool that throttles cohort fan-out to 4 (mirrors ForEach batchCount=4).
COHORT_POOL = "synthea_60"

# The 8 cohorts that drive the fan-out. These fields are the orchestration-
# relevant keys; the generate notebook resolves a dataset_id into its full
# Synthea config (modules, seed, fhir/csv export flags). If your notebooks
# expect those fields explicitly, add them to each dict here.
COHORTS: list[dict] = [
    {"dataset_id": "ma_diabetes", "patient_count": 50000, "state": "Massachusetts"},
    {"dataset_id": "oncology", "patient_count": 25000, "state": "Massachusetts"},
    {"dataset_id": "claims_cpcds", "patient_count": 50000, "state": "Massachusetts"},
    {"dataset_id": "ehr_fhir", "patient_count": 5000, "state": "Massachusetts"},
    {"dataset_id": "sdoh", "patient_count": 25000, "state": "Massachusetts"},
    {"dataset_id": "houston_geo", "patient_count": 40000, "state": "Texas"},
    {"dataset_id": "provider_directory", "patient_count": 500, "state": "Massachusetts"},
    {"dataset_id": "covid_national", "patient_count": 55000, "state": "Massachusetts"},
]

default_args = {
    "owner": "data-engineering",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def _run_notebook(
    task_id: str,
    item_id: str,
    execution_timeout: timedelta,
    job_params: dict | None = None,
) -> FabricRunItemOperator:
    """Build a FabricRunItemOperator that runs a Fabric notebook to completion.

    ``job_params`` is forwarded to the notebook's ``parameters``-tagged cell.
    (Verify the keyword against your installed plugin version — recent
    apache-airflow-microsoft-fabric-plugin releases accept ``job_params``.)
    """
    return FabricRunItemOperator(
        task_id=task_id,
        fabric_conn_id=FABRIC_CONN_ID,
        workspace_id=WORKSPACE_ID,
        item_id=item_id,
        job_type="RunNotebook",
        wait_for_termination=True,
        # deferrable=True frees the worker slot while polling but needs
        # "Enable triggers" on the Environment. Left False for zero extra setup.
        deferrable=False,
        execution_timeout=execution_timeout,
        job_params=job_params or {},
    )


with DAG(
    dag_id="synthea_full_run",
    description="Option B Airflow port of the Synthea bronze->silver->gold pipeline",
    # Manual / on-demand by default (matches the source pipeline). For a daily
    # run, set schedule="0 6 * * *".
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    max_active_tasks=4,  # belt-and-suspenders alongside the synthea_60 pool
    tags=["fabric", "synthea", "medallion", "option-b"],
) as dag:

    # Stage 0 — guarded init: creates gold.agent.run_log and gold.control.run_state
    # (idempotent CREATE IF NOT EXISTS; safe no-op on re-runs).
    init_run_log = _run_notebook(
        task_id="init_run_log",
        item_id=RUN_LOG_INIT_NB,
        execution_timeout=timedelta(minutes=30),
    )

    # Stage 1 — compute the run grouping key once (replaces SetVariable).
    @task(task_id="set_run_id")
    def set_run_id() -> str:
        return datetime.utcnow().strftime("%Y%m%d%H%M%S")

    # Build the per-cohort parameter lists for dynamic task mapping. Stage is
    # baked in so each notebook knows which run_state row to record/skip.
    @task(task_id="build_generate_params")
    def build_generate_params(run_id_root: str) -> list[dict]:
        return [
            {**c, "run_id_root": run_id_root, "stage": "generate"} for c in COHORTS
        ]

    @task(task_id="build_bronze_to_silver_params")
    def build_bronze_to_silver_params(run_id_root: str) -> list[dict]:
        return [
            {**c, "run_id_root": run_id_root, "stage": "bronze_to_silver"}
            for c in COHORTS
        ]

    run_id_root = set_run_id()
    gen_params = build_generate_params(run_id_root)
    b2s_params = build_bronze_to_silver_params(run_id_root)

    # Stage 2 — generate (8 mapped tasks, <=4 concurrent via the synthea_60 pool).
    generate = FabricRunItemOperator.partial(
        task_id="generate",
        fabric_conn_id=FABRIC_CONN_ID,
        workspace_id=WORKSPACE_ID,
        item_id=GENERATE_NB,
        job_type="RunNotebook",
        wait_for_termination=True,
        deferrable=False,
        pool=COHORT_POOL,
        execution_timeout=timedelta(hours=12),
    ).expand(job_params=gen_params)

    # Stage 3 — bronze -> silver (8 mapped tasks, <=4 concurrent). The
    # generate >> bronze_to_silver edge enforces the source's stage-gate:
    # ALL generate complete before ANY bronze->silver starts.
    bronze_to_silver = FabricRunItemOperator.partial(
        task_id="bronze_to_silver",
        fabric_conn_id=FABRIC_CONN_ID,
        workspace_id=WORKSPACE_ID,
        item_id=BRONZE_TO_SILVER_NB,
        job_type="RunNotebook",
        wait_for_termination=True,
        deferrable=False,
        pool=COHORT_POOL,
        execution_timeout=timedelta(hours=6),
    ).expand(job_params=b2s_params)

    # Stage 4 — silver -> gold (once, after all silver). run_id_root is pulled
    # from the set_run_id XCom via Jinja templating of job_params.
    silver_to_gold = _run_notebook(
        task_id="silver_to_gold",
        item_id=SILVER_TO_GOLD_NB,
        execution_timeout=timedelta(hours=4),
        job_params={
            "run_id_root": "{{ ti.xcom_pull(task_ids='set_run_id') }}",
            "stage": "silver_to_gold",
        },
    )

    init_run_log >> run_id_root
    gen_params >> generate
    b2s_params >> bronze_to_silver
    generate >> bronze_to_silver >> silver_to_gold
