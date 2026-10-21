"""
Medallion orchestration DAG for NYC Taxi Data
"""

import sys
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG

_DAGS_DIR = str(Path(__file__).resolve().parent)
if _DAGS_DIR not in sys.path:
    sys.path.insert(0, _DAGS_DIR)

from fabric_notebook_fallback import (
    SqlConfiguredFabricNotebookOperator,
)

 ### Config ######################################
WORKSPACE_ID = "896bca78-0d4a-4dc9-8716-05539ecf4ea5"
FABRIC_CONN_ID = "fabric_conn"
SQL_CONN_ID = "airflow_config_sql"

BRONZE_NOTEBOOK_ID = "d877f06a-d72a-469a-8c82-4ed918b0686b"  # nb_bronze_ingest
SILVER_NOTEBOOK_ID = "a1168d27-aeb6-419b-b835-38ef62206504"  # nb_silver_transform
GOLD_NOTEBOOK_ID = "bd214a6e-26f2-40d2-912e-467e5724e2e9"    # nb_gold_aggregate

default_args = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


def run_notebook(
    task_id: str, notebook_id: str
) -> SqlConfiguredFabricNotebookOperator:
    """
    Build a SQL-configured operator that runs a Fabric notebook to completion.
    """
    return SqlConfiguredFabricNotebookOperator(
        task_id=task_id,
        fabric_conn_id=FABRIC_CONN_ID,
        sql_conn_id=SQL_CONN_ID,
        workspace_id=WORKSPACE_ID,
        notebook_id=notebook_id,
    )


with DAG(
    dag_id="medallion_nyc_taxi",
    description="Bronze -> Silver -> Gold NYC taxi medallion orchestration",
    schedule_interval="0 10 * * *",  # 10:00 daily
    start_date=datetime(2024, 1, 1),
    catchup=False,
    is_paused_upon_creation=True,
    default_args=default_args,
    tags=["fabric", "medallion", "nyc-taxi"],
) as dag:

    bronze = run_notebook("bronze_ingest", BRONZE_NOTEBOOK_ID)
    silver = run_notebook("silver_transform", SILVER_NOTEBOOK_ID)
    gold = run_notebook("gold_aggregate", GOLD_NOTEBOOK_ID)

    bronze >> silver >> gold
