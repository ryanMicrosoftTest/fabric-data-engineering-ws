from datetime import timedelta

from fabric_notebook_fallback import (
    FabricJobSubmission,
    SqlConfiguredFabricNotebookOperator,
)

from medallion_orchestrator.dags.medallion_pipeline import dag


EXPECTED_NOTEBOOKS = {
    "bronze_ingest": "d877f06a-d72a-469a-8c82-4ed918b0686b",
    "silver_transform": "a1168d27-aeb6-419b-b835-38ef62206504",
    "gold_aggregate": "bd214a6e-26f2-40d2-912e-467e5724e2e9",
}


def test_dag_preserves_metadata_and_builds_sql_configured_notebook_tasks():
    assert dag.dag_id == "medallion_nyc_taxi"
    assert dag.schedule_interval == "0 10 * * *"
    assert dag.catchup is False
    assert dag.default_args["retries"] == 2
    assert dag.default_args["retry_delay"] == timedelta(minutes=5)
    assert set(dag.task_dict) == set(EXPECTED_NOTEBOOKS)

    for task_id, notebook_id in EXPECTED_NOTEBOOKS.items():
        task = dag.get_task(task_id)
        assert isinstance(task, SqlConfiguredFabricNotebookOperator)
        assert task.workspace_id == "896bca78-0d4a-4dc9-8716-05539ecf4ea5"
        assert task.notebook_id == notebook_id
        assert task.fabric_conn_id == "fabric_conn"
        assert task.sql_conn_id == "airflow_config_sql"
        assert task.deferrable is False
        assert not {
            "driver_memory",
            "driver_cores",
            "executor_memory",
            "executor_cores",
            "num_executors",
        }.intersection(vars(task))


def test_dag_preserves_bronze_silver_gold_order():
    assert dag.get_task("bronze_ingest").downstream_task_ids == {
        "silver_transform"
    }
    assert dag.get_task("silver_transform").downstream_task_ids == {
        "gold_aggregate"
    }
    assert dag.get_task("gold_aggregate").downstream_task_ids == set()


def test_each_task_queries_sql_by_its_notebook_id_at_execution(monkeypatch):
    queried_notebook_ids = []

    class SqlHook:
        def __init__(self, sql_conn_id):
            assert sql_conn_id == "airflow_config_sql"

        def get_compute_configuration(self, notebook_id):
            queried_notebook_ids.append(notebook_id)
            return None

    class FabricHook:
        def __init__(self, fabric_conn_id):
            assert fabric_conn_id == "fabric_conn"

        def submit_notebook(self, workspace_id, notebook_id, config, parameters):
            return FabricJobSubmission("job-id", "location", "request-id", 1)

        def wait_for_completion(self, submission, timeout):
            return {"status": "COMPLETED"}

    monkeypatch.setattr(
        "fabric_notebook_fallback.FabricSqlConfigHook", SqlHook
    )
    monkeypatch.setattr(
        "fabric_notebook_fallback.FabricNotebookHook", FabricHook
    )

    for task_id in EXPECTED_NOTEBOOKS:
        dag.get_task(task_id).execute({})

    assert queried_notebook_ids == list(EXPECTED_NOTEBOOKS.values())
