from __future__ import annotations

from datetime import timedelta
from types import SimpleNamespace

import pytest
from airflow.exceptions import AirflowException

from medallion_fabric.compute_config import ComputeConfiguration
from medallion_fabric.fabric_notebook_hook import FabricJobSubmission, FabricJobTimeout
from medallion_fabric.fabric_notebook_operator import (
    SqlConfiguredFabricNotebookOperator,
)

WORKSPACE = "896bca78-0d4a-4dc9-8716-05539ecf4ea5"
NOTEBOOK = "d877f06a-d72a-469a-8c82-4ed918b0686b"
JOB = "2d6aa964-5f3a-4c95-a878-cc761ae71391"


def operator(**kwargs):
    return SqlConfiguredFabricNotebookOperator(
        task_id="run",
        workspace_id=WORKSPACE,
        notebook_id=NOTEBOOK,
        **kwargs,
    )


def test_runtime_lookup_submission_poll_and_safe_xcom(monkeypatch):
    calls = []
    config = ComputeConfiguration("56g", 8, "56g", 8, 2)

    class SqlHook:
        def __init__(self, sql_conn_id):
            calls.append(("sql", sql_conn_id))

        def get_compute_configuration(self, notebook_id):
            calls.append(("lookup", notebook_id))
            return config

    class FabricHook:
        def __init__(self, fabric_conn_id):
            calls.append(("fabric", fabric_conn_id))

        def submit_notebook(self, workspace, notebook, configuration, parameters):
            calls.append(("submit", configuration, parameters))
            return FabricJobSubmission(JOB, "location", "request", 1)

        def wait_for_completion(self, submission, timeout):
            calls.append(("poll", timeout))
            return {"status": "COMPLETED"}

    monkeypatch.setattr(
        "medallion_fabric.fabric_notebook_operator.FabricSqlConfigHook", SqlHook
    )
    monkeypatch.setattr(
        "medallion_fabric.fabric_notebook_operator.FabricNotebookHook", FabricHook
    )
    ti = SimpleNamespace(values=[])
    ti.xcom_push = lambda **value: ti.values.append(value)
    result = operator(timeout_seconds=20).execute({"ti": ti})
    assert [call[0] for call in calls] == ["sql", "lookup", "fabric", "submit", "poll"]
    assert result["job_instance_id"] == JOB
    assert result["requested_compute_configuration"]["driverMemory"] == "56g"
    assert "secret" not in str(ti.values).lower()


def test_invalid_enabled_row_fails_before_fabric_hook_creation(monkeypatch):
    class SqlHook:
        def __init__(self, **kwargs):
            pass

        def get_compute_configuration(self, notebook_id):
            raise AirflowException("invalid enabled row")

    monkeypatch.setattr(
        "medallion_fabric.fabric_notebook_operator.FabricSqlConfigHook", SqlHook
    )
    with pytest.raises(AirflowException, match="invalid enabled row"):
        operator().execute({})


def test_execution_timeout_is_honored():
    instance = operator(timeout_seconds=100, execution_timeout=timedelta(seconds=30))
    assert instance._effective_timeout() == 30


def test_on_kill_cancels_active_job():
    instance = operator()
    calls = []
    instance._fabric_hook = SimpleNamespace(cancel=lambda *args: calls.append(args))
    instance._submission = FabricJobSubmission(JOB, "location", None, 1)
    instance.on_kill()
    assert calls == [(WORKSPACE, NOTEBOOK, JOB)]


def test_on_kill_propagates_cancellation_failure():
    instance = operator()

    def fail(*args):
        raise AirflowException("cancel failed")

    instance._fabric_hook = SimpleNamespace(cancel=fail)
    instance._submission = FabricJobSubmission(JOB, "location", None, 1)
    with pytest.raises(AirflowException, match="cancel failed"):
        instance.on_kill()


def test_timeout_cancels_remote_job_and_preserves_timeout(monkeypatch):
    calls = []

    class SqlHook:
        def __init__(self, **kwargs):
            pass

        def get_compute_configuration(self, notebook_id):
            return None

    class FabricHook:
        def __init__(self, **kwargs):
            pass

        def submit_notebook(self, *args):
            return FabricJobSubmission(JOB, "location", None, 1)

        def wait_for_completion(self, *args):
            raise FabricJobTimeout("original timeout")

        def cancel(self, *args):
            calls.append(args)

    monkeypatch.setattr(
        "medallion_fabric.fabric_notebook_operator.FabricSqlConfigHook", SqlHook
    )
    monkeypatch.setattr(
        "medallion_fabric.fabric_notebook_operator.FabricNotebookHook", FabricHook
    )
    with pytest.raises(FabricJobTimeout, match="original timeout"):
        operator().execute({})
    assert calls == [(WORKSPACE, NOTEBOOK, JOB)]


def test_timeout_reports_cancel_failure_without_replacing_timeout(monkeypatch):
    class SqlHook:
        def __init__(self, **kwargs):
            pass

        def get_compute_configuration(self, notebook_id):
            return None

    class FabricHook:
        def __init__(self, **kwargs):
            pass

        def submit_notebook(self, *args):
            return FabricJobSubmission(JOB, "location", None, 1)

        def wait_for_completion(self, *args):
            raise FabricJobTimeout("original timeout")

        def cancel(self, *args):
            raise AirflowException("cancel endpoint unavailable")

    monkeypatch.setattr(
        "medallion_fabric.fabric_notebook_operator.FabricSqlConfigHook", SqlHook
    )
    monkeypatch.setattr(
        "medallion_fabric.fabric_notebook_operator.FabricNotebookHook", FabricHook
    )
    with pytest.raises(FabricJobTimeout, match="original timeout") as raised:
        operator().execute({})
    assert raised.value.__cause__ is not None
    assert "cancel endpoint unavailable" in raised.value.__notes__[0]
