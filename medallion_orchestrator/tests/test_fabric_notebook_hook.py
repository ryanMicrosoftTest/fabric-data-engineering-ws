from __future__ import annotations

from types import SimpleNamespace

import pytest
from airflow.exceptions import AirflowException

from medallion_fabric.compute_config import ComputeConfiguration
from medallion_fabric.fabric_notebook_hook import (
    FABRIC_API_SCOPE,
    FabricJobSubmission,
    FabricNotebookHook,
)

WORKSPACE = "896bca78-0d4a-4dc9-8716-05539ecf4ea5"
NOTEBOOK = "d877f06a-d72a-469a-8c82-4ed918b0686b"
JOB = "2d6aa964-5f3a-4c95-a878-cc761ae71391"
LOCATION = (
    f"https://api.fabric.microsoft.com/v1/workspaces/{WORKSPACE}/items/{NOTEBOOK}"
    f"/jobs/instances/{JOB}"
)


class Response:
    def __init__(self, status_code, *, headers=None, payload=None, text=""):
        self.status_code = status_code
        self.headers = headers or {}
        self.payload = payload
        self.text = text

    def json(self):
        if isinstance(self.payload, Exception):
            raise self.payload
        return self.payload


class Session:
    def __init__(self, post=None, gets=None):
        self.post_response = post
        self.gets = list(gets or [])
        self.posts = []
        self.get_calls = []

    def post(self, url, **kwargs):
        self.posts.append((url, kwargs))
        return self.post_response

    def get(self, url, **kwargs):
        self.get_calls.append((url, kwargs))
        return self.gets.pop(0)


def hook(session):
    result = FabricNotebookHook(
        session=session, min_poll_interval=1, max_poll_interval=4
    )
    result._access_token = "fabric-token"
    result._connection_settings = lambda: (
        "https://api.fabric.microsoft.com",
        "tenant",
        "client",
        "secret",
    )
    return result


def test_exact_request_shape_and_top_level_typed_parameters():
    session = Session(
        post=Response(
            202,
            headers={"Location": LOCATION, "Retry-After": "3", "x-ms-request-id": "req"},
        )
    )
    config = ComputeConfiguration("56g", 8, "56g", 8, 2)
    parameters = [{"name": "fullLoad", "value": True, "type": "Boolean"}]
    submission = hook(session).submit_notebook(
        WORKSPACE, NOTEBOOK, config, parameters
    )
    url, kwargs = session.posts[0]
    assert url.endswith(f"/notebooks/{NOTEBOOK}/jobs/execute/instances?beta=false")
    assert kwargs["json"] == {
        "executionData": {
            "compute": "Spark",
            "computeConfiguration": {
                "driverMemory": "56g",
                "driverCores": 8,
                "executorMemory": "56g",
                "executorCores": 8,
                "numExecutors": 2,
            },
        },
        "parameters": parameters,
    }
    assert "instancePool" not in str(kwargs["json"])
    assert submission.retry_after == 3


def test_defaults_omit_compute_configuration():
    session = Session(post=Response(202, headers={"Location": LOCATION}))
    hook(session).submit_notebook(WORKSPACE, NOTEBOOK, None)
    assert session.posts[0][1]["json"] == {"executionData": {"compute": "Spark"}}


@pytest.mark.parametrize(
    ("location", "expected"),
    [
        (f"{LOCATION}?continuationToken=not-a-job-id", JOB),
        (
            LOCATION.replace(JOB, JOB.upper()) + "/?ignored=true",
            JOB,
        ),
        (
            LOCATION.replace(JOB, JOB.replace("-", "")),
            JOB,
        ),
    ],
)
def test_submission_parses_uuid_from_location_path(location, expected):
    session = Session(post=Response(202, headers={"Location": location}))
    submission = hook(session).submit_notebook(WORKSPACE, NOTEBOOK, None)
    assert submission.job_instance_id == expected


@pytest.mark.parametrize(
    "location",
    [
        "https://api.fabric.microsoft.com/jobs/instances?jobInstanceId=" + JOB,
        "https://api.fabric.microsoft.com/jobs/instances/not-a-uuid?value=" + JOB,
    ],
)
def test_submission_fails_when_location_path_has_no_job_uuid(location):
    session = Session(post=Response(202, headers={"Location": location}))
    with pytest.raises(AirflowException, match="Location header.*valid job instance UUID"):
        hook(session).submit_notebook(WORKSPACE, NOTEBOOK, None)


def test_submission_requires_202_and_does_not_expose_response_or_token():
    session = Session(
        post=Response(401, headers={"request-id": "req"}, text="secret fabric-token")
    )
    with pytest.raises(AirflowException) as exc:
        hook(session).submit_notebook(WORKSPACE, NOTEBOOK, None)
    message = str(exc.value)
    assert "401" in message and "req" in message
    assert "secret" not in message and "fabric-token" not in message


def test_poll_success_with_throttle_and_bounded_backoff(monkeypatch):
    session = Session(
        gets=[
            Response(429, headers={"Retry-After": "2"}),
            Response(200, payload={"status": "InProgress"}),
            Response(200, payload={"status": "Completed"}),
        ]
    )
    sleeps = []
    ticks = iter([0, 0, 1, 2, 3])
    monkeypatch.setattr(
        "medallion_fabric.fabric_notebook_hook.time.monotonic", lambda: next(ticks)
    )
    monkeypatch.setattr(
        "medallion_fabric.fabric_notebook_hook.time.sleep", sleeps.append
    )
    result = hook(session).wait_for_completion(
        FabricJobSubmission(JOB, LOCATION, "req", 1), 100
    )
    assert result["status"] == "COMPLETED"
    assert max(sleeps) <= 4


def test_poll_success_returns_only_sanitized_monitoring_evidence(monkeypatch):
    session = Session(
        gets=[
            Response(
                200,
                payload={
                    "status": "Completed",
                    "startTimeUtc": "2026-08-03T20:00:00Z",
                    "endTimeUtc": "2026-08-03T20:01:00Z",
                    "sparkApplicationId": "application_123",
                    "exitValue": {"driver_memory": "56g", "token": "private"},
                    "unrelated": "not returned",
                },
            )
        ]
    )
    monkeypatch.setattr(
        "medallion_fabric.fabric_notebook_hook.time.sleep", lambda _: None
    )
    result = hook(session).wait_for_completion(
        FabricJobSubmission(JOB, LOCATION, "req", 0), 10
    )
    assert result["monitoring"] == {
        "startTimeUtc": "2026-08-03T20:00:00Z",
        "endTimeUtc": "2026-08-03T20:01:00Z",
        "sparkApplicationId": "application_123",
        "exitValue": {"driver_memory": "56g", "token": "[REDACTED]"},
    }


@pytest.mark.parametrize("status", ["Failed", "Cancelled", "Deduped"])
def test_terminal_failure_states_raise(monkeypatch, status):
    session = Session(gets=[Response(200, payload={"status": status})])
    monkeypatch.setattr(
        "medallion_fabric.fabric_notebook_hook.time.sleep", lambda _: None
    )
    with pytest.raises(AirflowException, match=status.upper()):
        hook(session).wait_for_completion(
            FabricJobSubmission(JOB, LOCATION, None, 0), 10
        )


def test_terminal_failure_includes_sanitized_details(monkeypatch):
    session = Session(
        gets=[
            Response(
                200,
                payload={
                    "status": "Failed",
                    "failureReason": "Spark failed\r\nAuthorization: bearer-value",
                    "exitValue": {"code": 42, "token": "private"},
                },
            )
        ]
    )
    monkeypatch.setattr(
        "medallion_fabric.fabric_notebook_hook.time.sleep", lambda _: None
    )
    with pytest.raises(AirflowException) as raised:
        hook(session).wait_for_completion(
            FabricJobSubmission(JOB, LOCATION, None, 0), 10
        )
    message = str(raised.value)
    assert "failureReason: Spark failed Authorization: [REDACTED]" in message
    assert 'exitValue: {"code": 42, "token": "[REDACTED]"}' in message
    assert "\r" not in message and "\n" not in message
    assert "bearer-value" not in message and "private" not in message


def test_timeout_before_poll(monkeypatch):
    session = Session()
    ticks = iter([0, 9])
    monkeypatch.setattr(
        "medallion_fabric.fabric_notebook_hook.time.monotonic", lambda: next(ticks)
    )
    with pytest.raises(AirflowException, match="timed out"):
        hook(session).wait_for_completion(
            FabricJobSubmission(JOB, LOCATION, None, 2), 10
        )
    assert not session.get_calls


def test_cancel_requires_202_and_uses_cancel_endpoint():
    session = Session(post=Response(202))
    hook(session).cancel(WORKSPACE, NOTEBOOK, JOB)
    assert session.posts[0][0].endswith(
        f"/items/{NOTEBOOK}/jobs/instances/{JOB}/cancel"
    )


def test_fabric_token_uses_fabric_audience(monkeypatch):
    captured = {}

    class App:
        def __init__(self, *args, **kwargs):
            pass

        def acquire_token_for_client(self, scopes):
            captured["scopes"] = scopes
            return {"access_token": "token"}

    connection = SimpleNamespace(
        host="https://api.fabric.microsoft.com",
        login="client",
        password="secret",
        extra_dejson={"tenantId": "tenant"},
    )
    fabric_hook = FabricNotebookHook()
    monkeypatch.setattr(fabric_hook, "get_connection", lambda _: connection)
    monkeypatch.setattr(
        "medallion_fabric.fabric_notebook_hook.msal.ConfidentialClientApplication",
        App,
    )
    assert fabric_hook._token() == "token"
    assert captured["scopes"] == [FABRIC_API_SCOPE]
