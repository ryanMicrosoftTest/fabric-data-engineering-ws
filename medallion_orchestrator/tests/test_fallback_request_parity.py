from __future__ import annotations

from fabric_notebook_fallback import (
    ComputeConfiguration as FallbackComputeConfiguration,
)
from fabric_notebook_fallback import FabricNotebookHook as FallbackHook
from medallion_fabric.compute_config import (
    ComputeConfiguration as PackageComputeConfiguration,
)
from medallion_fabric.fabric_notebook_hook import FabricNotebookHook as PackageHook

WORKSPACE = "896bca78-0d4a-4dc9-8716-05539ecf4ea5"
NOTEBOOK = "d877f06a-d72a-469a-8c82-4ed918b0686b"
JOB = "2d6aa964-5f3a-4c95-a878-cc761ae71391"
LOCATION = (
    f"https://api.fabric.microsoft.com/v1/workspaces/{WORKSPACE}/items/{NOTEBOOK}"
    f"/jobs/instances/{JOB}"
)


class Response:
    def __init__(self, status_code, headers=None):
        self.status_code = status_code
        self.headers = headers or {}


class Session:
    def __init__(self, response):
        self.response = response
        self.posts = []

    def post(self, url, **kwargs):
        self.posts.append((url, kwargs))
        return self.response


def configured_hook(hook_type, session):
    hook = hook_type(session=session, request_timeout=17)
    hook._access_token = "test-token"
    hook._connection_settings = lambda: (
        "https://api.fabric.microsoft.com",
        "tenant",
        "client",
        "secret",
    )
    return hook


def test_fallback_submission_request_matches_package():
    parameters = [{"name": "fullLoad", "value": True, "type": "Boolean"}]
    response_headers = {
        "Location": LOCATION,
        "Retry-After": "3",
        "x-ms-request-id": "request-id",
    }
    package_session = Session(Response(202, response_headers))
    fallback_session = Session(Response(202, response_headers))

    package_submission = configured_hook(PackageHook, package_session).submit_notebook(
        WORKSPACE,
        NOTEBOOK,
        PackageComputeConfiguration("56g", 8, "56g", 8, 2),
        parameters,
    )
    fallback_submission = configured_hook(FallbackHook, fallback_session).submit_notebook(
        WORKSPACE,
        NOTEBOOK,
        FallbackComputeConfiguration("56g", 8, "56g", 8, 2),
        parameters,
    )

    assert fallback_session.posts == package_session.posts
    assert fallback_submission.job_instance_id == package_submission.job_instance_id
    assert fallback_submission.request_id == package_submission.request_id
    assert fallback_submission.retry_after == package_submission.retry_after


def test_fallback_cancellation_request_matches_package():
    package_session = Session(Response(202))
    fallback_session = Session(Response(202))

    configured_hook(PackageHook, package_session).cancel(WORKSPACE, NOTEBOOK, JOB)
    configured_hook(FallbackHook, fallback_session).cancel(WORKSPACE, NOTEBOOK, JOB)

    assert fallback_session.posts == package_session.posts
