"""Run a Fabric Data Pipeline by name and poll until it reaches a terminal status.

Usage (from Pipeline B):
    python run_fabric_pipeline.py \
        --workspace-id <GUID> \
        --pipeline-name onelake_security_role_pl \
        --poll-interval 30 \
        --timeout 3600

Environment variables required:
    AZURE_TENANT_ID      - Azure AD tenant ID
    AZURE_CLIENT_ID      - Service principal client ID
    AZURE_CLIENT_SECRET  - Service principal client secret
"""

import argparse
import logging
import os
import sys
import time

import msal
import requests

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

FABRIC_API = "https://api.fabric.microsoft.com/v1"


def get_access_token():
    """Acquire an access token using MSAL client credentials."""
    tenant_id = os.environ["AZURE_TENANT_ID"]
    client_id = os.environ["AZURE_CLIENT_ID"]
    client_secret = os.environ["AZURE_CLIENT_SECRET"]

    authority = f"https://login.microsoftonline.com/{tenant_id}"
    app = msal.ConfidentialClientApplication(
        client_id, authority=authority, client_credential=client_secret
    )
    result = app.acquire_token_for_client(
        scopes=["https://analysis.windows.net/powerbi/api/.default"]
    )
    if "access_token" not in result:
        raise RuntimeError(f"Token acquisition failed: {result.get('error_description', result)}")
    return result["access_token"]


def find_pipeline_item(workspace_id, pipeline_name, token):
    """List items in the workspace and find the pipeline by display name."""
    url = f"{FABRIC_API}/workspaces/{workspace_id}/items?type=DataPipeline"
    headers = {"Authorization": f"Bearer {token}"}
    items = []
    while url:
        resp = requests.get(url, headers=headers, timeout=60)
        resp.raise_for_status()
        body = resp.json()
        items.extend(body.get("value", []))
        url = body.get("continuationUri")

    for item in items:
        if item.get("displayName") == pipeline_name:
            return item["id"]

    available = [i.get("displayName") for i in items]
    raise RuntimeError(
        f"Pipeline '{pipeline_name}' not found in workspace {workspace_id}. "
        f"Available pipelines: {available}"
    )


def run_pipeline(workspace_id, item_id, token):
    """Start an on-demand pipeline job and return the job status URL."""
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }

    item_url = f"{FABRIC_API}/workspaces/{workspace_id}/items/{item_id}"
    item_resp = requests.get(item_url, headers=headers, timeout=60)
    item_resp.raise_for_status()
    item_body = item_resp.json()
    pipeline_name = item_body.get("displayName")
    if not pipeline_name:
        raise RuntimeError(
            f"Pipeline item {item_id} in workspace {workspace_id} did not include a displayName"
        )

    url = f"{FABRIC_API}/workspaces/{workspace_id}/items/{item_id}/jobs/instances?jobType=Pipeline"
    payload = {"executionData": {"pipelineName": pipeline_name}}
    resp = requests.post(url, headers=headers, json=payload, timeout=120)
    resp.raise_for_status()

    # The Location header contains the URL to poll for status
    location = resp.headers.get("Location", "").strip()
    if not location:
        response_text = resp.text.strip()
        raise RuntimeError(
            "Pipeline job started but response did not include a Location header for polling. "
            f"Status: {resp.status_code}. Response body: {response_text}"
        )
    logger.info("Pipeline job started. Location: %s", location)
    logger.info("Response status: %s", resp.status_code)
    return location


# Transient HTTP statuses returned by the Fabric job-status endpoint that
# should be retried instead of failing the entire pipeline run.
_RETRYABLE_STATUSES = {408, 429, 500, 502, 503, 504}
_MAX_CONSECUTIVE_TRANSIENT_ERRORS = 10


def poll_job(location_url, token, poll_interval, timeout):
    """Poll the job instance until a terminal status is reached.

    The Fabric ``jobs/instances/{id}`` endpoint intermittently returns 5xx
    responses while a job is still running. Those are treated as transient
    and retried; only after several consecutive transient failures (or the
    overall timeout) do we give up.
    """
    headers = {"Authorization": f"Bearer {token}"}
    start = time.time()
    consecutive_transient_errors = 0
    last_transient_error = None

    while True:
        elapsed = time.time() - start
        if elapsed > timeout:
            raise TimeoutError(f"Pipeline job did not complete within {timeout}s")

        try:
            resp = requests.get(location_url, headers=headers, timeout=60)
        except requests.RequestException as exc:
            consecutive_transient_errors += 1
            last_transient_error = exc
            logger.warning(
                "Transient network error polling job status (%d/%d): %s",
                consecutive_transient_errors,
                _MAX_CONSECUTIVE_TRANSIENT_ERRORS,
                exc,
            )
            if consecutive_transient_errors >= _MAX_CONSECUTIVE_TRANSIENT_ERRORS:
                raise RuntimeError(
                    f"Job status polling failed after {consecutive_transient_errors} "
                    f"consecutive network errors. Last error: {exc}"
                ) from exc
            time.sleep(poll_interval)
            continue

        if resp.status_code in _RETRYABLE_STATUSES:
            consecutive_transient_errors += 1
            request_id = (
                resp.headers.get("x-ms-request-id")
                or resp.headers.get("RequestId")
                or "<none>"
            )
            body_snippet = resp.text[:500].replace("\n", " ")
            last_transient_error = (
                f"HTTP {resp.status_code} (x-ms-request-id={request_id}): {body_snippet}"
            )
            logger.warning(
                "Transient HTTP %s polling job status (%d/%d). x-ms-request-id=%s body=%s",
                resp.status_code,
                consecutive_transient_errors,
                _MAX_CONSECUTIVE_TRANSIENT_ERRORS,
                request_id,
                body_snippet,
            )
            if consecutive_transient_errors >= _MAX_CONSECUTIVE_TRANSIENT_ERRORS:
                raise RuntimeError(
                    f"Job status polling failed after {consecutive_transient_errors} "
                    f"consecutive transient HTTP errors. Last error: {last_transient_error}"
                )
            time.sleep(poll_interval)
            continue

        # Any other non-2xx is treated as a hard failure (e.g. 401, 403, 404).
        resp.raise_for_status()

        consecutive_transient_errors = 0
        last_transient_error = None
        body = resp.json()

        status = body.get("status", "Unknown")
        logger.info("Job status: %s (elapsed %.0fs)", status, elapsed)

        if status in ("Completed", "Failed", "Cancelled", "Deduped"):
            return body

        time.sleep(poll_interval)


def main():
    parser = argparse.ArgumentParser(description="Run a Fabric pipeline and poll for completion")
    parser.add_argument("--workspace-id", required=True, help="Fabric workspace ID")
    parser.add_argument("--pipeline-name", required=True, help="Display name of the pipeline")
    parser.add_argument("--poll-interval", type=int, default=30, help="Seconds between polls")
    parser.add_argument("--timeout", type=int, default=3600, help="Max seconds to wait")
    args = parser.parse_args()

    token = get_access_token()
    logger.info("Acquired access token.")

    item_id = find_pipeline_item(args.workspace_id, args.pipeline_name, token)
    logger.info("Found pipeline '%s' -> item ID %s", args.pipeline_name, item_id)

    location = run_pipeline(args.workspace_id, item_id, token)
    if not location:
        logger.error("No Location header returned — cannot poll for job status.")
        sys.exit(1)

    result = poll_job(location, token, args.poll_interval, args.timeout)
    status = result.get("status", "Unknown")
    logger.info("Terminal status: %s", status)
    logger.info("Full result: %s", result)

    if status != "Completed":
        logger.error("Pipeline run did not succeed. Status: %s", status)
        sys.exit(1)

    logger.info("Pipeline run succeeded.")


if __name__ == "__main__":
    main()
