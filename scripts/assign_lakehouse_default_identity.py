"""Assign a service principal as the default identity of a Microsoft Fabric lakehouse.

The Fabric API assigns the identity of the *caller*, so this script must
authenticate as the service principal itself (client credentials flow).
A user-delegated token would silently assign the wrong identity.

Required environment variables:
    FABRIC_TENANT_ID
    FABRIC_CLIENT_ID
    FABRIC_CLIENT_SECRET
    FABRIC_WORKSPACE_ID
    FABRIC_LAKEHOUSE_ITEM_ID
"""

from __future__ import annotations

import json
import os
import sys
import time

import requests

FABRIC_BASE = "https://api.fabric.microsoft.com/v1"
SCOPE = "https://api.fabric.microsoft.com/.default"
POLL_TIMEOUT_SECONDS = 300
INITIAL_RETRY_AFTER = 5
MAX_RETRY_AFTER = 30
REQUEST_TIMEOUT = 60


class ConfigurationError(RuntimeError):
    """Raised for 401/403 and missing configuration - retrying will not help."""


class FabricApiError(RuntimeError):
    pass


def _require_env() -> dict[str, str]:
    names = (
        "FABRIC_TENANT_ID",
        "FABRIC_CLIENT_ID",
        "FABRIC_CLIENT_SECRET",
        "FABRIC_WORKSPACE_ID",
        "FABRIC_LAKEHOUSE_ITEM_ID",
    )
    values = {n: os.environ.get(n, "").strip() for n in names}
    missing = [n for n, v in values.items() if not v]
    if missing:
        raise ConfigurationError(
            "Missing required environment variables: " + ", ".join(missing)
        )
    return values


def _fail_on_error(response: requests.Response, context: str) -> None:
    if response.status_code in (401, 403):
        raise ConfigurationError(
            f"{context} returned {response.status_code}. This is a configuration "
            "problem, not a transient one - check that the Fabric tenant setting "
            "allowing service principals to call Fabric APIs is enabled and that "
            "the service principal is in the security group scoped by that setting, "
            "and that it has Contributor/Admin rights on the workspace.\n"
            f"Response body: {response.text}"
        )
    if not response.ok:
        raise FabricApiError(
            f"{context} failed with HTTP {response.status_code}\n"
            f"Response body: {response.text}"
        )


def get_service_principal_token(tenant_id: str, client_id: str, client_secret: str) -> str:
    url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"
    response = requests.post(
        url,
        data={
            "grant_type": "client_credentials",
            "client_id": client_id,
            "client_secret": client_secret,
            "scope": SCOPE,
        },
        timeout=REQUEST_TIMEOUT,
    )
    if not response.ok:
        # Never echo the secret; the AAD error body does not contain it.
        raise ConfigurationError(
            f"Token acquisition failed with HTTP {response.status_code}\n"
            f"Response body: {response.text}"
        )
    token = response.json().get("access_token")
    if not token:
        raise ConfigurationError("Token endpoint returned no access_token.")
    return token


def _headers(token: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}


def assign_default_identity(token: str, workspace_id: str, item_id: str) -> requests.Response:
    # Use the generic /items/ form; the item-type form (/lakehouses/) is a
    # documented known issue and may error.
    url = (
        f"{FABRIC_BASE}/workspaces/{workspace_id}/items/{item_id}"
        "/identities/default/assign?beta=true"
    )
    response = requests.post(
        url,
        headers=_headers(token),
        json={"assignmentType": "Caller"},
        timeout=REQUEST_TIMEOUT,
    )
    if (
        response.status_code == 400
        and "already the identity" in response.text.lower()
    ):
        print("Item already has this service principal as its identity; nothing to do.")
        return response
    _fail_on_error(response, "Assign default identity")
    return response


def fetch_operation_result(token: str, location: str) -> dict:
    """The terminal LRO status body may omit assignmentStatus; it lives on /result."""
    response = requests.get(
        location.rstrip("/") + "/result", headers=_headers(token), timeout=REQUEST_TIMEOUT
    )
    if response.ok and response.content:
        try:
            return response.json()
        except ValueError:
            return {}
    return {}


def poll_operation(token: str, location: str, retry_after: float) -> dict:
    deadline = time.monotonic() + POLL_TIMEOUT_SECONDS
    delay = max(retry_after or INITIAL_RETRY_AFTER, 1.0)

    while True:
        if time.monotonic() >= deadline:
            raise FabricApiError(
                f"Long-running operation did not complete within {POLL_TIMEOUT_SECONDS}s: {location}"
            )
        sleep_for = min(delay, max(deadline - time.monotonic(), 0))
        print(f"  waiting {sleep_for:.0f}s before polling operation...", flush=True)
        time.sleep(sleep_for)

        response = requests.get(location, headers=_headers(token), timeout=REQUEST_TIMEOUT)
        _fail_on_error(response, "Poll long-running operation")

        body = response.json() if response.content else {}
        status = (body.get("status") or "").lower()
        print(f"  operation status: {body.get('status') or '(none)'}", flush=True)

        if status in ("succeeded", "completed"):
            result = fetch_operation_result(token, location)
            if result:
                body = {**body, **result} if isinstance(result, dict) else body
            return body
        if status == "failed":
            raise FabricApiError(
                "Identity assignment operation failed.\n"
                + json.dumps(body, indent=2)
            )

        server_retry = response.headers.get("Retry-After")
        delay = float(server_retry) if server_retry else min(delay * 2, MAX_RETRY_AFTER)


def report_assignment_status(result: dict) -> None:
    statuses = result.get("assignmentStatus")
    if statuses is None and isinstance(result.get("result"), dict):
        statuses = result["result"].get("assignmentStatus")

    if not statuses:
        print("Warning: operation completed but returned no assignmentStatus array.")
        print(json.dumps(result, indent=2))
        return

    failures = []
    for entry in statuses:
        item = entry.get("itemId") or entry.get("id") or "(unknown item)"
        item_type = entry.get("itemType") or entry.get("type") or ""
        status = entry.get("status")
        print(f"  {status:<12} {item_type:<12} {item}")
        if str(status).lower() != "succeeded":
            failures.append(entry)

    if failures:
        raise FabricApiError(
            "One or more item assignments did not succeed:\n"
            + json.dumps(failures, indent=2)
        )


def _confirm_via_reassign(token: str, workspace_id: str, item_id: str) -> None:
    """Authoritative fallback when the read API does not expose defaultIdentity.

    Re-issuing the assign as the same service principal returns
    400 InvalidInput "The requesting user is already the identity of the artifact."
    only when the caller IS already the default identity. Anything else means the
    identity is not ours - fail loudly.
    """
    url = (
        f"{FABRIC_BASE}/workspaces/{workspace_id}/items/{item_id}"
        "/identities/default/assign?beta=true"
    )
    response = requests.post(
        url,
        headers=_headers(token),
        json={"assignmentType": "Caller"},
        timeout=REQUEST_TIMEOUT,
    )
    already = (
        response.status_code == 400
        and "already the identity" in response.text.lower()
    )
    if already:
        print(
            "Verified (via idempotency probe): the API reports this service principal "
            "is already the identity of the item."
        )
        return
    if response.status_code in (401, 403):
        _fail_on_error(response, "Verification probe")
    raise FabricApiError(
        "Verification FAILED: re-issuing the assignment did not report that this "
        "service principal is already the item identity, so the default identity is "
        "NOT the expected service principal.\n"
        f"HTTP {response.status_code}\nResponse body: {response.text}"
    )


def verify_identity(token: str, workspace_id: str, item_id: str, client_id: str) -> None:
    url = (
        f"{FABRIC_BASE}/workspaces/{workspace_id}/items/{item_id}"
        "?beta=true&$expand=DefaultIdentity"
    )
    response = requests.get(url, headers=_headers(token), timeout=REQUEST_TIMEOUT)
    _fail_on_error(response, "Verify default identity")
    body = response.json() if response.content else {}

    identity = body.get("defaultIdentity")
    if not identity:
        # This tenant's read API omits defaultIdentity entirely (the $expand is
        # silently ignored), so absence proves nothing. Use the probe instead.
        print(
            "Read API did not return defaultIdentity ($expand not honored here); "
            "confirming through the assignment API instead."
        )
        _confirm_via_reassign(token, workspace_id, item_id)
        return

    print(json.dumps(identity, indent=2))
    identity_type = identity.get("type")
    app_id = (identity.get("servicePrincipalDetails") or {}).get("aadAppId")

    if identity_type != "ServicePrincipal":
        raise FabricApiError(
            f"Verification FAILED: defaultIdentity.type is '{identity_type}', expected "
            "'ServicePrincipal'. The assignment used the wrong caller identity - "
            "make sure the token came from the client credentials flow, not a user login."
        )
    if (app_id or "").lower() != client_id.lower():
        raise FabricApiError(
            f"Verification FAILED: servicePrincipalDetails.aadAppId '{app_id}' does not "
            f"match FABRIC_CLIENT_ID '{client_id}'."
        )
    print("Verified: default identity is the expected service principal.")


def discover(token: str, workspace_id: str) -> None:
    """List workspaces, or lakehouses in a workspace, to help find the IDs."""
    if not workspace_id:
        response = requests.get(
            f"{FABRIC_BASE}/workspaces", headers=_headers(token), timeout=REQUEST_TIMEOUT
        )
        _fail_on_error(response, "List workspaces")
        for ws in response.json().get("value", []):
            print(f"  {ws.get('id')}  {ws.get('displayName')}")
        return

    response = requests.get(
        f"{FABRIC_BASE}/workspaces/{workspace_id}/items?type=Lakehouse",
        headers=_headers(token),
        timeout=REQUEST_TIMEOUT,
    )
    _fail_on_error(response, "List lakehouses")
    for item in response.json().get("value", []):
        print(f"  {item.get('id')}  {item.get('displayName')}")


def main() -> int:
    discover_only = "--discover" in sys.argv
    try:
        if discover_only:
            for name in ("FABRIC_TENANT_ID", "FABRIC_CLIENT_ID", "FABRIC_CLIENT_SECRET"):
                if not os.environ.get(name, "").strip():
                    raise ConfigurationError(f"Missing required environment variable: {name}")
            token = get_service_principal_token(
                os.environ["FABRIC_TENANT_ID"],
                os.environ["FABRIC_CLIENT_ID"],
                os.environ["FABRIC_CLIENT_SECRET"],
            )
            workspace_id = os.environ.get("FABRIC_WORKSPACE_ID", "").strip()
            print("Lakehouses:" if workspace_id else "Workspaces:")
            discover(token, workspace_id)
            return 0

        cfg = _require_env()

        print("Acquiring service principal token (client credentials)...")
        token = get_service_principal_token(
            cfg["FABRIC_TENANT_ID"], cfg["FABRIC_CLIENT_ID"], cfg["FABRIC_CLIENT_SECRET"]
        )

        print(
            f"Assigning default identity on item {cfg['FABRIC_LAKEHOUSE_ITEM_ID']} "
            f"in workspace {cfg['FABRIC_WORKSPACE_ID']}..."
        )
        response = assign_default_identity(
            token, cfg["FABRIC_WORKSPACE_ID"], cfg["FABRIC_LAKEHOUSE_ITEM_ID"]
        )
        print(f"HTTP {response.status_code}")

        if response.status_code == 202:
            location = response.headers.get("Location")
            if not location:
                raise FabricApiError("202 Accepted returned without a Location header.")
            retry_after = float(response.headers.get("Retry-After") or INITIAL_RETRY_AFTER)
            result = poll_operation(token, location, retry_after)
            print("Assignment results:")
            report_assignment_status(result)
        elif response.status_code == 400:
            pass  # already assigned; verification below still confirms it
        else:
            body = response.json() if response.content else {}
            if body:
                print("Assignment results:")
                report_assignment_status(body)

        print("Verifying default identity...")
        verify_identity(
            token,
            cfg["FABRIC_WORKSPACE_ID"],
            cfg["FABRIC_LAKEHOUSE_ITEM_ID"],
            cfg["FABRIC_CLIENT_ID"],
        )
        print("Done.")
        return 0

    except ConfigurationError as exc:
        print(f"CONFIGURATION ERROR: {exc}", file=sys.stderr)
        return 2
    except FabricApiError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1
    except requests.RequestException as exc:
        print(f"NETWORK ERROR: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
