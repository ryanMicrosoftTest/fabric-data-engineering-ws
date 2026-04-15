"""Fabric REST API client for OneLake Data Access Roles.

Handles HTTP transport, ETag concurrency, 429 retry with backoff.
Business logic (reconciliation, merging) lives elsewhere — this module
only knows how to GET and PUT roles.
"""

from __future__ import annotations

import time
import requests
from typing import Optional


class PreconditionFailedError(Exception):
    """Raised when a PUT fails with 412 — the ETag is stale.

    The caller (workflow service) should re-fetch, re-reconcile, and retry.
    """
    pass


class OneLakeSecurityClient:
    """REST client for the Fabric Data Access Roles API.

    Args:
        api_token: Bearer token for authentication.
        base_url: Fabric API base URL (override for testing).
        max_retries: Max retry attempts for 429 responses.
        default_retry_delay: Fallback delay if Retry-After header is missing.
    """

    def __init__(
        self,
        api_token: str,
        base_url: str = "https://api.fabric.microsoft.com/v1",
        max_retries: int = 3,
        default_retry_delay: int = 30,
    ):
        if not api_token:
            raise ValueError("api_token is required")

        self.api_token = api_token
        self.base_url = base_url
        self.max_retries = max_retries
        self.default_retry_delay = default_retry_delay

    def list_roles(
        self, workspace_id: str, item_id: str
    ) -> tuple[list[dict], Optional[str]]:
        """GET current data access roles and their ETag.

        Returns:
            Tuple of (roles list, etag string or None).
        """
        url = self._roles_url(workspace_id, item_id)
        resp = self._get_with_retry(url)
        etag = resp.headers.get("ETag")
        roles = resp.json().get("value", [])
        return roles, etag

    def put_roles(
        self,
        workspace_id: str,
        item_id: str,
        roles: list[dict],
        etag: Optional[str] = None,
    ) -> Optional[str]:
        """PUT the full set of roles (replaces all existing roles).

        Args:
            workspace_id: Target workspace.
            item_id: Target lakehouse.
            roles: Complete role list to write.
            etag: ETag from previous GET for optimistic concurrency.

        Returns:
            New ETag from the response.

        Raises:
            PreconditionFailedError: If the ETag is stale (412).
        """
        url = self._roles_url(workspace_id, item_id)
        headers = self._auth_headers()
        if etag:
            headers["If-Match"] = etag

        body = {"value": roles}
        resp = self._put_with_retry(url, headers, body)

        if resp.status_code == 412:
            raise PreconditionFailedError(
                f"ETag mismatch — roles were modified concurrently. "
                f"Re-fetch and retry."
            )

        if resp.status_code >= 400:
            resp.raise_for_status()

        return resp.headers.get("ETag")

    def put_roles_dry_run(
        self, workspace_id: str, item_id: str, roles: list[dict]
    ) -> bool:
        """Validate roles without applying (dryRun=true).

        Returns:
            True if validation passed, False otherwise.
        """
        url = self._roles_url(workspace_id, item_id) + "?dryRun=true"
        headers = self._auth_headers()
        body = {"value": roles}

        resp = requests.put(url, headers=headers, json=body)
        return resp.status_code == 200

    # --- Private helpers ---

    def _roles_url(self, workspace_id: str, item_id: str) -> str:
        return (
            f"{self.base_url}/workspaces/{workspace_id}"
            f"/items/{item_id}/dataAccessRoles"
        )

    def _auth_headers(self) -> dict:
        return {
            "Authorization": f"Bearer {self.api_token}",
            "Content-Type": "application/json",
        }

    def _get_with_retry(self, url: str) -> requests.Response:
        """GET with 429 retry logic."""
        headers = self._auth_headers()
        for attempt in range(self.max_retries + 1):
            resp = requests.get(url, headers=headers)
            if resp.status_code != 429:
                return resp
            if attempt < self.max_retries:
                delay = int(resp.headers.get("Retry-After", self.default_retry_delay))
                time.sleep(delay)

        raise Exception(
            f"GET {url} failed: 429 Too Many Requests after "
            f"{self.max_retries} retries"
        )

    def _put_with_retry(
        self, url: str, headers: dict, body: dict
    ) -> requests.Response:
        """PUT with 429 retry logic (does NOT retry 412 — that's the caller's job)."""
        for attempt in range(self.max_retries + 1):
            resp = requests.put(url, headers=headers, json=body)
            if resp.status_code != 429:
                return resp
            if attempt < self.max_retries:
                delay = int(resp.headers.get("Retry-After", self.default_retry_delay))
                time.sleep(delay)

        raise Exception(
            f"PUT {url} failed: 429 Too Many Requests after "
            f"{self.max_retries} retries"
        )
