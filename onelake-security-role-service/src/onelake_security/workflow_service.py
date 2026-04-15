"""Workflow service — orchestration layer for OneLake security role management.

Coordinates between the YAML parser, reconcilers, and API client.
Handles the 412 ETag retry loop: on PreconditionFailedError, re-fetch
current roles, re-reconcile against updated state, and retry the PUT.

This is the entry point that notebooks call.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional

from onelake_security.api_client import OneLakeSecurityClient, PreconditionFailedError
from onelake_security.models import RoleDefinition, UserMapping
from onelake_security.role_definition_reconciler import reconcile_role_definitions
from onelake_security.role_membership_reconciler import reconcile_role_membership


@dataclass
class RoleWorkflowResult:
    """Outcome of processing role definitions."""

    success: bool
    roles_applied: int = 0
    error: Optional[str] = None


@dataclass
class MappingWorkflowResult:
    """Outcome of processing user mappings."""

    success: bool
    mappings_applied: int = 0
    error: Optional[str] = None


def process_role_definitions(
    client: OneLakeSecurityClient,
    workspace_id: str,
    item_id: str,
    role_definitions: list[RoleDefinition],
    validate_first: bool = False,
    max_concurrency_retries: int = 3,
) -> RoleWorkflowResult:
    """Apply role definitions to a lakehouse with ETag concurrency control.

    Flow:
      1. GET existing roles + ETag
      2. Reconcile all role definitions against current state
      3. (Optional) Dry-run validation
      4. PUT with If-Match ETag
      5. On 412, re-fetch and retry (up to max_concurrency_retries)

    Args:
        client: Authenticated API client.
        workspace_id: Target workspace GUID.
        item_id: Target lakehouse GUID.
        role_definitions: Parsed role definitions from YAML files.
        validate_first: If True, run dry-run validation before applying.
        max_concurrency_retries: Max 412 retry attempts.

    Returns:
        RoleWorkflowResult with success status and details.
    """
    if not role_definitions:
        return RoleWorkflowResult(success=True, roles_applied=0)

    try:
        for attempt in range(max_concurrency_retries + 1):
            # Step 1: Fetch current state
            existing_roles, etag = client.list_roles(workspace_id, item_id)

            # Step 2: Reconcile
            new_roles = reconcile_role_definitions(existing_roles, role_definitions)

            # Step 3: Optional dry-run validation
            if validate_first and attempt == 0:
                is_valid = client.put_roles_dry_run(workspace_id, item_id, new_roles)
                if not is_valid:
                    return RoleWorkflowResult(
                        success=False,
                        error="Dry-run validation failed — roles not applied",
                    )

            # Step 4: PUT with ETag
            try:
                client.put_roles(workspace_id, item_id, new_roles, etag=etag)
                return RoleWorkflowResult(
                    success=True,
                    roles_applied=len(role_definitions),
                )
            except PreconditionFailedError:
                if attempt >= max_concurrency_retries:
                    return RoleWorkflowResult(
                        success=False,
                        roles_applied=0,
                        error=(
                            f"Concurrency conflict: ETag mismatch after "
                            f"{max_concurrency_retries} retries. "
                            f"Roles were modified concurrently."
                        ),
                    )
                # Retry: loop back to re-fetch

    except Exception as e:
        return RoleWorkflowResult(
            success=False,
            roles_applied=0,
            error=str(e),
        )

    # Should not reach here, but safety net
    return RoleWorkflowResult(success=False, error="Unexpected workflow exit")


def process_user_mappings(
    client: OneLakeSecurityClient,
    workspace_id: str,
    item_id: str,
    tenant_id: str,
    user_mappings: list[UserMapping],
    max_concurrency_retries: int = 3,
) -> MappingWorkflowResult:
    """Apply user-role mappings to a lakehouse with ETag concurrency control.

    Each mapping is applied sequentially. After each mapping, the local
    role state is updated so subsequent mappings see the latest members.

    Flow:
      1. GET existing roles + ETag
      2. For each mapping, reconcile members into the role list
      3. PUT with If-Match ETag
      4. On 412, re-fetch and retry

    Args:
        client: Authenticated API client.
        workspace_id: Target workspace GUID.
        item_id: Target lakehouse GUID.
        tenant_id: Entra tenant ID for member stamping.
        user_mappings: Parsed user mappings from YAML files.
        max_concurrency_retries: Max 412 retry attempts.

    Returns:
        MappingWorkflowResult with success status and details.
    """
    if not user_mappings:
        return MappingWorkflowResult(success=True, mappings_applied=0)

    try:
        for attempt in range(max_concurrency_retries + 1):
            # Step 1: Fetch current state
            existing_roles, etag = client.list_roles(workspace_id, item_id)

            # Step 2: Reconcile all mappings sequentially
            current_roles = existing_roles
            try:
                for mapping in user_mappings:
                    current_roles = reconcile_role_membership(
                        current_roles, mapping, tenant_id
                    )
            except ValueError as e:
                return MappingWorkflowResult(
                    success=False,
                    mappings_applied=0,
                    error=str(e),
                )

            # Step 3: PUT with ETag
            try:
                client.put_roles(workspace_id, item_id, current_roles, etag=etag)
                return MappingWorkflowResult(
                    success=True,
                    mappings_applied=len(user_mappings),
                )
            except PreconditionFailedError:
                if attempt >= max_concurrency_retries:
                    return MappingWorkflowResult(
                        success=False,
                        mappings_applied=0,
                        error=(
                            f"Concurrency conflict: ETag mismatch after "
                            f"{max_concurrency_retries} retries."
                        ),
                    )

    except Exception as e:
        return MappingWorkflowResult(
            success=False,
            mappings_applied=0,
            error=str(e),
        )

    return MappingWorkflowResult(success=False, error="Unexpected workflow exit")
