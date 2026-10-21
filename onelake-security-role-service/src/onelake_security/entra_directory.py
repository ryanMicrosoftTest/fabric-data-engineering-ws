"""Microsoft Entra directory lookups for audit enrichment.

Resolves Entra object IDs to friendly display names and true object types
(User, Group, ServicePrincipal) via Microsoft Graph.

Uses POST /directoryObjects/getByIds, which resolves mixed object types in
one call and silently omits IDs the caller cannot see or that no longer
exist. Those omissions are retried individually via GET /directoryObjects/{id}
so the caller learns *why* an ID could not be mapped to a friendly name,
rather than being handed a bare GUID labelled "n/a".

Beyond name resolution this module can also:

- expand a security group into the principals inside it (`get_group_members`),
  so an audit shows who effectively holds access — not just the group name
- resolve a display name / UPN back to an object ID (`find_by_name`), which is
  what you want when authoring role YAML by hand
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Iterable, Optional

import requests


# Graph caps getByIds at 1000 ids per request.
MAX_BATCH_SIZE = 1000

# Object types that can contain other principals.
GROUP_TYPES = ("Group",)


class DirectoryLookupError(Exception):
    """Raised when Graph rejects the lookup (e.g. missing permissions)."""


@dataclass(frozen=True)
class DirectoryObject:
    """A resolved Entra directory object."""

    object_id: str
    display_name: Optional[str] = None
    object_type: Optional[str] = None
    user_principal_name: Optional[str] = None
    mail: Optional[str] = None

    @property
    def friendly_name(self) -> str:
        """Best available human-readable label for this object."""
        return self.display_name or self.user_principal_name or self.object_id

    @property
    def is_group(self) -> bool:
        return self.object_type in GROUP_TYPES

    def to_dict(self) -> dict:
        return {
            "object_id": self.object_id,
            "display_name": self.display_name,
            "object_type": self.object_type,
            "user_principal_name": self.user_principal_name,
            "mail": self.mail,
            "friendly_name": self.friendly_name,
        }


@dataclass(frozen=True)
class UnresolvedObject:
    """An object ID that could not be mapped to a friendly name.

    `reason` names the cause — deleted, cross-tenant, or not visible to the
    calling identity — so an audit report can say more than "n/a".
    """

    object_id: str
    reason: str

    def to_dict(self) -> dict:
        return {"object_id": self.object_id, "reason": self.reason}


@dataclass
class DirectoryResolution:
    """Outcome of resolving a batch of object IDs."""

    resolved: dict[str, DirectoryObject] = field(default_factory=dict)
    unresolved: list[UnresolvedObject] = field(default_factory=list)

    @property
    def unresolved_ids(self) -> list[str]:
        return [u.object_id for u in self.unresolved]

    def to_dict(self) -> dict:
        return {
            "resolved": {k: v.to_dict() for k, v in self.resolved.items()},
            "unresolved": [u.to_dict() for u in self.unresolved],
        }


class EntraDirectoryClient:
    """Resolves Entra object IDs to display names via Microsoft Graph.

    Requires an application token with directory read permissions
    (e.g. `Directory.Read.All`, or `User.Read.All` + `Group.Read.All`).
    Group expansion additionally needs `GroupMember.Read.All`.

    Args:
        graph_token: Bearer token for https://graph.microsoft.com/.default
        base_url: Graph API base URL (override for testing).
        max_retries: Max retry attempts for 429 responses.
        default_retry_delay: Fallback delay if Retry-After is missing.
        types: Optional Graph type filter (e.g. ["user", "group"]). Left
            unset, Graph resolves every directory object type — including
            app registrations, which a narrower filter would silently drop.
        cache: Reuse resolved objects across calls. An audit routinely hits
            the same group from many roles, so this collapses repeat lookups.
    """

    def __init__(
        self,
        graph_token: str,
        base_url: str = "https://graph.microsoft.com/v1.0",
        max_retries: int = 3,
        default_retry_delay: int = 10,
        types: Optional[list[str]] = None,
        cache: bool = True,
    ):
        if not graph_token:
            raise ValueError("graph_token is required")

        self.graph_token = graph_token
        self.base_url = base_url
        self.max_retries = max_retries
        self.default_retry_delay = default_retry_delay
        self.types = types
        self.cache_enabled = cache
        self._cache: dict[str, DirectoryObject] = {}
        self._group_member_cache: dict[tuple[str, bool], list[DirectoryObject]] = {}

    def resolve_objects(self, object_ids: Iterable[str]) -> dict[str, DirectoryObject]:
        """Resolve object IDs to DirectoryObjects.

        Args:
            object_ids: Entra object IDs. Duplicates and blanks are ignored.

        Returns:
            Mapping of object_id -> DirectoryObject. IDs that Graph could not
            resolve are omitted from the mapping.

        Raises:
            DirectoryLookupError: If Graph rejects the request (401/403/etc).
        """
        return self.resolve(object_ids).resolved

    def resolve(self, object_ids: Iterable[str]) -> DirectoryResolution:
        """Resolve object IDs, reporting why any of them failed.

        Unlike `resolve_objects`, every ID the bulk call omitted is retried
        via GET /directoryObjects/{id}, so each failure carries a concrete
        reason instead of vanishing.

        Args:
            object_ids: Entra object IDs. Duplicates and blanks are ignored.

        Returns:
            DirectoryResolution holding the resolved map and the failures.

        Raises:
            DirectoryLookupError: If Graph rejects the bulk request.
        """
        unique_ids = sorted({oid for oid in object_ids if oid})
        if not unique_ids:
            return DirectoryResolution()

        resolved: dict[str, DirectoryObject] = {}
        pending: list[str] = []
        for object_id in unique_ids:
            cached = self._cache.get(object_id)
            if cached is not None:
                resolved[object_id] = cached
            else:
                pending.append(object_id)

        for start in range(0, len(pending), MAX_BATCH_SIZE):
            batch = pending[start:start + MAX_BATCH_SIZE]
            for item in self._get_by_ids(batch):
                obj = _to_directory_object(item)
                if obj.object_id:
                    resolved[obj.object_id] = obj
                    self._remember(obj)

        unresolved: list[UnresolvedObject] = []
        for object_id in unique_ids:
            if object_id in resolved:
                continue
            obj, reason = self._get_single(object_id)
            if obj is not None:
                resolved[object_id] = obj
                self._remember(obj)
            else:
                unresolved.append(UnresolvedObject(object_id, reason))

        return DirectoryResolution(resolved=resolved, unresolved=unresolved)

    def resolve_object(self, object_id: str) -> Optional[DirectoryObject]:
        """Resolve a single object ID, or None if it cannot be seen."""
        if not object_id:
            return None
        return self.resolve([object_id]).resolved.get(object_id)

    def get_group_members(
        self,
        group_id: str,
        transitive: bool = True,
    ) -> list[DirectoryObject]:
        """List the principals inside a security group.

        Args:
            group_id: Entra group object ID.
            transitive: Flatten nested groups so the result is the users who
                actually hold the access. When False, only direct members are
                returned and nested groups appear as Group entries.

        Returns:
            Member DirectoryObjects. Empty when the group has no members or
            is not visible to the calling identity.

        Raises:
            DirectoryLookupError: If Graph rejects the request.
        """
        if not group_id:
            return []

        cache_key = (group_id, transitive)
        cached = self._group_member_cache.get(cache_key)
        if cached is not None:
            return list(cached)

        segment = "transitiveMembers" if transitive else "members"
        url: Optional[str] = f"{self.base_url}/groups/{group_id}/{segment}"
        params: dict = {"$top": 999}

        members: list[DirectoryObject] = []
        seen: set[str] = set()
        while url:
            resp = self._request("GET", url, params=params)
            params = {}

            if resp.status_code == 404:
                break
            if resp.status_code >= 400:
                raise DirectoryLookupError(
                    f"Graph group members failed ({resp.status_code}): {resp.text}. "
                    f"The identity needs directory read permissions "
                    f"(Directory.Read.All, or Group.Read.All + GroupMember.Read.All)."
                )

            payload = resp.json()
            for item in payload.get("value", []):
                obj = _to_directory_object(item)
                if obj.object_id and obj.object_id not in seen:
                    seen.add(obj.object_id)
                    members.append(obj)
                    self._remember(obj)

            url = payload.get("@odata.nextLink")

        if self.cache_enabled:
            self._group_member_cache[cache_key] = list(members)
        return members

    def find_by_name(self, name: str, limit: int = 25) -> list[DirectoryObject]:
        """Reverse lookup — resolve a display name or UPN to object IDs.

        Searches users, groups, and service principals. Matching is a prefix
        match on displayName, plus an exact match on userPrincipalName or
        mail for users.

        Args:
            name: Display name, UPN, or mail address to search for.
            limit: Max results per directory collection.

        Returns:
            Matching DirectoryObjects, ordered users -> groups -> SPNs.

        Raises:
            DirectoryLookupError: If Graph rejects the request.
        """
        if not name or not name.strip():
            return []

        escaped = name.strip().replace("'", "''")
        collections = [
            (
                "users",
                f"startswith(displayName,'{escaped}') "
                f"or userPrincipalName eq '{escaped}' "
                f"or mail eq '{escaped}'",
            ),
            ("groups", f"startswith(displayName,'{escaped}')"),
            ("servicePrincipals", f"startswith(displayName,'{escaped}')"),
        ]

        results: list[DirectoryObject] = []
        seen: set[str] = set()
        for collection, filter_expr in collections:
            resp = self._request(
                "GET",
                f"{self.base_url}/{collection}",
                params={"$filter": filter_expr, "$top": limit},
            )

            if resp.status_code >= 400:
                raise DirectoryLookupError(
                    f"Graph {collection} search failed ({resp.status_code}): "
                    f"{resp.text}. The identity needs directory read permissions "
                    f"(Directory.Read.All, or User.Read.All + Group.Read.All)."
                )

            fallback_type = _COLLECTION_TYPES.get(collection)
            for item in resp.json().get("value", []):
                obj = _to_directory_object(item, fallback_type=fallback_type)
                if obj.object_id and obj.object_id not in seen:
                    seen.add(obj.object_id)
                    results.append(obj)
                    self._remember(obj)

        return results

    def clear_cache(self) -> None:
        """Drop every cached lookup."""
        self._cache.clear()
        self._group_member_cache.clear()

    # --- Private helpers ---

    def _remember(self, obj: DirectoryObject) -> None:
        if self.cache_enabled and obj.object_id:
            self._cache[obj.object_id] = obj

    def _get_by_ids(self, ids: list[str]) -> list[dict]:
        body: dict = {"ids": ids}
        if self.types:
            body["types"] = self.types

        resp = self._request(
            "POST",
            f"{self.base_url}/directoryObjects/getByIds",
            json=body,
        )

        if resp.status_code >= 400:
            raise DirectoryLookupError(
                f"Graph getByIds failed ({resp.status_code}): {resp.text}. "
                f"The identity needs directory read permissions "
                f"(Directory.Read.All, or User.Read.All + Group.Read.All)."
            )

        return resp.json().get("value", [])

    def _get_single(self, object_id: str) -> tuple[Optional[DirectoryObject], str]:
        """Retry one omitted ID directly, and explain any failure."""
        try:
            resp = self._request(
                "GET", f"{self.base_url}/directoryObjects/{object_id}"
            )
        except DirectoryLookupError as exc:
            return None, str(exc)

        if resp.status_code == 200:
            obj = _to_directory_object(resp.json())
            return (obj, "") if obj.object_id else (None, UNRESOLVED_MISSING)

        if resp.status_code == 404:
            return None, UNRESOLVED_MISSING
        if resp.status_code in (401, 403):
            return None, UNRESOLVED_FORBIDDEN

        return None, f"Graph returned {resp.status_code} for this object id."

    def _request(self, method: str, url: str, **kwargs) -> requests.Response:
        """Issue a Graph request, retrying on 429."""
        headers = {
            "Authorization": "Bearer " + self.graph_token,
            "Content-Type": "application/json",
        }

        for attempt in range(self.max_retries + 1):
            resp = requests.request(method, url, headers=headers, **kwargs)

            if resp.status_code != 429:
                return resp
            if attempt < self.max_retries:
                delay = int(resp.headers.get("Retry-After", self.default_retry_delay))
                time.sleep(delay)

        raise DirectoryLookupError(
            f"Graph {method} {url} failed: 429 Too Many Requests after "
            f"{self.max_retries} retries"
        )


UNRESOLVED_MISSING = (
    "Object not found — it was deleted, or it lives in another tenant."
)
UNRESOLVED_FORBIDDEN = (
    "Not visible to the calling identity — grant Directory.Read.All "
    "(or User.Read.All + Group.Read.All)."
)

_COLLECTION_TYPES = {
    "users": "User",
    "groups": "Group",
    "servicePrincipals": "ServicePrincipal",
}


def _to_directory_object(
    item: dict,
    fallback_type: Optional[str] = None,
) -> DirectoryObject:
    """Convert a Graph directory object payload into a DirectoryObject."""
    return DirectoryObject(
        object_id=item.get("id", ""),
        display_name=item.get("displayName"),
        object_type=_friendly_type(item.get("@odata.type")) or fallback_type,
        user_principal_name=item.get("userPrincipalName"),
        mail=item.get("mail"),
    )


def _friendly_type(odata_type: Optional[str]) -> Optional[str]:
    """Map '#microsoft.graph.group' to 'Group'."""
    if not odata_type:
        return None

    short = odata_type.rsplit(".", 1)[-1]
    known = {
        "user": "User",
        "group": "Group",
        "servicePrincipal": "ServicePrincipal",
        "application": "Application",
        "device": "Device",
        "orgContact": "OrgContact",
    }
    return known.get(short, short[:1].upper() + short[1:])
