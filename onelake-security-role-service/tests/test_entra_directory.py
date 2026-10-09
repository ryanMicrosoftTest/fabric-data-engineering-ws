"""Tests for entra_directory — Graph lookups that resolve object IDs to names."""

from unittest.mock import MagicMock, patch

import pytest

from onelake_security.entra_directory import (
    EntraDirectoryClient,
    DirectoryLookupError,
    _friendly_type,
)


def _mock_response(status_code: int, json_data: dict = None, headers: dict = None):
    resp = MagicMock()
    resp.status_code = status_code
    resp.json.return_value = json_data or {}
    resp.headers = headers or {}
    resp.text = "error body"
    return resp


def _not_found():
    """Graph's answer when a single-object retry cannot see the object."""
    return _mock_response(404)


GRAPH_PAYLOAD = {
    "value": [
        {
            "@odata.type": "#microsoft.graph.group",
            "id": "g1",
            "displayName": "AAD-FABRIC-PRIVATE-SQL-ADMINS",
        },
        {
            "@odata.type": "#microsoft.graph.user",
            "id": "u1",
            "displayName": "Ada Lovelace",
            "userPrincipalName": "ada@contoso.com",
            "mail": "ada@contoso.com",
        },
    ]
}


def _last_json(mock_request):
    return mock_request.call_args.kwargs["json"]


class TestResolveObjects:
    @patch("onelake_security.entra_directory.requests.request")
    def test_resolves_users_and_groups(self, mock_request):
        mock_request.return_value = _mock_response(200, GRAPH_PAYLOAD)

        client = EntraDirectoryClient(graph_token="fake")
        resolved = client.resolve_objects(["u1", "g1"])

        assert resolved["g1"].display_name == "AAD-FABRIC-PRIVATE-SQL-ADMINS"
        assert resolved["g1"].object_type == "Group"
        assert resolved["g1"].is_group is True
        assert resolved["u1"].display_name == "Ada Lovelace"
        assert resolved["u1"].object_type == "User"
        assert resolved["u1"].user_principal_name == "ada@contoso.com"
        assert resolved["u1"].friendly_name == "Ada Lovelace"

    @patch("onelake_security.entra_directory.requests.request")
    def test_deduplicates_and_ignores_blanks(self, mock_request):
        mock_request.return_value = _mock_response(200, GRAPH_PAYLOAD)

        client = EntraDirectoryClient(graph_token="fake")
        client.resolve_objects(["u1", "u1", "", None, "g1"])

        assert sorted(_last_json(mock_request)["ids"]) == ["g1", "u1"]

    @patch("onelake_security.entra_directory.requests.request")
    def test_omits_unresolvable_ids(self, mock_request):
        mock_request.side_effect = [
            _mock_response(200, GRAPH_PAYLOAD),
            _not_found(),
        ]

        client = EntraDirectoryClient(graph_token="fake")
        resolved = client.resolve_objects(["u1", "g1", "deleted-obj"])

        assert "deleted-obj" not in resolved
        assert len(resolved) == 2

    @patch("onelake_security.entra_directory.requests.request")
    def test_resolves_all_types_by_default(self, mock_request):
        """App registrations must not be dropped by a narrow type filter."""
        mock_request.return_value = _mock_response(200, {
            "value": [{
                "@odata.type": "#microsoft.graph.application",
                "id": "a1",
                "displayName": "fabric-data-engineer-spn",
            }]
        })

        client = EntraDirectoryClient(graph_token="fake")
        resolved = client.resolve_objects(["a1"])

        assert "types" not in _last_json(mock_request)
        assert resolved["a1"].object_type == "Application"
        assert resolved["a1"].display_name == "fabric-data-engineer-spn"

    @patch("onelake_security.entra_directory.requests.request")
    def test_honours_explicit_type_filter(self, mock_request):
        mock_request.return_value = _mock_response(200, {"value": []})

        client = EntraDirectoryClient(graph_token="fake", types=["group"])
        client.resolve_objects(["g1"])

        assert mock_request.call_args_list[0].kwargs["json"]["types"] == ["group"]

    def test_empty_input_skips_call(self):
        client = EntraDirectoryClient(graph_token="fake")

        assert client.resolve_objects([]) == {}

    @patch("onelake_security.entra_directory.requests.request")
    def test_batches_large_id_sets(self, mock_request):
        mock_request.return_value = _mock_response(
            200, {"value": [{"@odata.type": "#microsoft.graph.user", "id": "x"}]}
        )

        client = EntraDirectoryClient(graph_token="fake", cache=False)
        client.resolve_objects([f"id-{i:05d}" for i in range(2500)])

        bulk_calls = [
            c for c in mock_request.call_args_list if c.args[0] == "POST"
        ]
        assert len(bulk_calls) == 3

    @patch("onelake_security.entra_directory.requests.request")
    def test_permission_error_is_actionable(self, mock_request):
        mock_request.return_value = _mock_response(403)

        client = EntraDirectoryClient(graph_token="fake")
        with pytest.raises(DirectoryLookupError, match="Directory.Read.All"):
            client.resolve_objects(["u1"])

    @patch("onelake_security.entra_directory.time.sleep")
    @patch("onelake_security.entra_directory.requests.request")
    def test_retries_on_throttling(self, mock_request, mock_sleep):
        mock_request.side_effect = [
            _mock_response(429, headers={"Retry-After": "1"}),
            _mock_response(200, GRAPH_PAYLOAD),
        ]

        client = EntraDirectoryClient(graph_token="fake")
        resolved = client.resolve_objects(["u1"])

        assert mock_request.call_count == 2
        assert resolved["u1"].display_name == "Ada Lovelace"

    def test_requires_token(self):
        with pytest.raises(ValueError):
            EntraDirectoryClient(graph_token="")


class TestResolveWithReasons:
    @patch("onelake_security.entra_directory.requests.request")
    def test_retries_omitted_ids_individually(self, mock_request):
        """getByIds drops IDs silently — the single-object retry recovers them."""
        mock_request.side_effect = [
            _mock_response(200, {"value": []}),
            _mock_response(200, {
                "@odata.type": "#microsoft.graph.user",
                "id": "u1",
                "displayName": "Ada Lovelace",
            }),
        ]

        client = EntraDirectoryClient(graph_token="fake")
        resolution = client.resolve(["u1"])

        assert resolution.resolved["u1"].display_name == "Ada Lovelace"
        assert resolution.unresolved == []

    @patch("onelake_security.entra_directory.requests.request")
    def test_explains_deleted_object(self, mock_request):
        mock_request.side_effect = [
            _mock_response(200, {"value": []}),
            _not_found(),
        ]

        client = EntraDirectoryClient(graph_token="fake")
        resolution = client.resolve(["gone"])

        assert resolution.unresolved_ids == ["gone"]
        assert "deleted" in resolution.unresolved[0].reason

    @patch("onelake_security.entra_directory.requests.request")
    def test_explains_missing_permission(self, mock_request):
        mock_request.side_effect = [
            _mock_response(200, {"value": []}),
            _mock_response(403),
        ]

        client = EntraDirectoryClient(graph_token="fake")
        resolution = client.resolve(["hidden"])

        assert "Directory.Read.All" in resolution.unresolved[0].reason

    @patch("onelake_security.entra_directory.requests.request")
    def test_resolve_object_returns_single(self, mock_request):
        mock_request.return_value = _mock_response(200, GRAPH_PAYLOAD)

        client = EntraDirectoryClient(graph_token="fake")

        assert client.resolve_object("u1").display_name == "Ada Lovelace"
        assert client.resolve_object("") is None


class TestCaching:
    @patch("onelake_security.entra_directory.requests.request")
    def test_second_lookup_hits_cache(self, mock_request):
        mock_request.return_value = _mock_response(200, GRAPH_PAYLOAD)

        client = EntraDirectoryClient(graph_token="fake")
        client.resolve_objects(["u1", "g1"])
        client.resolve_objects(["u1", "g1"])

        assert mock_request.call_count == 1

    @patch("onelake_security.entra_directory.requests.request")
    def test_cache_can_be_disabled(self, mock_request):
        mock_request.return_value = _mock_response(200, GRAPH_PAYLOAD)

        client = EntraDirectoryClient(graph_token="fake", cache=False)
        client.resolve_objects(["u1", "g1"])
        client.resolve_objects(["u1", "g1"])

        assert mock_request.call_count == 2

    @patch("onelake_security.entra_directory.requests.request")
    def test_clear_cache_forces_refetch(self, mock_request):
        mock_request.return_value = _mock_response(200, GRAPH_PAYLOAD)

        client = EntraDirectoryClient(graph_token="fake")
        client.resolve_objects(["u1", "g1"])
        client.clear_cache()
        client.resolve_objects(["u1", "g1"])

        assert mock_request.call_count == 2


GROUP_MEMBERS_PAGE = {
    "value": [
        {
            "@odata.type": "#microsoft.graph.user",
            "id": "user-a",
            "displayName": "Ada Lovelace",
            "userPrincipalName": "ada@contoso.com",
        },
        {
            "@odata.type": "#microsoft.graph.user",
            "id": "user-b",
            "displayName": "Grace Hopper",
            "userPrincipalName": "grace@contoso.com",
        },
    ]
}


class TestGroupMembers:
    @patch("onelake_security.entra_directory.requests.request")
    def test_lists_transitive_members_by_default(self, mock_request):
        mock_request.return_value = _mock_response(200, GROUP_MEMBERS_PAGE)

        client = EntraDirectoryClient(graph_token="fake")
        members = client.get_group_members("g1")

        assert "transitiveMembers" in mock_request.call_args.args[1]
        assert [m.display_name for m in members] == ["Ada Lovelace", "Grace Hopper"]

    @patch("onelake_security.entra_directory.requests.request")
    def test_direct_members_when_not_transitive(self, mock_request):
        mock_request.return_value = _mock_response(200, GROUP_MEMBERS_PAGE)

        client = EntraDirectoryClient(graph_token="fake")
        client.get_group_members("g1", transitive=False)

        assert mock_request.call_args.args[1].endswith("/groups/g1/members")

    @patch("onelake_security.entra_directory.requests.request")
    def test_follows_pagination_and_dedupes(self, mock_request):
        mock_request.side_effect = [
            _mock_response(200, {
                "value": GROUP_MEMBERS_PAGE["value"],
                "@odata.nextLink": "https://graph/next",
            }),
            _mock_response(200, {
                "value": [
                    GROUP_MEMBERS_PAGE["value"][0],
                    {
                        "@odata.type": "#microsoft.graph.user",
                        "id": "user-c",
                        "displayName": "Alan Turing",
                    },
                ]
            }),
        ]

        client = EntraDirectoryClient(graph_token="fake")
        members = client.get_group_members("g1")

        assert [m.object_id for m in members] == ["user-a", "user-b", "user-c"]

    @patch("onelake_security.entra_directory.requests.request")
    def test_missing_group_returns_empty(self, mock_request):
        mock_request.return_value = _not_found()

        client = EntraDirectoryClient(graph_token="fake")

        assert client.get_group_members("nope") == []

    @patch("onelake_security.entra_directory.requests.request")
    def test_permission_error_is_actionable(self, mock_request):
        mock_request.return_value = _mock_response(403)

        client = EntraDirectoryClient(graph_token="fake")
        with pytest.raises(DirectoryLookupError, match="GroupMember.Read.All"):
            client.get_group_members("g1")

    @patch("onelake_security.entra_directory.requests.request")
    def test_members_are_cached(self, mock_request):
        mock_request.return_value = _mock_response(200, GROUP_MEMBERS_PAGE)

        client = EntraDirectoryClient(graph_token="fake")
        client.get_group_members("g1")
        client.get_group_members("g1")

        assert mock_request.call_count == 1

    def test_blank_group_id_skips_call(self):
        client = EntraDirectoryClient(graph_token="fake")

        assert client.get_group_members("") == []


class TestFindByName:
    @patch("onelake_security.entra_directory.requests.request")
    def test_searches_users_groups_and_spns(self, mock_request):
        mock_request.side_effect = [
            _mock_response(200, {"value": [{
                "id": "u1", "displayName": "Ada Lovelace",
                "userPrincipalName": "ada@contoso.com",
            }]}),
            _mock_response(200, {"value": []}),
            _mock_response(200, {"value": []}),
        ]

        client = EntraDirectoryClient(graph_token="fake")
        results = client.find_by_name("Ada")

        assert mock_request.call_count == 3
        assert results[0].object_id == "u1"
        assert results[0].object_type == "User"

    @patch("onelake_security.entra_directory.requests.request")
    def test_infers_type_per_collection(self, mock_request):
        mock_request.side_effect = [
            _mock_response(200, {"value": []}),
            _mock_response(200, {"value": [
                {"id": "g1", "displayName": "AAD-NEUROLOGY-READERS"}
            ]}),
            _mock_response(200, {"value": []}),
        ]

        client = EntraDirectoryClient(graph_token="fake")
        results = client.find_by_name("AAD-NEUROLOGY")

        assert results[0].object_type == "Group"
        assert results[0].is_group is True

    @patch("onelake_security.entra_directory.requests.request")
    def test_escapes_quotes_in_filter(self, mock_request):
        mock_request.return_value = _mock_response(200, {"value": []})

        client = EntraDirectoryClient(graph_token="fake")
        client.find_by_name("O'Brien")

        assert "O''Brien" in mock_request.call_args_list[0].kwargs["params"]["$filter"]

    @patch("onelake_security.entra_directory.requests.request")
    def test_permission_error_is_actionable(self, mock_request):
        mock_request.return_value = _mock_response(403)

        client = EntraDirectoryClient(graph_token="fake")
        with pytest.raises(DirectoryLookupError, match="Directory.Read.All"):
            client.find_by_name("Ada")

    def test_blank_name_skips_call(self):
        client = EntraDirectoryClient(graph_token="fake")

        assert client.find_by_name("   ") == []


class TestFriendlyType:
    def test_maps_known_types(self):
        assert _friendly_type("#microsoft.graph.group") == "Group"
        assert _friendly_type("#microsoft.graph.user") == "User"
        assert _friendly_type("#microsoft.graph.servicePrincipal") == "ServicePrincipal"

    def test_handles_unknown_and_missing(self):
        assert _friendly_type("#microsoft.graph.widget") == "Widget"
        assert _friendly_type(None) is None
