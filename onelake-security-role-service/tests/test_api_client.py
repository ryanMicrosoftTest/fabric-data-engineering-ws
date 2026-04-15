"""Tests for the Fabric API client — REST transport with ETag and retry handling."""

import pytest
from unittest.mock import MagicMock, patch, PropertyMock
from onelake_security.api_client import OneLakeSecurityClient, PreconditionFailedError


def _mock_response(status_code: int, json_data: dict = None, headers: dict = None):
    """Create a mock requests.Response."""
    resp = MagicMock()
    resp.status_code = status_code
    resp.json.return_value = json_data or {}
    resp.headers = headers or {}
    resp.raise_for_status = MagicMock()
    if status_code >= 400:
        from requests.exceptions import HTTPError
        resp.raise_for_status.side_effect = HTTPError(response=resp)
    resp.content = b'{"value": []}' if json_data else b''
    return resp


class TestListRoles:
    """GET /dataAccessRoles — retrieve current roles + ETag."""

    @patch("onelake_security.api_client.requests.get")
    def test_returns_roles_and_etag(self, mock_get):
        roles_data = {"value": [{"name": "Role1", "id": "r1"}]}
        mock_get.return_value = _mock_response(
            200, roles_data, headers={"ETag": '"abc123"'}
        )

        client = OneLakeSecurityClient(api_token="fake-token")
        roles, etag = client.list_roles("ws-id", "item-id")

        assert len(roles) == 1
        assert roles[0]["name"] == "Role1"
        assert etag == '"abc123"'

    @patch("onelake_security.api_client.requests.get")
    def test_empty_roles(self, mock_get):
        mock_get.return_value = _mock_response(
            200, {"value": []}, headers={"ETag": '"empty"'}
        )

        client = OneLakeSecurityClient(api_token="fake-token")
        roles, etag = client.list_roles("ws-id", "item-id")

        assert roles == []
        assert etag == '"empty"'

    @patch("onelake_security.api_client.requests.get")
    def test_correct_url_and_headers(self, mock_get):
        mock_get.return_value = _mock_response(200, {"value": []}, headers={})

        client = OneLakeSecurityClient(api_token="my-token")
        client.list_roles("ws-123", "item-456")

        mock_get.assert_called_once()
        call_args = mock_get.call_args
        assert "ws-123" in call_args[0][0]
        assert "item-456" in call_args[0][0]
        assert call_args[1]["headers"]["Authorization"] == "Bearer my-token"


class TestPutRoles:
    """PUT /dataAccessRoles — replace all roles with ETag concurrency."""

    @patch("onelake_security.api_client.requests.put")
    def test_successful_put_returns_new_etag(self, mock_put):
        mock_put.return_value = _mock_response(
            200, headers={"ETag": '"new-etag"'}
        )

        client = OneLakeSecurityClient(api_token="fake-token")
        new_etag = client.put_roles(
            "ws-id", "item-id",
            roles=[{"name": "Role1"}],
            etag='"old-etag"',
        )

        assert new_etag == '"new-etag"'

    @patch("onelake_security.api_client.requests.put")
    def test_sends_if_match_header(self, mock_put):
        mock_put.return_value = _mock_response(200, headers={"ETag": '"x"'})

        client = OneLakeSecurityClient(api_token="fake-token")
        client.put_roles("ws", "item", [{"name": "R"}], etag='"my-etag"')

        call_headers = mock_put.call_args[1]["headers"]
        assert call_headers["If-Match"] == '"my-etag"'

    @patch("onelake_security.api_client.requests.put")
    def test_sends_correct_body(self, mock_put):
        mock_put.return_value = _mock_response(200, headers={})

        client = OneLakeSecurityClient(api_token="fake-token")
        roles = [{"name": "R1"}, {"name": "R2"}]
        client.put_roles("ws", "item", roles, etag='"e"')

        call_json = mock_put.call_args[1]["json"]
        assert call_json == {"value": roles}

    @patch("onelake_security.api_client.requests.put")
    def test_412_raises_precondition_failed(self, mock_put):
        mock_put.return_value = _mock_response(412)

        client = OneLakeSecurityClient(api_token="fake-token")
        with pytest.raises(PreconditionFailedError):
            client.put_roles("ws", "item", [], etag='"stale"')

    @patch("onelake_security.api_client.requests.put")
    def test_put_without_etag_omits_if_match(self, mock_put):
        mock_put.return_value = _mock_response(200, headers={})

        client = OneLakeSecurityClient(api_token="fake-token")
        client.put_roles("ws", "item", [], etag=None)

        call_headers = mock_put.call_args[1]["headers"]
        assert "If-Match" not in call_headers


class TestDryRun:
    """PUT with dryRun=true — validate without applying."""

    @patch("onelake_security.api_client.requests.put")
    def test_dry_run_returns_true_on_success(self, mock_put):
        mock_put.return_value = _mock_response(200)

        client = OneLakeSecurityClient(api_token="fake-token")
        result = client.put_roles_dry_run("ws", "item", [{"name": "R"}])

        assert result is True
        assert "dryRun=true" in mock_put.call_args[0][0]

    @patch("onelake_security.api_client.requests.put")
    def test_dry_run_returns_false_on_error(self, mock_put):
        mock_put.return_value = _mock_response(400)

        client = OneLakeSecurityClient(api_token="fake-token")
        result = client.put_roles_dry_run("ws", "item", [{"name": "R"}])

        assert result is False


class TestRetryOn429:
    """429 Too Many Requests — retry with backoff."""

    @patch("onelake_security.api_client.time.sleep")
    @patch("onelake_security.api_client.requests.get")
    def test_retries_on_429_then_succeeds(self, mock_get, mock_sleep):
        """Should retry after 429 and succeed on second attempt."""
        resp_429 = _mock_response(429, headers={"Retry-After": "1"})
        resp_429.raise_for_status = MagicMock()  # 429 is handled before raise
        resp_200 = _mock_response(200, {"value": []}, headers={"ETag": '"ok"'})

        mock_get.side_effect = [resp_429, resp_200]

        client = OneLakeSecurityClient(api_token="fake-token")
        roles, etag = client.list_roles("ws", "item")

        assert etag == '"ok"'
        assert mock_sleep.called

    @patch("onelake_security.api_client.time.sleep")
    @patch("onelake_security.api_client.requests.get")
    def test_gives_up_after_max_retries(self, mock_get, mock_sleep):
        """Should raise after exhausting retry attempts."""
        resp_429 = _mock_response(429, headers={"Retry-After": "1"})
        resp_429.raise_for_status = MagicMock()
        mock_get.return_value = resp_429

        client = OneLakeSecurityClient(api_token="fake-token", max_retries=2)
        with pytest.raises(Exception, match="429"):
            client.list_roles("ws", "item")


class TestClientConstruction:
    """Client initialization and configuration."""

    def test_requires_api_token(self):
        with pytest.raises(ValueError, match="api_token"):
            OneLakeSecurityClient(api_token="")

    def test_default_base_url(self):
        client = OneLakeSecurityClient(api_token="token")
        assert "fabric.microsoft.com" in client.base_url

    def test_custom_base_url(self):
        client = OneLakeSecurityClient(
            api_token="token",
            base_url="https://custom.api.com"
        )
        assert client.base_url == "https://custom.api.com"
