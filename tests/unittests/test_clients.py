import unittest
from unittest import mock
from tap_appsflyer.client import Client, raise_for_error

import requests

from tap_appsflyer.exceptions import (
    appsflyerBadRequestError,
    appsflyerUnauthorizedError,
    appsflyerForbiddenError,
    appsflyerNotFoundError,
    appsflyerConflictError,
    appsflyerUnprocessableEntityError,
    appsflyerRateLimitError,
    appsflyerInternalServerError,
    appsflyerError,
)

def make_response(status_code, json_data=None, raise_json=False):
    resp = mock.MagicMock(spec=requests.Response)
    resp.status_code = status_code
    if raise_json:
        resp.json.side_effect = Exception("bad json")
    else:
        resp.json.return_value = json_data or {}
    return resp

class TestClient(unittest.TestCase):

    @mock.patch("requests.Request")
    @mock.patch("requests.Session.send", return_value=mock.MagicMock(status_code=200))
    def test_valid_v2_token(self, mocked_session, mocked_request):
        """Verify V2 token with valid JWT format and greater than 36 character length is accepted."""
        valid_v2_token = f"{'a'*12}.{'b'*12}.{'c'*13}"

        expected_request_type = "GET"
        expected_url = "dummy_endpoint"  # Relative URL
        expected_params = {}
        expected_headers = {"Authorization": f"Bearer {valid_v2_token}"}

        # Mocking the Client behavior
        client_config = {"api_token": valid_v2_token, "base_url": "https://hq1.appsflyer.com"}

        # Instantiate the client with the provided config
        with Client(client_config) as client:
            # Call the `get` method of the client
            client.get("dummy_endpoint", params=expected_params, headers=expected_headers)

            # Assert that the `requests.Request` was called with the correct URL, headers, and params
            mocked_request.assert_called_with(
                expected_request_type,
                expected_url,
                params=expected_params,
                headers=expected_headers
            )

    @mock.patch("requests.Request")
    def test_authenticate(self, MockRequest):
        """Test the `authenticate` method to ensure the headers are properly set."""
        # Sample configuration for Client
        config = {
            "api_token": "valid_api_token",
            "user_agent": "my_user_agent"
        }

        # Instantiate the client with the config
        client = Client(config)

        # Mock headers and params to pass to authenticate
        headers = {}
        params = {}

        # Call the authenticate method
        authenticated_headers, authenticated_params = client.authenticate(headers, params)

        # Check if Authorization header is set correctly
        self.assertEqual(authenticated_headers["Authorization"], "Bearer valid_api_token")

        # Check if User-Agent header is set correctly
        self.assertEqual(authenticated_headers["User-Agent"], "my_user_agent")

        # Ensure params are passed through unchanged
        self.assertEqual(authenticated_params, params)


class TestRaiseForError(unittest.TestCase):

    def test_200_does_not_raise(self):
        resp = make_response(200)
        raise_for_error(resp)  # should not raise

    def test_400_with_error_field_raises_bad_request(self):
        resp = make_response(400, {"error": "bad request detail"})
        with self.assertRaises(appsflyerBadRequestError) as ctx:
            raise_for_error(resp)
        self.assertIn("400", str(ctx.exception))
        self.assertIn("bad request detail", str(ctx.exception))

    def test_400_with_message_field_raises_bad_request(self):
        resp = make_response(400, {"message": "custom message"})
        with self.assertRaises(appsflyerBadRequestError) as ctx:
            raise_for_error(resp)
        self.assertIn("custom message", str(ctx.exception))

    def test_400_with_no_fields_uses_mapping_message(self):
        resp = make_response(400, {})
        with self.assertRaises(appsflyerBadRequestError) as ctx:
            raise_for_error(resp)
        self.assertIn("validation", str(ctx.exception).lower())

    def test_401_raises_unauthorized(self):
        resp = make_response(401, {})
        with self.assertRaises(appsflyerUnauthorizedError):
            raise_for_error(resp)

    def test_403_raises_forbidden(self):
        resp = make_response(403, {})
        with self.assertRaises(appsflyerForbiddenError):
            raise_for_error(resp)

    def test_404_raises_not_found(self):
        resp = make_response(404, {})
        with self.assertRaises(appsflyerNotFoundError):
            raise_for_error(resp)

    def test_429_raises_rate_limit(self):
        resp = make_response(429, {})
        with self.assertRaises(appsflyerRateLimitError):
            raise_for_error(resp)

    def test_500_raises_internal_server_error(self):
        resp = make_response(500, {})
        with self.assertRaises(appsflyerInternalServerError):
            raise_for_error(resp)

    def test_unmapped_status_raises_generic_error(self):
        resp = make_response(418, {})
        with self.assertRaises(appsflyerError) as ctx:
            raise_for_error(resp)
        self.assertIn("Unknown Error", str(ctx.exception))

    def test_json_parse_failure_uses_empty_dict(self):
        resp = make_response(500, raise_json=True)
        with self.assertRaises(appsflyerInternalServerError):
            raise_for_error(resp)


class TestClientGiveup(unittest.TestCase):

    def setUp(self):
        self.client = Client({"api_token": "test_token", "app_id": "app123"})

    def test_giveup_returns_true_for_4xx(self):
        exc = mock.MagicMock()
        exc.response.status_code = 400
        self.assertTrue(self.client.giveup(exc))

    def test_giveup_returns_false_for_5xx(self):
        exc = mock.MagicMock()
        exc.response.status_code = 500
        self.assertFalse(self.client.giveup(exc))

    def test_giveup_returns_false_when_no_response(self):
        exc = mock.MagicMock()
        exc.response = None
        self.assertFalse(self.client.giveup(exc))

    def test_giveup_returns_true_for_499(self):
        exc = mock.MagicMock()
        exc.response.status_code = 499
        self.assertTrue(self.client.giveup(exc))


class TestClientRequestTimeout(unittest.TestCase):

    def test_custom_request_timeout(self):
        client = Client({"api_token": "token", "request_timeout": "120"})
        self.assertEqual(client.request_timeout, 120.0)

    def test_default_request_timeout(self):
        from tap_appsflyer.client import REQUEST_TIMEOUT
        client = Client({"api_token": "token"})
        self.assertEqual(client.request_timeout, REQUEST_TIMEOUT)


class TestClientMakeRequest(unittest.TestCase):

    def setUp(self):
        self.client = Client({"api_token": "test_token", "app_id": "app123"})

    @mock.patch("tap_appsflyer.client.raise_for_error")
    def test_make_request_success(self, mock_raise_for_error):
        mock_response = mock.MagicMock()
        mock_response.json.return_value = {"data": "value"}
        self.client._session.request = mock.MagicMock(return_value=mock_response)

        result = self.client._Client__make_request("GET", "https://example.com/api")

        self.client._session.request.assert_called_once_with(
            "GET", "https://example.com/api"
        )
        mock_raise_for_error.assert_called_once_with(mock_response)
        self.assertEqual(result, {"data": "value"})

    @mock.patch("tap_appsflyer.client.raise_for_error")
    def test_make_request_with_kwargs(self, mock_raise_for_error):
        mock_response = mock.MagicMock()
        mock_response.json.return_value = {}
        self.client._session.request = mock.MagicMock(return_value=mock_response)

        self.client._Client__make_request(
            "GET", "https://example.com/api", params={"key": "value"}
        )

        self.client._session.request.assert_called_once_with(
            "GET", "https://example.com/api", params={"key": "value"}
        )
        