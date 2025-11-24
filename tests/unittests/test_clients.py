import unittest
from unittest import mock
from tap_appsflyer.client import Client

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
