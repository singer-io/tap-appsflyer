import unittest
from unittest import mock
from tap_appsflyer import request


class TestAppsFlyerTokens(unittest.TestCase):

    @mock.patch("requests.Request")
    @mock.patch("requests.Session.send", return_value=mock.MagicMock(status_code=200))
    def test_valid_v1_token(self, mocked_session, mocked_request):
        """ Verify V1 token with less than or equal to 36 character length is accepted """
        valid_v1_token = "a"*36

        expected_request_type = "GET"
        expected_url = "dummy_url"
        expected_params = {"api_token": valid_v1_token}
        expected_headers = {}

        request(url=expected_url, api_token=valid_v1_token)

        mocked_request.assert_called_with(expected_request_type,
                                          expected_url,
                                          params=expected_params,
                                          headers=expected_headers)

    @mock.patch("requests.Request")
    @mock.patch("requests.Session.send", return_value=mock.MagicMock(status_code=200))
    def test_valid_v2_token(self, mocked_session, mocked_request):
        """ Verify V2 token with valid JWT format and greater than 36 character length is accepted """
        valid_v2_token = f"{'a'*12}.{'b'*12}.{'c'*13}"

        expected_request_type = "GET"
        expected_url = "dummy_url"
        expected_params = {}
        expected_headers = {"Authorization": f"Bearer {valid_v2_token}"}

        request(url=expected_url, api_token=valid_v2_token)
        
        mocked_request.assert_called_with(expected_request_type,
                                          expected_url,
                                          params=expected_params,
                                          headers=expected_headers)

    def test_invalid_v2_token(self):
        """
        Verify V2 token with valid JWT format but less than or equal to 36 character length throws an exception
        """
        invalid_token = f"{'a'*12}.{'b'*12}*{'c'*13}"
        with self.assertRaises(Exception) as ex:
            request(url="dummy_url", api_token=invalid_token)

        self.assertIn("Invalid AppsFlyer V2 token", ex.exception.args)     
