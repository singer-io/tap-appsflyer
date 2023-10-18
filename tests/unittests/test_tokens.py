import unittest
from unittest import mock
from tap_appsflyer import request


class TestAppsFlyerTokens(unittest.TestCase):

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
