import unittest

from tap_appsflyer.exceptions import (
    appsflyerError,
    appsflyerBackoffError,
    appsflyerBadRequestError,
    appsflyerUnauthorizedError,
    appsflyerForbiddenError,
    appsflyerNotFoundError,
    appsflyerConflictError,
    appsflyerUnprocessableEntityError,
    appsflyerRateLimitError,
    appsflyerInternalServerError,
    appsflyerNotImplementedError,
    appsflyerBadGatewayError,
    appsflyerServiceUnavailableError,
    ERROR_CODE_EXCEPTION_MAPPING,
)


class TestAppsflyerError(unittest.TestCase):

    def test_init_with_message_and_response(self):
        exc = appsflyerError("test message", response="mock_response")
        self.assertEqual(exc.message, "test message")
        self.assertEqual(exc.response, "mock_response")
        self.assertEqual(str(exc), "test message")

    def test_init_defaults(self):
        exc = appsflyerError()
        self.assertIsNone(exc.message)
        self.assertIsNone(exc.response)

    def test_is_exception(self):
        exc = appsflyerError("error")
        self.assertIsInstance(exc, Exception)


class TestExceptionHierarchy(unittest.TestCase):

    def test_backoff_error_is_appsflyer_error(self):
        self.assertTrue(issubclass(appsflyerBackoffError, appsflyerError))

    def test_bad_request_is_appsflyer_error(self):
        self.assertTrue(issubclass(appsflyerBadRequestError, appsflyerError))

    def test_unauthorized_is_appsflyer_error(self):
        self.assertTrue(issubclass(appsflyerUnauthorizedError, appsflyerError))

    def test_unprocessable_is_backoff_error(self):
        self.assertTrue(issubclass(appsflyerUnprocessableEntityError, appsflyerBackoffError))

    def test_rate_limit_is_backoff_error(self):
        self.assertTrue(issubclass(appsflyerRateLimitError, appsflyerBackoffError))

    def test_internal_server_is_backoff_error(self):
        self.assertTrue(issubclass(appsflyerInternalServerError, appsflyerBackoffError))


class TestErrorCodeMapping(unittest.TestCase):

    def test_mapping_contains_expected_codes(self):
        expected_codes = [400, 401, 403, 404, 409, 422, 429, 500, 501, 502, 503]
        for code in expected_codes:
            self.assertIn(code, ERROR_CODE_EXCEPTION_MAPPING)

    def test_mapping_entries_have_required_keys(self):
        for code, entry in ERROR_CODE_EXCEPTION_MAPPING.items():
            self.assertIn("raise_exception", entry, f"code {code} missing raise_exception")
            self.assertIn("message", entry, f"code {code} missing message")

    def test_400_maps_to_bad_request(self):
        self.assertEqual(
            ERROR_CODE_EXCEPTION_MAPPING[400]["raise_exception"],
            appsflyerBadRequestError,
        )

    def test_429_maps_to_rate_limit(self):
        self.assertEqual(
            ERROR_CODE_EXCEPTION_MAPPING[429]["raise_exception"],
            appsflyerRateLimitError,
        )
        