import unittest

try:
    from tap_tester.base_suite_tests.pagination_test import PaginationTest
except ImportError:
    try:
        from tap_tester.base_suite_tests.pagenation_test import PaginationTest
    except ImportError:
        PaginationTest = None

from base import AppsFlyerBaseTest


@unittest.skip("AppsFlyer raw-data endpoints are date-window based and do not expose page-size pagination semantics.")
@unittest.skipIf(PaginationTest is None, "tap_tester PaginationTest is unavailable in this environment")
class AppsFlyerPaginationTest(PaginationTest, AppsFlyerBaseTest):
    @staticmethod
    def name():
        return "tap_tester_appsflyer_pagination_test"

    def streams_to_test(self):
        return {
            "installs",
            "in_app_events",
        }
