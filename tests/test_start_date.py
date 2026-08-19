from base import AppsFlyerBaseTest
from tap_tester.base_suite_tests.start_date_test import StartDateTest as TT_StartDateTest


class StartDateTest(TT_StartDateTest, AppsFlyerBaseTest):
    start_date_1 = "2024-01-01T00:00:00Z"
    start_date_2 = "2024-06-01T00:00:00Z"

    @staticmethod
    def name():
        return "tap_appsflyer_start_date_test"

    def streams_to_test(self):
        return {
            s for s, m in self.expected_replication_method().items()
            if m == self.INCREMENTAL
        }
