from base import AppsFlyerBaseTest
from tap_tester.base_suite_tests.automatic_fields_test import MinimumSelectionTest


class AppsFlyerAutomaticFields(MinimumSelectionTest, AppsFlyerBaseTest):
    @staticmethod
    def name():
        return "tap_appsflyer_automatic_fields_test"

    def streams_to_test(self):
        return self.expected_stream_names()
