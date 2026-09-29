from base import AppsFlyerBaseTest
from tap_tester.base_suite_tests.all_fields_test import AllFieldsTest


class AppsFlyerAllFieldsTest(AllFieldsTest, AppsFlyerBaseTest):
    @staticmethod
    def name():
        return "tap_appsflyer_all_fields_test"

    def streams_to_test(self):
        return self.expected_stream_names()
