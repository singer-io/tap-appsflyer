from base import AppsFlyerBaseTest
from tap_tester.base_suite_tests.discovery_test import DiscoveryTest as TT_DiscoveryTest


class DiscoveryTest(TT_DiscoveryTest, AppsFlyerBaseTest):
    @staticmethod
    def name():
        return "tap_appsflyer_discovery_test"

    def streams_to_test(self):
        return self.expected_stream_names()
