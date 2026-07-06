import os
import unittest

from base import _resolve_mode

mode = os.environ.get("INTEGRATION_TEST_MODE", "auto").lower()
if mode == "auto":
    mode = _resolve_mode()

if mode == "mock":
    raise unittest.SkipTest("Root tests run in live mode. Use tests/mock_integration/ for mock tests.")

from base import AppsFlyerBaseTest
from tap_tester.base_suite_tests.bookmark_test import BookmarkTest


class AppsFlyerBookmarkTest(BookmarkTest, AppsFlyerBaseTest):
    bookmark_format = "%Y-%m-%d %H:%M:%S"
    initial_bookmarks = None

    @staticmethod
    def name():
        return "tap_appsflyer_bookmark_test"

    def streams_to_test(self):
        return {
            s for s, m in self.expected_replication_method().items()
            if m == self.INCREMENTAL
        }
