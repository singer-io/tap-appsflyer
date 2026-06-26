import copy
import unittest

import tap_appsflyer
from tap_appsflyer import Stream
from tap_appsflyer import get_streams_to_sync


class TestDiscoverySelection(unittest.TestCase):
    def setUp(self):
        self.original_config = copy.deepcopy(tap_appsflyer.CONFIG)

    def tearDown(self):
        tap_appsflyer.CONFIG.clear()
        tap_appsflyer.CONFIG.update(self.original_config)

    @staticmethod
    def _streams():
        return [
            Stream("installs", lambda: None),
            Stream("in_app_events", lambda: None),
        ]

    def test_default_streams_returned_when_no_target(self):
        tap_appsflyer.CONFIG.clear()
        tap_appsflyer.CONFIG.update({"app_id": "x", "api_token": "y"})

        streams = get_streams_to_sync(self._streams(), {})

        self.assertEqual([s.name for s in streams], ["installs", "in_app_events"])

    def test_organic_stream_added_when_enabled(self):
        tap_appsflyer.CONFIG.clear()
        tap_appsflyer.CONFIG.update({"app_id": "x", "api_token": "y", "organic_installs": True})

        streams = get_streams_to_sync(self._streams(), {})

        self.assertEqual([s.name for s in streams], ["installs", "in_app_events", "organic_installs"])

    def test_resume_from_target_stream(self):
        tap_appsflyer.CONFIG.clear()
        tap_appsflyer.CONFIG.update({"app_id": "x", "api_token": "y"})

        streams = get_streams_to_sync(self._streams(), {"this_stream": "in_app_events"})

        self.assertEqual([s.name for s in streams], ["in_app_events"])

    def test_unknown_resume_stream_raises(self):
        tap_appsflyer.CONFIG.clear()
        tap_appsflyer.CONFIG.update({"app_id": "x", "api_token": "y"})

        with self.assertRaises(Exception) as ctx:
            get_streams_to_sync(self._streams(), {"this_stream": "missing_stream"})

        self.assertIn("Unknown stream", str(ctx.exception))
