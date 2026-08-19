import datetime

try:
    from .base import AppsFlyerMockBaseTest
except ImportError:
    from base import AppsFlyerMockBaseTest


class AppsFlyerMockBookmarkTest(AppsFlyerMockBaseTest):
    def test_bookmarks_written_for_default_streams(self):
        result = self._run_mock_sync()
        self.assertEqual(result["returncode"], 0, msg=result["stderr"])
        final_state = result["last_state"] or {}

        self.assertIn("installs", final_state)
        self.assertIn("in_app_events", final_state)
        self.assertIsNone(final_state.get("this_stream"))

    def test_resume_from_in_app_events_skips_installs(self):
        recent = (self._now() - datetime.timedelta(hours=1)).strftime("%Y-%m-%dT%H:%M:%SZ")
        state = {"this_stream": "in_app_events", "in_app_events": recent}

        result = self._run_mock_sync(state=state)
        self.assertEqual(result["returncode"], 0, msg=result["stderr"])
        schema_streams = [
            msg.get("stream")
            for msg in result["messages"]
            if msg.get("type") == "SCHEMA"
        ]

        self.assertEqual(schema_streams, ["in_app_events"])

    def test_unknown_resume_stream_raises(self):
        result = self._run_mock_sync(state={"this_stream": "unknown_stream"})
        self.assertNotEqual(result["returncode"], 0)
        self.assertIn("Unknown stream", result["stderr"])
