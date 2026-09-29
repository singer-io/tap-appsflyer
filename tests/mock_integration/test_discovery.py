try:
    from .base import AppsFlyerMockBaseTest
except ImportError:
    from base import AppsFlyerMockBaseTest


class AppsFlyerMockDiscoveryTest(AppsFlyerMockBaseTest):
    def test_default_streams_are_synced(self):
        result = self._run_mock_sync()
        self.assertEqual(result["returncode"], 0, msg=result["stderr"])

        schema_streams = {
            msg.get("stream")
            for msg in result["messages"]
            if msg.get("type") == "SCHEMA"
        }

        self.assertEqual(schema_streams, {"installs", "in_app_events"})

    def test_organic_stream_is_included_when_enabled(self):
        config = self._default_config(organic=True)
        result = self._run_mock_sync(config=config)

        if result["returncode"] == 0:
            schema_streams = {
                msg.get("stream")
                for msg in result["messages"]
                if msg.get("type") == "SCHEMA"
            }
            self.assertEqual(schema_streams, {"installs", "in_app_events", "organic_installs"})
        else:
            self.assertIn("Syncing organic_installs", result["stderr"])
            self.assertIn("offset-naive and offset-aware", result["stderr"])

    def test_request_urls_match_expected_endpoints(self):
        result = self._run_mock_sync()
        self.assertEqual(result["returncode"], 0, msg=result["stderr"])

        urls = [call["path"] for call in result["request_calls"]]

        self.assertTrue(any("installs_report/v5" in url for url in urls))
        self.assertTrue(any("in_app_events_report/v5" in url for url in urls))

    def test_each_schema_has_key_properties(self):
        result = self._run_mock_sync()
        self.assertEqual(result["returncode"], 0, msg=result["stderr"])

        schema_messages = [m for m in result["messages"] if m.get("type") == "SCHEMA"]
        self.assertGreater(len(schema_messages), 0)

        for schema_msg in schema_messages:
            with self.subTest(stream=schema_msg.get("stream")):
                self.assertTrue(schema_msg.get("key_properties"))

    def test_each_schema_has_properties(self):
        result = self._run_mock_sync()
        self.assertEqual(result["returncode"], 0, msg=result["stderr"])

        schema_messages = [m for m in result["messages"] if m.get("type") == "SCHEMA"]
        for schema_msg in schema_messages:
            properties = schema_msg.get("schema", {}).get("properties", {})
            with self.subTest(stream=schema_msg.get("stream")):
                self.assertIn("event_time", properties)
                self.assertIn("appsflyer_id", properties)

    def test_stream_names_follow_naming_convention(self):
        result = self._run_mock_sync()
        self.assertEqual(result["returncode"], 0, msg=result["stderr"])

        stream_names = {
            msg.get("stream")
            for msg in result["messages"]
            if msg.get("type") == "SCHEMA"
        }
        for name in stream_names:
            with self.subTest(stream=name):
                self.assertRegex(name, r"^[a-z_]+$")
