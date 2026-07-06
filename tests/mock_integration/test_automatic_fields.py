try:
    from .base import AppsFlyerMockBaseTest
except ImportError:
    from base import AppsFlyerMockBaseTest


class AppsFlyerMockAutomaticFieldsTest(AppsFlyerMockBaseTest):
    def _schemas(self, messages):
        return [msg for msg in messages if msg.get("type") == "SCHEMA"]

    def _records(self, messages, stream):
        return [
            msg.get("record", {})
            for msg in messages
            if msg.get("type") == "RECORD" and msg.get("stream") == stream
        ]

    def test_schema_key_properties_are_present(self):
        result = self._run_mock_sync()
        self.assertEqual(result["returncode"], 0, msg=result["stderr"])

        for schema_msg in self._schemas(result["messages"]):
            with self.subTest(stream=schema_msg.get("stream")):
                self.assertGreater(len(schema_msg.get("key_properties", [])), 0)

    def test_installs_key_properties_exist_in_every_record(self):
        result = self._run_mock_sync()
        self.assertEqual(result["returncode"], 0, msg=result["stderr"])

        schema = next(
            s for s in self._schemas(result["messages"])
            if s.get("stream") == "installs"
        )
        key_props = schema.get("key_properties", [])
        records = self._records(result["messages"], "installs")

        self.assertGreater(len(records), 0)
        for record in records:
            for key_prop in key_props:
                with self.subTest(stream="installs", key=key_prop):
                    self.assertIn(key_prop, record)
                    self.assertIsNotNone(record.get(key_prop))

    def test_in_app_events_key_properties_exist_in_every_record(self):
        result = self._run_mock_sync()
        self.assertEqual(result["returncode"], 0, msg=result["stderr"])

        schema = next(
            s for s in self._schemas(result["messages"])
            if s.get("stream") == "in_app_events"
        )
        key_props = schema.get("key_properties", [])
        records = self._records(result["messages"], "in_app_events")

        self.assertGreater(len(records), 0)
        for record in records:
            for key_prop in key_props:
                with self.subTest(stream="in_app_events", key=key_prop):
                    self.assertIn(key_prop, record)
                    self.assertIsNotNone(record.get(key_prop))
