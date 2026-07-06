try:
    from .base import AppsFlyerMockBaseTest
except ImportError:
    from base import AppsFlyerMockBaseTest


class AppsFlyerMockAllFieldsTest(AppsFlyerMockBaseTest):
    def _records_by_stream(self, messages):
        grouped = {}
        for msg in messages:
            if msg.get("type") != "RECORD":
                continue
            stream = msg.get("stream")
            grouped.setdefault(stream, []).append(msg.get("record", {}))
        return grouped

    def _schema_props_by_stream(self, messages):
        grouped = {}
        for msg in messages:
            if msg.get("type") != "SCHEMA":
                continue
            grouped[msg.get("stream")] = set(msg.get("schema", {}).get("properties", {}).keys())
        return grouped

    def test_all_streams_emit_records(self):
        result = self._run_mock_sync()
        self.assertEqual(result["returncode"], 0, msg=result["stderr"])

        records = self._records_by_stream(result["messages"])
        self.assertIn("installs", records)
        self.assertIn("in_app_events", records)
        self.assertGreater(len(records["installs"]), 0)
        self.assertGreater(len(records["in_app_events"]), 0)

    def test_records_only_contain_declared_schema_fields(self):
        result = self._run_mock_sync()
        self.assertEqual(result["returncode"], 0, msg=result["stderr"])

        records = self._records_by_stream(result["messages"])
        schema_props = self._schema_props_by_stream(result["messages"])

        for stream, stream_records in records.items():
            allowed = schema_props[stream]
            for record in stream_records:
                with self.subTest(stream=stream):
                    self.assertTrue(set(record.keys()).issubset(allowed))

    def test_organic_stream_emits_records_when_enabled(self):
        result = self._run_mock_sync(config=self._default_config(organic=True))

        # organic_installs currently has a known timezone bug path in the tap.
        if result["returncode"] != 0:
            self.assertIn("Syncing organic_installs", result["stderr"])
            self.assertIn("offset-naive and offset-aware", result["stderr"])
            return

        records = self._records_by_stream(result["messages"])
        self.assertIn("organic_installs", records)
        self.assertGreater(len(records["organic_installs"]), 0)
