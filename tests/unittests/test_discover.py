import unittest
from unittest import mock

import singer

from tap_appsflyer.discover import discover


class TestDiscover(unittest.TestCase):

    @mock.patch("tap_appsflyer.discover.get_schemas")
    def test_discover_returns_catalog(self, mock_get_schemas):
        schema_dict = {
            "type": "object",
            "properties": {
                "event_time": {"type": ["null", "string"]},
                "event_name": {"type": ["null", "string"]},
                "appsflyer_id": {"type": ["null", "string"]},
            },
        }
        mdata = [
            {
                "breadcrumb": [],
                "metadata": {
                    "table-key-properties": ["event_time", "event_name", "appsflyer_id"],
                    "forced-replication-method": "INCREMENTAL",
                    "valid-replication-keys": ["event_time"],
                },
            },
            {
                "breadcrumb": ["properties", "event_time"],
                "metadata": {"inclusion": "automatic"},
            },
        ]
        mock_get_schemas.return_value = (
            {"in_app_events": schema_dict},
            {"in_app_events": mdata},
        )

        catalog = discover()

        self.assertIsInstance(catalog, singer.catalog.Catalog)
        self.assertEqual(len(catalog.streams), 1)
        entry = catalog.streams[0]
        self.assertEqual(entry.stream, "in_app_events")
        self.assertEqual(entry.tap_stream_id, "in_app_events")
        self.assertEqual(entry.key_properties, ["event_time", "event_name", "appsflyer_id"])

    @mock.patch("tap_appsflyer.discover.get_schemas")
    def test_discover_multiple_streams(self, mock_get_schemas):
        schema_dict = {
            "type": "object",
            "properties": {"event_time": {"type": ["null", "string"]}},
        }
        mdata = [
            {
                "breadcrumb": [],
                "metadata": {
                    "table-key-properties": ["event_time"],
                    "forced-replication-method": "INCREMENTAL",
                    "valid-replication-keys": ["event_time"],
                },
            }
        ]
        mock_get_schemas.return_value = (
            {"stream_a": schema_dict, "stream_b": schema_dict},
            {"stream_a": mdata, "stream_b": mdata},
        )

        catalog = discover()

        self.assertEqual(len(catalog.streams), 2)

    @mock.patch("tap_appsflyer.discover.get_schemas")
    def test_discover_raises_on_bad_schema(self, mock_get_schemas):
        mock_get_schemas.return_value = (
            {"bad_stream": "not_a_dict"},
            {"bad_stream": []},
        )

        with self.assertRaises(Exception):
            discover()

