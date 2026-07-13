import unittest
from unittest.mock import patch

from tap_appsflyer.discover import discover


class TestDiscover(unittest.TestCase):
    @patch("tap_appsflyer.discover.get_schemas")
    def test_discover_builds_catalog_entries(self, mock_get_schemas):
        mock_get_schemas.return_value = (
            {
                "installs": {
                    "type": "object",
                    "properties": {
                        "attributed_touch_time": {"type": ["null", "string"]},
                    },
                }
            },
            {
                "installs": [
                    {
                        "breadcrumb": (),
                        "metadata": {
                            "table-key-properties": ["attributed_touch_time"],
                        },
                    }
                ]
            },
        )

        catalog = discover()

        self.assertEqual(len(catalog.streams), 1)
        stream = catalog.streams[0]
        self.assertEqual(stream.stream, "installs")
        self.assertEqual(stream.tap_stream_id, "installs")
        self.assertEqual(stream.key_properties, ["attributed_touch_time"])

    @patch("tap_appsflyer.discover.LOGGER")
    @patch("tap_appsflyer.discover.Schema.from_dict", side_effect=Exception("bad schema"))
    @patch("tap_appsflyer.discover.get_schemas")
    def test_discover_logs_and_reraises_schema_errors(self, mock_get_schemas, mock_schema, mock_logger):
        mock_get_schemas.return_value = (
            {
                "installs": {
                    "type": "object",
                    "properties": {
                        "attributed_touch_time": {"type": ["null", "string"]},
                    },
                }
            },
            {"installs": []},
        )

        with self.assertRaises(Exception):
            discover()

        self.assertGreaterEqual(mock_logger.error.call_count, 1)
