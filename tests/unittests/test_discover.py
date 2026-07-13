import importlib
import unittest
from unittest.mock import patch

discover_module = importlib.import_module("tap_appsflyer.discover")


class TestDiscover(unittest.TestCase):
    @patch.object(discover_module, "get_schemas")
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

        catalog = discover_module.discover()

        self.assertEqual(len(catalog.streams), 1)
        stream = catalog.streams[0]
        self.assertEqual(stream.stream, "installs")
        self.assertEqual(stream.tap_stream_id, "installs")
        self.assertEqual(stream.key_properties, ["attributed_touch_time"])

    @patch.object(discover_module, "LOGGER")
    @patch.object(discover_module.Schema, "from_dict", side_effect=Exception("bad schema"))
    @patch.object(discover_module, "get_schemas")
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
            discover_module.discover()

        self.assertGreaterEqual(mock_logger.error.call_count, 1)
