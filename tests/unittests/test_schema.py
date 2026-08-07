import json
import os
import unittest
from unittest import mock

from tap_appsflyer.schema import get_abs_path, get_schemas, load_schema_references


class TestGetAbsPath(unittest.TestCase):

    def test_returns_absolute_path(self):
        result = get_abs_path("schemas")
        self.assertTrue(os.path.isabs(result))
        self.assertTrue(result.endswith("schemas"))

    def test_path_is_relative_to_module(self):
        result = get_abs_path("schemas/installs.json")
        self.assertTrue(os.path.exists(result))


class TestLoadSchemaReferences(unittest.TestCase):

    def test_returns_empty_dict_when_no_shared_dir(self):
        with mock.patch("os.path.exists", return_value=False):
            result = load_schema_references()
        self.assertEqual(result, {})

    def test_loads_shared_schemas_when_dir_exists(self):
        fake_schema = {"type": "object"}
        fake_files = ["shared_schema.json"]

        with mock.patch("os.path.exists", return_value=True), \
             mock.patch("os.listdir", return_value=fake_files), \
             mock.patch("os.path.isfile", return_value=True), \
             mock.patch("builtins.open", mock.mock_open(read_data=json.dumps(fake_schema))):
            result = load_schema_references()

        self.assertIn("shared/shared_schema.json", result)
        self.assertEqual(result["shared/shared_schema.json"], fake_schema)


class TestGetSchemas(unittest.TestCase):

    def test_get_schemas_returns_all_streams(self):
        schemas, field_metadata = get_schemas()

        from tap_appsflyer.streams import STREAMS
        for stream_name in STREAMS:
            self.assertIn(stream_name, schemas)
            self.assertIn(stream_name, field_metadata)

    def test_schemas_are_dicts(self):
        schemas, _ = get_schemas()
        for stream_name, schema in schemas.items():
            self.assertIsInstance(schema, dict, f"{stream_name} schema is not a dict")

    def test_field_metadata_contains_entries(self):
        _, field_metadata = get_schemas()
        for stream_name, mdata in field_metadata.items():
            self.assertIsInstance(mdata, list)
            self.assertGreater(len(mdata), 0)

    def test_schemas_have_properties(self):
        schemas, _ = get_schemas()
        for stream_name, schema in schemas.items():
            self.assertIn("properties", schema, f"{stream_name} missing 'properties'")

    def test_metadata_has_key_properties(self):
        from singer import metadata
        _, field_metadata = get_schemas()
        for stream_name, mdata in field_metadata.items():
            mdata_map = metadata.to_map(mdata)
            key_props = mdata_map.get((), {}).get("table-key-properties")
            self.assertIsNotNone(key_props, f"{stream_name} missing table-key-properties")
<<<<<<< HEAD
            
=======
>>>>>>> origin/SAC-31821/python-upgrade
