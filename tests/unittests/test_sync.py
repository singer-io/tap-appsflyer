import importlib
import unittest
from unittest.mock import patch

sync_module = importlib.import_module("tap_appsflyer.sync")


class DummySelectedStream:
    def __init__(self, stream):
        self.stream = stream


class DummySchema:
    def to_dict(self):
        return {"type": "object", "properties": {}}


class DummyCatalogStream:
    def __init__(self):
        self.schema = DummySchema()
        self.metadata = [{"breadcrumb": (), "metadata": {}}]


class DummyCatalog:
    def __init__(self, selected_names):
        self._selected = [DummySelectedStream(name) for name in selected_names]

    def get_selected_streams(self, state):
        return self._selected

    def get_stream(self, stream_name):
        return DummyCatalogStream()


class DummyTransformer:
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class DummyStream:
    def __init__(self, client):
        pass

    def write_schema(self, stream_schema, stream_name):
        return None

    def sync(self, state, schema, stream_metadata, transformer):
        return 7


class TestSyncHelpers(unittest.TestCase):
    @patch.object(sync_module.singer, "write_state")
    @patch.object(sync_module.singer, "set_currently_syncing")
    @patch.object(sync_module.singer, "get_currently_syncing", return_value="installs")
    def test_update_currently_syncing_clears_value(self, mock_get, mock_set, mock_write):
        state = {"currently_syncing": "installs"}
        sync_module.update_currently_syncing(state, None)

        self.assertNotIn("currently_syncing", state)
        mock_set.assert_not_called()
        mock_write.assert_called_once_with(state)


class TestSync(unittest.TestCase):
    @patch.object(sync_module, "update_currently_syncing")
    @patch.object(sync_module.singer.metadata, "to_map", return_value={})
    @patch.object(sync_module.singer, "Transformer", return_value=DummyTransformer())
    @patch.object(sync_module.singer, "get_currently_syncing", return_value=None)
    @patch.dict(sync_module.STREAMS, {"installs": DummyStream}, clear=True)
    def test_sync_runs_selected_stream(
        self,
        mock_current,
        mock_transformer,
        mock_to_map,
        mock_update,
    ):
        catalog = DummyCatalog(["installs"])

        sync_module.sync(client=object(), config={}, catalog=catalog, state={})

        self.assertEqual(mock_update.call_count, 2)
        mock_to_map.assert_called_once()
