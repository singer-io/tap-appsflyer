import unittest
from unittest import mock

import singer

from tap_appsflyer.client import Client
from tap_appsflyer.sync import sync, update_currently_syncing


class TestUpdateCurrentlySyncing(unittest.TestCase):

    def test_sets_currently_syncing_when_stream_name_given(self):
        state = {}
        with mock.patch("tap_appsflyer.sync.singer.write_state") as mock_write_state:
            update_currently_syncing(state, "my_stream")
        self.assertEqual(state.get("currently_syncing"), "my_stream")
        mock_write_state.assert_called_once_with(state)

    def test_clears_currently_syncing_when_none_given(self):
        state = {"currently_syncing": "old_stream"}
        with mock.patch("tap_appsflyer.sync.singer.write_state") as mock_write_state:
            update_currently_syncing(state, None)
        self.assertNotIn("currently_syncing", state)
        mock_write_state.assert_called_once_with(state)

    def test_no_error_when_clearing_with_no_existing_syncing(self):
        state = {}
        with mock.patch("tap_appsflyer.sync.singer.write_state"):
            # Should not raise even if there's no currently_syncing key
            update_currently_syncing(state, None)


class TestSync(unittest.TestCase):

    def _make_catalog(self, stream_names):
        catalog = mock.MagicMock(spec=singer.Catalog)

        selected = []
        for name in stream_names:
            entry = mock.MagicMock()
            entry.stream = name
            entry.schema = mock.MagicMock()
            entry.schema.to_dict.return_value = {
                "type": "object",
                "properties": {"event_time": {"type": ["null", "string"]}},
            }
            entry.metadata = []
            selected.append(entry)

        catalog.get_selected_streams.return_value = selected
        catalog.get_stream.side_effect = lambda name: next(
            s for s in selected if s.stream == name
        )
        return catalog

    def _make_client(self):
        client = mock.MagicMock(spec=Client)
        client.base_url = "https://hq1.appsflyer.com"
        client.config = {"api_token": "tok", "app_id": "app", "start_date": "2020-01-01T00:00:00Z"}
        return client

    @mock.patch("tap_appsflyer.sync.singer.write_state")
    def test_sync_calls_stream_sync(self, mock_write_state):
        client = self._make_client()
        catalog = self._make_catalog(["in_app_events"])

        mock_stream_instance = mock.MagicMock()
        mock_stream_instance.sync.return_value = 5

        with mock.patch.dict(
            "tap_appsflyer.sync.STREAMS",
            {"in_app_events": mock.MagicMock(return_value=mock_stream_instance)},
        ):
            with singer.Transformer() as transformer:
                sync(
                    client=client,
                    config=client.config,
                    catalog=catalog,
                    state={},
                )

        mock_stream_instance.sync.assert_called_once()

    @mock.patch("tap_appsflyer.sync.singer.write_state")
    def test_sync_updates_currently_syncing(self, mock_write_state):
        client = self._make_client()
        catalog = self._make_catalog(["in_app_events"])

        mock_stream_instance = mock.MagicMock()
        mock_stream_instance.sync.return_value = 0

        with mock.patch.dict(
            "tap_appsflyer.sync.STREAMS",
            {"in_app_events": mock.MagicMock(return_value=mock_stream_instance)},
        ):
            with singer.Transformer() as transformer:
                sync(
                    client=client,
                    config=client.config,
                    catalog=catalog,
                    state={},
                )

        # write_state called at least twice: once per update_currently_syncing call
        self.assertGreaterEqual(mock_write_state.call_count, 2)

    @mock.patch("tap_appsflyer.sync.singer.write_state")
    def test_sync_no_selected_streams(self, mock_write_state):
        client = self._make_client()
        catalog = self._make_catalog([])

        # Should not raise
        with singer.Transformer() as transformer:
            sync(
                client=client,
                config=client.config,
                catalog=catalog,
                state={},
            )

    @mock.patch("tap_appsflyer.sync.singer.write_state")
    def test_sync_calls_write_schema(self, mock_write_state):
        client = self._make_client()
        catalog = self._make_catalog(["installs"])

        mock_stream_instance = mock.MagicMock()
        mock_stream_instance.sync.return_value = 3

        with mock.patch.dict(
            "tap_appsflyer.sync.STREAMS",
            {"installs": mock.MagicMock(return_value=mock_stream_instance)},
        ):
            with singer.Transformer() as transformer:
                sync(
                    client=client,
                    config=client.config,
                    catalog=catalog,
                    state={},
                )

        mock_stream_instance.write_schema.assert_called_once()

    @mock.patch("tap_appsflyer.sync.singer.write_state")
    def test_sync_with_last_stream_in_state(self, mock_write_state):
        client = self._make_client()
        catalog = self._make_catalog(["organic_installs"])
        state = {"currently_syncing": "organic_installs"}

        mock_stream_instance = mock.MagicMock()
        mock_stream_instance.sync.return_value = 2

        with mock.patch.dict(
            "tap_appsflyer.sync.STREAMS",
            {"organic_installs": mock.MagicMock(return_value=mock_stream_instance)},
        ):
            with singer.Transformer() as transformer:
                sync(
                    client=client,
                    config=client.config,
                    catalog=catalog,
                    state=state,
                )

        mock_stream_instance.sync.assert_called_once()
<<<<<<< HEAD
        
=======
>>>>>>> origin/SAC-31821/python-upgrade
