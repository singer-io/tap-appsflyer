import io
import unittest
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import tap_appsflyer


class TestEntrypoint(unittest.TestCase):
    @patch("tap_appsflyer.discover")
    def test_do_discover_writes_catalog_json(self, mock_discover):
        mock_catalog = MagicMock()
        mock_catalog.to_dict.return_value = {"streams": [{"stream": "installs"}]}
        mock_discover.return_value = mock_catalog

        output = io.StringIO()
        with patch("sys.stdout", output):
            tap_appsflyer.do_discover()

        self.assertIn('"streams"', output.getvalue())
        self.assertIn('"installs"', output.getvalue())

    @patch("tap_appsflyer.sync")
    @patch("tap_appsflyer.Client")
    @patch("tap_appsflyer.singer.utils.parse_args")
    def test_main_runs_sync_when_catalog_present(self, mock_parse_args, mock_client, mock_sync):
        mock_parse_args.return_value = SimpleNamespace(
            state={"bookmarks": {}},
            config={"api_token": "t", "app_id": "id"},
            discover=False,
            catalog={"streams": []},
        )
        mock_client.return_value.__enter__.return_value = object()

        tap_appsflyer.main()

        mock_sync.assert_called_once()

    @patch("tap_appsflyer.do_discover")
    @patch("tap_appsflyer.Client")
    @patch("tap_appsflyer.singer.utils.parse_args")
    def test_main_runs_discover_when_requested(self, mock_parse_args, mock_client, mock_do_discover):
        mock_parse_args.return_value = SimpleNamespace(
            state=None,
            config={"api_token": "t", "app_id": "id"},
            discover=True,
            catalog=None,
        )
        mock_client.return_value.__enter__.return_value = object()

        tap_appsflyer.main()

        mock_do_discover.assert_called_once()
