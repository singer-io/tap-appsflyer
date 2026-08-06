import unittest
from unittest import mock

import singer

import tap_appsflyer
from tap_appsflyer import do_discover, REQUIRED_CONFIG_KEYS
from tap_appsflyer.client import Client


class TestDoDiscover(unittest.TestCase):

    @mock.patch("tap_appsflyer.json.dump")
    @mock.patch("tap_appsflyer.discover")
    def test_do_discover_calls_discover_and_dumps(self, mock_discover, mock_json_dump):
        mock_catalog = mock.MagicMock()
        mock_catalog.to_dict.return_value = {"streams": []}
        mock_discover.return_value = mock_catalog

        do_discover()

        mock_discover.assert_called_once()
        mock_catalog.to_dict.assert_called_once()
        mock_json_dump.assert_called_once()

    @mock.patch("tap_appsflyer.json.dump")
    @mock.patch("tap_appsflyer.discover")
    def test_do_discover_outputs_to_stdout(self, mock_discover, mock_json_dump):
        import sys
        mock_catalog = mock.MagicMock()
        mock_catalog.to_dict.return_value = {"streams": []}
        mock_discover.return_value = mock_catalog

        do_discover()

        args, kwargs = mock_json_dump.call_args
        self.assertEqual(args[1], sys.stdout)


class TestMain(unittest.TestCase):

    @mock.patch("tap_appsflyer.Client")
    @mock.patch("tap_appsflyer.do_discover")
    @mock.patch("singer.utils.parse_args")
    def test_main_discover_mode(self, mock_parse_args, mock_do_discover, mock_client_cls):
        mock_args = mock.MagicMock()
        mock_args.discover = True
        mock_args.catalog = None
        mock_args.state = None
        mock_args.config = {"api_token": "tok", "app_id": "app"}
        mock_parse_args.return_value = mock_args

        mock_client_instance = mock.MagicMock()
        mock_client_cls.return_value.__enter__ = mock.MagicMock(
            return_value=mock_client_instance
        )
        mock_client_cls.return_value.__exit__ = mock.MagicMock(return_value=False)

        tap_appsflyer.main()

        mock_do_discover.assert_called_once()

    @mock.patch("tap_appsflyer.sync")
    @mock.patch("tap_appsflyer.Client")
    @mock.patch("singer.utils.parse_args")
    def test_main_sync_mode(self, mock_parse_args, mock_client_cls, mock_sync):
        mock_args = mock.MagicMock()
        mock_args.discover = False
        mock_args.catalog = mock.MagicMock()
        mock_args.state = None
        mock_args.config = {"api_token": "tok", "app_id": "app"}
        mock_parse_args.return_value = mock_args

        mock_client_instance = mock.MagicMock()
        mock_client_cls.return_value.__enter__ = mock.MagicMock(
            return_value=mock_client_instance
        )
        mock_client_cls.return_value.__exit__ = mock.MagicMock(return_value=False)

        tap_appsflyer.main()

        mock_sync.assert_called_once()

    @mock.patch("tap_appsflyer.Client")
    @mock.patch("singer.utils.parse_args")
    def test_main_uses_provided_state(self, mock_parse_args, mock_client_cls):
        mock_args = mock.MagicMock()
        mock_args.discover = False
        mock_args.catalog = None
        mock_args.state = {"bookmarks": {"stream": {"key": "value"}}}
        mock_args.config = {"api_token": "tok", "app_id": "app"}
        mock_parse_args.return_value = mock_args

        mock_client_cls.return_value.__enter__ = mock.MagicMock(
            return_value=mock.MagicMock()
        )
        mock_client_cls.return_value.__exit__ = mock.MagicMock(return_value=False)

        # Just verify it doesn't raise
        tap_appsflyer.main()

    def test_main_guard(self):
        with mock.patch("singer.utils.parse_args") as mock_parse_args, \
             mock.patch("tap_appsflyer.client.Client") as mock_client_cls:
            mock_args = mock.MagicMock()
            mock_args.discover = False
            mock_args.catalog = None
            mock_args.state = None
            mock_args.config = {"api_token": "tok", "app_id": "app"}
            mock_parse_args.return_value = mock_args
            mock_client_cls.return_value.__enter__ = mock.MagicMock(
                return_value=mock.MagicMock()
            )
            mock_client_cls.return_value.__exit__ = mock.MagicMock(return_value=False)
            import runpy, tap_appsflyer
            runpy.run_path(tap_appsflyer.__file__, run_name="__main__")


class TestRequiredConfigKeys(unittest.TestCase):

    def test_required_keys(self):
        self.assertIn("app_id", REQUIRED_CONFIG_KEYS)
        self.assertIn("api_token", REQUIRED_CONFIG_KEYS)
        