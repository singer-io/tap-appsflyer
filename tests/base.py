import json
import os
import unittest
from datetime import timedelta
from pathlib import Path

from tap_tester.base_suite_tests.base_case import BaseCase


def _config_paths():
    env_path = os.environ.get("TAP_APPSFLYER_CONFIG_JSON") or os.environ.get("APPSFLYER_CONFIG_JSON")
    if env_path:
        yield Path(env_path)

    yield Path(__file__).resolve().parent / "config.json"
    yield Path(__file__).resolve().parents[1] / "config.json"


def _load_credentials_from_config():
    for config_path in _config_paths():
        if not config_path.is_file():
            continue

        with config_path.open("r", encoding="utf-8") as config_file:
            config = json.load(config_file)

        app_id = config.get("app_id")
        api_token = config.get("api_token")
        if app_id and api_token:
            return {
                "app_id": app_id,
                "api_token": api_token,
            }

    return None


def _resolve_mode() -> str:
    mode = os.environ.get("INTEGRATION_TEST_MODE", "auto").lower()
    if mode in {"live", "mock"}:
        return mode

    if _load_credentials_from_config() is not None:
        return "live"

    required_env = (
        "TAP_APPSFLYER_APP_ID",
        "TAP_APPSFLYER_API_TOKEN",
    )
    has_live_creds = bool(os.environ.get("TAP_APPSFLYER_API_CREDS")) or all(
        os.environ.get(var) for var in required_env
    )
    return "live" if has_live_creds else "mock"


class AppsFlyerBaseTest(BaseCase):
    start_date = "2024-01-01T00:00:00Z"
    bookmark_format = "%Y-%m-%d %H:%M:%S"

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        if _resolve_mode() != "live":
            raise unittest.SkipTest(
                "Root integration tests run only in live mode. "
                "Use tests/mock_integration for mock mode."
            )

    @staticmethod
    def tap_name():
        return "tap-appsflyer"

    @staticmethod
    def get_type():
        return "platform.appsflyer"

    def get_properties(self, original=True):
        return {
            "start_date": self.start_date,
            "user_agent": "tap-appsflyer <api_user_agent@example.com>",
        }

    def get_credentials(self):
        credentials = _load_credentials_from_config()
        if credentials is not None:
            return credentials

        return {
            "app_id": os.environ["TAP_APPSFLYER_APP_ID"],
            "api_token": os.environ["TAP_APPSFLYER_API_TOKEN"],
        }

    @classmethod
    def expected_metadata(cls):
        return {
            "installs": {
                cls.PRIMARY_KEYS: {"event_time", "event_name", "appsflyer_id"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"attributed_touch_time"},
                cls.OBEYS_START_DATE: True,
                cls.RESPECTS_START_DATE: True,
                cls.API_LIMIT: 0,
            },
            "in_app_events": {
                cls.PRIMARY_KEYS: {"event_time", "event_name", "appsflyer_id"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"event_time"},
                cls.OBEYS_START_DATE: True,
                cls.RESPECTS_START_DATE: True,
                cls.API_LIMIT: 0,
                cls.LOOK_BACK_WINDOW: timedelta(days=0),
            },
        }

    @classmethod
    def expected_stream_names(cls) -> set:
        return set(cls.expected_metadata().keys())

    def expected_primary_keys(self, stream=None) -> dict:
        primary_keys = {
            table: properties.get(self.PRIMARY_KEYS, set())
            for table, properties in self.expected_metadata().items()
        }
        if stream is None:
            return primary_keys
        return primary_keys[stream]

    def expected_replication_keys(self, stream=None) -> dict:
        replication_keys = {
            table: properties.get(self.REPLICATION_KEYS, set())
            for table, properties in self.expected_metadata().items()
        }
        if stream is None:
            return replication_keys
        return replication_keys[stream]

    def expected_replication_method(self, stream=None) -> dict:
        replication_method = {
            table: properties.get(self.REPLICATION_METHOD, None)
            for table, properties in self.expected_metadata().items()
        }
        if stream is None:
            return replication_method
        return replication_method[stream]

    def expected_lookback_window(self, stream=None):
        lookback = {
            "installs": timedelta(days=0),
            "in_app_events": timedelta(days=0),
        }
        if stream is None:
            return lookback
        return lookback[stream]
