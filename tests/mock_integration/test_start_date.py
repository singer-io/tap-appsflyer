import datetime

try:
    from .base import AppsFlyerMockBaseTest
except ImportError:
    from base import AppsFlyerMockBaseTest


class AppsFlyerMockStartDateTest(AppsFlyerMockBaseTest):
    def _parse_param_datetime(self, value):
        return datetime.datetime.strptime(value, "%Y-%m-%d %H:%M")

    def test_first_request_uses_config_start_date(self):
        start = (self._now() - datetime.timedelta(hours=3)).strftime("%Y-%m-%dT%H:%M:%SZ")
        config = self._default_config(start_date=start)

        result = self._run_mock_sync(config=config)
        self.assertEqual(result["returncode"], 0, msg=result["stderr"])
        first_call = result["request_calls"][0]
        from_dt = self._parse_param_datetime(first_call["query"]["from"][0])
        expected = datetime.datetime.strptime(start, "%Y-%m-%dT%H:%M:%SZ")

        self.assertEqual(from_dt, expected.replace(second=0))

    def test_state_overrides_start_date(self):
        state_dt = (self._now() - datetime.timedelta(hours=1)).strftime("%Y-%m-%dT%H:%M:%SZ")
        old_start = (self._now() - datetime.timedelta(days=2)).strftime("%Y-%m-%dT%H:%M:%SZ")
        config = self._default_config(start_date=old_start)

        result = self._run_mock_sync(config=config, state={"installs": state_dt})
        self.assertEqual(result["returncode"], 0, msg=result["stderr"])
        install_calls = [c for c in result["request_calls"] if "installs_report/v5" in c["path"]]
        from_dt = self._parse_param_datetime(install_calls[0]["query"]["from"][0])
        expected = datetime.datetime.strptime(state_dt, "%Y-%m-%dT%H:%M:%SZ")

        self.assertEqual(from_dt, expected.replace(second=0))
