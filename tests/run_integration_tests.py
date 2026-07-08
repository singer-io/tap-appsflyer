"""Run tap-appsflyer integration tests by execution mode.

Usage examples:
  python tests/run_integration_tests.py --mode live
  python tests/run_integration_tests.py --mode mock
  python tests/run_integration_tests.py --mode auto
"""

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path


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


def _has_live_credentials() -> bool:
    if _load_credentials_from_config() is not None:
        return True

    if os.environ.get("TAP_APPSFLYER_API_CREDS"):
        return True

    required_env = (
        "TAP_APPSFLYER_APP_ID",
        "TAP_APPSFLYER_API_TOKEN",
    )
    return all(os.environ.get(var) for var in required_env)


def _resolve_mode(requested_mode: str) -> tuple[str, str | None]:
    has_live_creds = _has_live_credentials()

    if requested_mode == "mock":
        return "mock", None

    if requested_mode == "live":
        if has_live_creds:
            return "live", None
        return "mock", "Live mode requested but AppsFlyer credentials are missing; running mock tests instead."

    if has_live_creds:
        return "live", None

    return "mock", "AppsFlyer credentials not found; running mock tests."


def _live_test_files() -> list[str]:
    tests_dir = Path(__file__).resolve().parent
    return sorted(
        str(path.relative_to(tests_dir.parent))
        for path in tests_dir.glob("test*.py")
        if path.is_file()
    )


def main() -> int:
    parser = argparse.ArgumentParser(description="Run integration tests by mode")
    parser.add_argument(
        "--mode",
        choices=["live", "mock", "auto"],
        default=os.environ.get("INTEGRATION_TEST_MODE", "auto").lower(),
        help="live runs tests/*.py, mock runs tests/mock_integration",
    )
    args = parser.parse_args()

    mode, note = _resolve_mode(args.mode)
    targets = _live_test_files() if mode == "live" else ["tests/mock_integration"]

    print("Selected integration test mode:", mode)
    if note:
        print(note)
    print("Running:", " ".join([sys.executable, "-m", "pytest", *targets]))

    command = [sys.executable, "-m", "pytest", *targets]
    env = os.environ.copy()
    env["INTEGRATION_TEST_MODE"] = mode
    return subprocess.call(command, env=env)


if __name__ == "__main__":
    raise SystemExit(main())
