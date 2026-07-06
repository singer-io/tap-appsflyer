"""Run tap-appsflyer integration tests by execution mode.

Usage examples:
  python tests/run_integration_tests.py --mode live
  python tests/run_integration_tests.py --mode mock
  python tests/run_integration_tests.py --mode auto
"""

import argparse
import os
import subprocess
import sys
from pathlib import Path

from base import _load_credentials_from_config


def _resolve_mode(requested_mode: str) -> str:
    if requested_mode in {"live", "mock"}:
        return requested_mode

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

    mode = _resolve_mode(args.mode)
    targets = _live_test_files() if mode == "live" else ["tests/mock_integration"]

    print("Selected integration test mode:", mode)
    print("Running:", " ".join([sys.executable, "-m", "pytest", *targets]))

    command = [sys.executable, "-m", "pytest", *targets]
    return subprocess.call(command)


if __name__ == "__main__":
    raise SystemExit(main())
