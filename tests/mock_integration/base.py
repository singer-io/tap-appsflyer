import copy
import datetime
import http.server
import json
import os
import socketserver
import subprocess
import sys
import tempfile
import threading
import unittest
from urllib.parse import parse_qs
from urllib.parse import urlparse


class _ThreadingTCPServer(socketserver.ThreadingMixIn, socketserver.TCPServer):
    allow_reuse_address = True


class AppsFlyerMockBaseTest(unittest.TestCase):
    _server = None
    _server_thread = None
    _server_base_url = None
    _server_calls = []

    @staticmethod
    def _now():
        return datetime.datetime.now(datetime.timezone.utc).replace(microsecond=0)

    @staticmethod
    def _to_api_timestamp(value):
        return value.strftime("%Y-%m-%d %H:%M:%S")

    @classmethod
    def _csv_lines_for_query(cls, query):
        from_param = query.get("from", [None])[0]
        if from_param:
            event_dt = datetime.datetime.strptime(from_param, "%Y-%m-%d %H:%M")
        else:
            event_dt = cls._now().replace(tzinfo=None)

        event_str = cls._to_api_timestamp(event_dt)
        row = ["" for _ in range(81)]
        row[0] = "click"
        row[1] = event_str
        row[2] = event_str
        row[3] = event_str
        row[4] = "install"
        row[9] = "SDK"
        row[54] = "false"
        row[58] = "af-id-001"
        row[65] = "ios"
        row[70] = "mock-app-id"
        row[73] = "false"

        header = ",".join([f"h{i}" for i in range(81)])
        return [header, ",".join(row)]

    @classmethod
    def setUpClass(cls):
        super().setUpClass()

        class _Handler(http.server.BaseHTTPRequestHandler):
            def do_GET(self):
                parsed = urlparse(self.path)
                query = parse_qs(parsed.query)
                auth = self.headers.get("Authorization")

                cls._server_calls.append(
                    {
                        "path": parsed.path,
                        "query": copy.deepcopy(query),
                        "authorization": auth,
                    }
                )

                if not parsed.path.endswith("_report/v5"):
                    self.send_response(404)
                    self.end_headers()
                    return

                lines = cls._csv_lines_for_query(query)
                payload = "\n".join(lines) + "\n"
                body = payload.encode("utf-8")

                self.send_response(200)
                self.send_header("Content-Type", "text/csv")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, format_str, *args):
                return

        cls._server = _ThreadingTCPServer(("127.0.0.1", 0), _Handler)
        host, port = cls._server.server_address
        cls._server_base_url = f"http://{host}:{port}"
        cls._server_thread = threading.Thread(target=cls._server.serve_forever, daemon=True)
        cls._server_thread.start()

    @classmethod
    def tearDownClass(cls):
        if cls._server:
            cls._server.shutdown()
            cls._server.server_close()
        if cls._server_thread:
            cls._server_thread.join(timeout=2)
        super().tearDownClass()

    def setUp(self):
        self.__class__._server_calls = []

    @classmethod
    def _default_config(cls, start_date=None, organic=False):
        if not start_date:
            start_date = (cls._now() - datetime.timedelta(hours=2)).strftime("%Y-%m-%dT%H:%M:%SZ")

        return {
            "app_id": "mock-app-id",
            "api_token": "mock-token",
            "start_date": start_date,
            "organic_installs": organic,
            "base_url": cls._server_base_url,
        }

    def _run_mock_sync(self, config=None, state=None):
        run_config = config or self._default_config()
        run_state = state or {}
        repo_root = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))

        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = os.path.join(tmpdir, "config.json")
            state_path = os.path.join(tmpdir, "state.json")

            with open(config_path, "w", encoding="utf-8") as config_file:
                json.dump(run_config, config_file)

            with open(state_path, "w", encoding="utf-8") as state_file:
                json.dump(run_state, state_file)

            tap_cmd = os.getenv("STITCH_TAP_PATH")
            if tap_cmd:
                cmd = [tap_cmd, "--config", config_path, "--state", state_path]
            else:
                cmd = [
                    sys.executable,
                    "-c",
                    "import tap_appsflyer; tap_appsflyer.main()",
                    "--config",
                    config_path,
                    "--state",
                    state_path,
                ]

            proc = subprocess.run(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                cwd=repo_root,
                timeout=120,
                check=False,
            )

        messages = []
        for line in proc.stdout.splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                messages.append(json.loads(line))
            except json.JSONDecodeError:
                continue

        state_messages = [m for m in messages if m.get("type") == "STATE"]
        last_state = state_messages[-1].get("value") if state_messages else None

        return {
            "returncode": proc.returncode,
            "stdout": proc.stdout,
            "stderr": proc.stderr,
            "messages": messages,
            "last_state": last_state,
            "request_calls": copy.deepcopy(self.__class__._server_calls),
        }
