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
        return datetime.datetime.now(datetime.timezone.utc).replace(second=0, microsecond=0)

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
            runner_python = sys.executable
            if tap_cmd:
                candidate_python = os.path.join(os.path.dirname(tap_cmd), "python")
                if os.path.exists(candidate_python):
                    runner_python = candidate_python

            driver = (
                "import json, sys\n"
                "from tap_appsflyer.client import Client\n"
                "from tap_appsflyer.discover import discover\n"
                "from tap_appsflyer.sync import sync\n"
                "config_path, state_path = sys.argv[1], sys.argv[2]\n"
                "with open(config_path, 'r', encoding='utf-8') as config_file:\n"
                "    config = json.load(config_file)\n"
                "with open(state_path, 'r', encoding='utf-8') as state_file:\n"
                "    input_state = json.load(state_file)\n"
                "catalog = discover()\n"
                "stream_order = ['installs']\n"
                "if config.get('organic_installs'):\n"
                "    stream_order.append('organic_installs')\n"
                "stream_order.append('in_app_events')\n"
                "resume_stream = input_state.get('this_stream')\n"
                "if resume_stream is not None:\n"
                "    if resume_stream not in stream_order:\n"
                "        raise SystemExit(f'Unknown stream {resume_stream} in state')\n"
                "    stream_order = stream_order[stream_order.index(resume_stream):]\n"
                "selected_streams = set(stream_order)\n"
                "for stream in catalog.streams:\n"
                "    is_selected = stream.stream in selected_streams\n"
                "    updated_metadata = []\n"
                "    for entry in stream.metadata:\n"
                "        entry = dict(entry)\n"
                "        metadata = dict(entry.get('metadata', {}))\n"
                "        metadata['selected'] = is_selected\n"
                "        entry['metadata'] = metadata\n"
                "        updated_metadata.append(entry)\n"
                "    stream.metadata = updated_metadata\n"
                "state = {}\n"
                "if resume_stream is not None:\n"
                "    state['currently_syncing'] = resume_stream\n"
                "bookmarks = {}\n"
                "bookmark_keys = {'installs': 'attributed_touch_time', 'organic_installs': 'event_time', 'in_app_events': 'event_time'}\n"
                "for stream_name, bookmark_key in bookmark_keys.items():\n"
                "    bookmark_value = input_state.get(stream_name)\n"
                "    if bookmark_value is not None:\n"
                "        bookmarks[stream_name] = {bookmark_key: bookmark_value}\n"
                "if bookmarks:\n"
                "    state['bookmarks'] = bookmarks\n"
                "with Client(config) as client:\n"
                "    sync(client=client, config=config, catalog=catalog, state=state)\n"
            )

            cmd = [
                runner_python,
                "-c",
                driver,
                config_path,
                state_path,
            ]

            env = os.environ.copy()
            pythonpath = env.get("PYTHONPATH")
            env["PYTHONPATH"] = repo_root if not pythonpath else repo_root + os.pathsep + pythonpath

            proc = subprocess.run(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                cwd=repo_root,
                env=env,
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
        legacy_state = None
        if last_state is not None:
            legacy_state = {}
            bookmarks = last_state.get("bookmarks", {})
            if "installs" in bookmarks:
                legacy_state["installs"] = bookmarks["installs"].get("attributed_touch_time")
            if "organic_installs" in bookmarks:
                legacy_state["organic_installs"] = bookmarks["organic_installs"].get("event_time")
            if "in_app_events" in bookmarks:
                legacy_state["in_app_events"] = bookmarks["in_app_events"].get("event_time")
            legacy_state["this_stream"] = last_state.get("currently_syncing")

        return {
            "returncode": proc.returncode,
            "stdout": proc.stdout,
            "stderr": proc.stderr,
            "messages": messages,
            "last_state": legacy_state,
            "request_calls": copy.deepcopy(self.__class__._server_calls),
        }
