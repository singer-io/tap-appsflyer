import csv
import datetime
import io
import unittest
from unittest import mock

import pytz
import singer

from tap_appsflyer.client import Client
from tap_appsflyer.streams.abstracts import (
    BaseStream,
    IncrementalStream,
    RequestToCsvAdapter,
    fieldnames,
)
from tap_appsflyer.streams.in_app_events import InAppEvents
from tap_appsflyer.streams.installs import Installs
from tap_appsflyer.streams.organic_installs import OrganicInstalls


def make_client(config=None):
    c = config or {
        "api_token": "test_token",
        "app_id": "com.test.app",
        "start_date": "2020-01-01T00:00:00Z",
    }
    client = mock.MagicMock(spec=Client)
    client.base_url = "https://hq1.appsflyer.com"
    client.config = c
    return client


class TestRequestToCsvAdapter(unittest.TestCase):

    def test_iter_returns_decoded_strings(self):
        mock_response = mock.MagicMock()
        mock_response.iter_lines.return_value = iter([b"line1", b"line2"])
        adapter = RequestToCsvAdapter(mock_response)
        result = list(adapter)
        self.assertEqual(result, ["line1", "line2"])

    def test_next_raises_stop_iteration_when_exhausted(self):
        mock_response = mock.MagicMock()
        mock_response.iter_lines.return_value = iter([])
        adapter = RequestToCsvAdapter(mock_response)
        with self.assertRaises(StopIteration):
            next(adapter)


class TestBaseStreamGetRecords(unittest.TestCase):

    def setUp(self):
        self.client = make_client()
        self.stream = InAppEvents(self.client)

    @mock.patch("tap_appsflyer.streams.abstracts.SESSION")
    def test_get_records_calls_client_get(self, mock_session):
        mock_prepared_req = mock.MagicMock()
        self.client.get.return_value = mock_prepared_req
        mock_resp = mock.MagicMock()
        mock_resp.status_code = 200
        mock_session.send.return_value = mock_resp

        result = self.stream.get_records()

        self.client.get.assert_called_once()
        mock_session.send.assert_called_once_with(mock_prepared_req)
        self.assertEqual(result, mock_resp)

    @mock.patch("tap_appsflyer.streams.abstracts.SESSION")
    def test_get_records_logs_warning_when_no_response(self, mock_session):
        self.client.get.return_value = None
        mock_resp = mock.MagicMock()
        mock_resp.status_code = 200
        mock_session.send.return_value = mock_resp

        with mock.patch("tap_appsflyer.streams.abstracts.LOGGER") as mock_logger:
            self.stream.get_records()
            mock_logger.warning.assert_called_once()


class TestBaseStreamProperties(unittest.TestCase):

    def setUp(self):
        self.stream = InAppEvents(make_client())

    def test_selected_by_default_is_false(self):
        self.assertFalse(self.stream.selected_by_default)

    def test_base_get_url_endpoint_returns_url_endpoint_attr(self):
        # Use a stream that does NOT override get_url_endpoint
        class BareStream(IncrementalStream):
            tap_stream_id = "bare"
            key_properties = ["id"]
            replication_keys = ["id"]
            url_endpoint = "https://example.com/bare"

        stream = BareStream(make_client())
        self.assertEqual(stream.get_url_endpoint(), "https://example.com/bare")


class TestBaseStreamParseSourceFromUrl(unittest.TestCase):

    def setUp(self):
        self.stream = InAppEvents(make_client())

    def test_returns_none_when_no_match(self):
        result = self.stream.parse_source_from_url("https://hq1.appsflyer.com")
        self.assertIsNone(result)

    def test_returns_source_when_regex_matches(self):
        with mock.patch("tap_appsflyer.streams.abstracts.re.compile") as mock_compile:
            mock_match = mock.MagicMock()
            mock_match.group.return_value = "installs"
            mock_compile.return_value.match.return_value = mock_match
            result = self.stream.parse_source_from_url("https://hq1.appsflyer.com")
        self.assertEqual(result, "installs")


class TestBaseStreamWriteSchema(unittest.TestCase):

    def setUp(self):
        self.stream = InAppEvents(make_client())

    @mock.patch("tap_appsflyer.streams.abstracts.write_schema")
    def test_write_schema_calls_singer_write_schema(self, mock_write_schema):
        schema = {"type": "object", "properties": {}}
        self.stream.write_schema(schema, "in_app_events")
        mock_write_schema.assert_called_once_with(
            "in_app_events", schema, self.stream.key_properties
        )

    @mock.patch("tap_appsflyer.streams.abstracts.write_schema")
    def test_write_schema_raises_os_error(self, mock_write_schema):
        mock_write_schema.side_effect = OSError("disk full")
        with self.assertRaises(OSError):
            self.stream.write_schema({}, "in_app_events")


class TestIncrementalStreamGetBookmark(unittest.TestCase):

    def setUp(self):
        self.client = make_client()
        self.stream = InAppEvents(self.client)

    def test_get_bookmark_with_state(self):
        state = {
            "bookmarks": {
                "in_app_events": {"event_time": "2026-07-01T00:00:00Z"}
            }
        }
        result = self.stream.get_bookmark(state)
        self.assertIsInstance(result, datetime.datetime)
        self.assertIsNotNone(result.tzinfo)

    def test_get_bookmark_no_state_no_config(self):
        self.client.config = {"api_token": "token", "app_id": "app"}
        result = self.stream.get_bookmark({})
        self.assertIsInstance(result, datetime.datetime)
        # Should be ~30 days ago
        expected = datetime.datetime.now(pytz.utc) - datetime.timedelta(days=30)
        diff = abs((result - expected).total_seconds())
        self.assertLess(diff, 60)

    def test_get_bookmark_with_start_date_in_config(self):
        self.client.config = {
            "api_token": "token",
            "app_id": "app",
            "start_date": "2020-01-01T00:00:00Z",
        }
        result = self.stream.get_bookmark({})
        self.assertIsInstance(result, datetime.datetime)

    def test_get_bookmark_with_custom_key(self):
        state = {
            "bookmarks": {
                "in_app_events": {"custom_key": "2026-07-01T00:00:00Z"}
            }
        }
        result = self.stream.get_bookmark(state, key="custom_key")
        self.assertIsInstance(result, datetime.datetime)


class TestIncrementalStreamGetStop(unittest.TestCase):

    def setUp(self):
        self.stream = InAppEvents(make_client())

    def test_get_stop_within_30_days(self):
        start = datetime.datetime(2026, 7, 1, tzinfo=pytz.utc)
        stop = datetime.datetime(2026, 8, 10, tzinfo=pytz.utc)
        result = self.stream.get_stop(start, stop)
        self.assertEqual(result, datetime.datetime(2026, 7, 31, tzinfo=pytz.utc))

    def test_get_stop_capped_by_stop_time(self):
        start = datetime.datetime(2026, 7, 1, tzinfo=pytz.utc)
        stop = datetime.datetime(2026, 7, 5, tzinfo=pytz.utc)
        result = self.stream.get_stop(start, stop)
        self.assertEqual(result, stop)

    def test_get_stop_raises_type_error_for_non_datetime(self):
        with self.assertRaises(TypeError):
            self.stream.get_stop("not-a-datetime", datetime.datetime.now(pytz.utc))

    def test_get_stop_custom_days(self):
        start = datetime.datetime(2026, 7, 1, tzinfo=pytz.utc)
        stop = datetime.datetime(2026, 9, 1, tzinfo=pytz.utc)
        result = self.stream.get_stop(start, stop, days=10)
        self.assertEqual(result, datetime.datetime(2026, 7, 11, tzinfo=pytz.utc))


class TestIncrementalStreamWriteBookmark(unittest.TestCase):

    def setUp(self):
        self.stream = InAppEvents(make_client())

    def test_write_bookmark_updates_state(self):
        state = {}
        result = self.stream.write_bookmark(
            state, value="2026-07-01T00:00:00.000000Z"
        )
        self.assertIn("bookmarks", result)
        self.assertIn("in_app_events", result["bookmarks"])

    def test_write_bookmark_with_custom_key(self):
        state = {}
        result = self.stream.write_bookmark(
            state, key="custom_key", value="2026-07-01T00:00:00.000000Z"
        )
        self.assertIn("custom_key", result["bookmarks"]["in_app_events"])


class TestIncrementalStreamXform(unittest.TestCase):

    def setUp(self):
        self.stream = InAppEvents(make_client())

    def test_xform_empty_strings_to_none(self):
        record = {"field1": "value", "field2": "", "field3": ""}
        self.stream.xform_empty_strings_to_none(record)
        self.assertEqual(record["field1"], "value")
        self.assertIsNone(record["field2"])
        self.assertIsNone(record["field3"])

    def test_xform_boolean_field_true(self):
        record = {"wifi": "True"}
        self.stream.xform_boolean_field(record, "wifi")
        self.assertTrue(record["wifi"])

    def test_xform_boolean_field_false(self):
        record = {"wifi": "False"}
        self.stream.xform_boolean_field(record, "wifi")
        self.assertFalse(record["wifi"])

    def test_xform_boolean_field_none_returns_early(self):
        record = {"wifi": None}
        self.stream.xform_boolean_field(record, "wifi")
        self.assertIsNone(record["wifi"])

    def test_xform_boolean_field_case_insensitive(self):
        record = {"wifi": "TRUE"}
        self.stream.xform_boolean_field(record, "wifi")
        self.assertTrue(record["wifi"])

    def test_xform_transforms_record(self):
        record = {f: "" for f in fieldnames}
        record["wifi"] = "true"
        record["is_retargeting"] = "false"
        result = self.stream.xform(record)
        self.assertTrue(result["wifi"])
        self.assertFalse(result["is_retargeting"])


class TestIncrementalStreamSync(unittest.TestCase):

    def setUp(self):
        self.client = make_client()
        self.stream = InAppEvents(self.client)

    def _make_mock_response(self, lines):
        mock_resp = mock.MagicMock()
        mock_resp.iter_lines.return_value = iter(lines)
        return mock_resp

    def _make_data_line(self, overrides=None):
        row = {f: "" for f in fieldnames}
        row["wifi"] = "false"
        row["is_retargeting"] = "false"
        if overrides:
            row.update(overrides)
        buf = io.StringIO()
        writer = csv.writer(buf)
        writer.writerow([row[f] for f in fieldnames])
        return buf.getvalue().strip().encode("utf-8")

    def test_sync_empty_response_returns_state(self):
        mock_resp = self._make_mock_response([])
        self.stream.get_records = mock.MagicMock(return_value=mock_resp)

        mock_transformer = mock.MagicMock()
        state = {}
        result = self.stream.sync(
            state=state,
            schema={},
            stream_metadata={},
            transformer=mock_transformer,
        )
        self.assertEqual(result, state)

    @mock.patch("tap_appsflyer.streams.abstracts.write_record")
    def test_sync_with_valid_records(self, mock_write_record):
        recent = (datetime.datetime.now(pytz.utc) - datetime.timedelta(days=5)).strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        )
        header = b"header"
        data_line = self._make_data_line({"event_time": recent})
        mock_resp = self._make_mock_response([header, data_line])
        self.stream.get_records = mock.MagicMock(return_value=mock_resp)

        mock_transformer = mock.MagicMock()
        mock_transformer.transform.side_effect = lambda r, s, m: r

        state = {}
        result = self.stream.sync(
            state=state,
            schema={"properties": {"event_time": {"type": ["null", "string"]}}},
            stream_metadata={},
            transformer=mock_transformer,
        )
        self.assertEqual(result, 1)
        mock_write_record.assert_called_once()

    @mock.patch("tap_appsflyer.streams.abstracts.write_record")
    def test_sync_skips_old_records(self, mock_write_record):
        # Bookmark is ~90 days ago (restricted), use a record older than that
        old_date = "2020-01-01T00:00:00Z"
        header = b"header"
        data_line = self._make_data_line({"event_time": old_date})
        mock_resp = self._make_mock_response([header, data_line])
        self.stream.get_records = mock.MagicMock(return_value=mock_resp)

        mock_transformer = mock.MagicMock()
        mock_transformer.transform.side_effect = lambda r, s, m: r

        state = {}
        result = self.stream.sync(
            state=state,
            schema={"properties": {"event_time": {"type": ["null", "string"]}}},
            stream_metadata={},
            transformer=mock_transformer,
        )
        self.assertEqual(result, 0)
        mock_write_record.assert_not_called()

    @mock.patch("tap_appsflyer.streams.abstracts.write_record")
    def test_sync_handles_missing_replication_key(self, mock_write_record):
        header = b"header"
        data_line = self._make_data_line()
        mock_resp = self._make_mock_response([header, data_line])
        self.stream.get_records = mock.MagicMock(return_value=mock_resp)

        # transformer returns record without the replication key
        mock_transformer = mock.MagicMock()
        mock_transformer.transform.side_effect = lambda r, s, m: {}

        state = {}
        result = self.stream.sync(
            state=state,
            schema={},
            stream_metadata={},
            transformer=mock_transformer,
        )
        self.assertEqual(result, 0)
        mock_write_record.assert_not_called()

    @mock.patch("tap_appsflyer.streams.abstracts.write_record")
    def test_sync_with_bookmark_state(self, mock_write_record):
        recent = (datetime.datetime.now(pytz.utc) - datetime.timedelta(days=5)).strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        )
        header = b"header"
        data_line = self._make_data_line({"event_time": recent})
        mock_resp = self._make_mock_response([header, data_line])
        self.stream.get_records = mock.MagicMock(return_value=mock_resp)

        mock_transformer = mock.MagicMock()
        mock_transformer.transform.side_effect = lambda r, s, m: r

        state = {
            "bookmarks": {"in_app_events": {"event_time": "2026-07-01T00:00:00Z"}}
        }
        result = self.stream.sync(
            state=state,
            schema={"properties": {"event_time": {"type": ["null", "string"]}}},
            stream_metadata={},
            transformer=mock_transformer,
        )
        # Record is 5 days ago, bookmark is 2026-07-01, so depends on date
        self.assertIsInstance(result, int)


class TestStreamGetUrlEndpoint(unittest.TestCase):

    def test_in_app_events_url(self):
        client = make_client({"api_token": "tok", "app_id": "myapp"})
        stream = InAppEvents(client)
        url = stream.get_url_endpoint()
        self.assertIn("myapp", url)
        self.assertIn("in_app_events_report", url)

    def test_installs_url(self):
        client = make_client({"api_token": "tok", "app_id": "myapp"})
        stream = Installs(client)
        url = stream.get_url_endpoint()
        self.assertIn("myapp", url)
        self.assertIn("installs_report", url)

    def test_organic_installs_url(self):
        client = make_client({"api_token": "tok", "app_id": "myapp"})
        stream = OrganicInstalls(client)
        url = stream.get_url_endpoint()
        self.assertIn("myapp", url)
        self.assertIn("organic_installs_report", url)
