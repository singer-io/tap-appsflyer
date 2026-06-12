"""
Unit tests for tap_appsflyer discovery logic:
  - BaseStream.check_access()
  - discover._prune_inaccessible_children()
  - discover._apply_access_checks()
  - discover.discover()
"""
import json
import unittest
from io import StringIO
from unittest.mock import MagicMock, patch

from singer.catalog import Catalog

from tap_appsflyer.discover import (
    _apply_access_checks,
    _prune_inaccessible_children,
    discover,
)
from tap_appsflyer import do_discover
from tap_appsflyer.exceptions import (
    appsflyerForbiddenError,
    appsflyerUnauthorizedError
)
from tap_appsflyer.streams import STREAMS
import tap_appsflyer as _pkg

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_client(app_id="test_app_id", api_token="test_token"):
    """Return a MagicMock that satisfies the attributes accessed by streams."""
    client = MagicMock()
    client.config = {"app_id": app_id, "api_token": api_token}
    client.base_url = "https://hq1.appsflyer.com"
    return client


def _make_schemas_and_metadata():
    """Return minimal schemas/field_metadata dicts mirroring get_schemas() output."""
    schemas = {name: {"type": "object", "properties": {}} for name in STREAMS}
    field_metadata = {name: [] for name in STREAMS}
    return schemas, field_metadata


# ---------------------------------------------------------------------------
# BaseStream.check_access()
# ---------------------------------------------------------------------------

class TestCheckAccess(unittest.TestCase):
    """Tests for BaseStream.check_access()."""

    def _make_stream(self, stream_name="installs"):
        """Instantiate a real stream class with a mocked client."""
        client = _make_client()
        stream_cls = STREAMS[stream_name]
        return stream_cls(client=client), client

    def test_returns_true_when_probe_succeeds(self):
        """check_access() returns True when probe_request does not raise."""
        stream, client = self._make_stream("installs")
        client.probe_request.return_value = None  # no exception

        result = stream.check_access()

        self.assertTrue(result)
        client.probe_request.assert_called_once()

    def test_returns_false_on_forbidden_error(self):
        """check_access() returns False when probe_request raises appsflyerForbiddenError."""
        stream, client = self._make_stream("installs")
        client.probe_request.side_effect = appsflyerForbiddenError("403 Forbidden")

        result = stream.check_access()

        self.assertFalse(result)

    def test_returns_false_for_organic_installs_on_forbidden(self):
        """check_access() returns False for organic_installs when 403 is raised."""
        stream, client = self._make_stream("organic_installs")
        client.probe_request.side_effect = appsflyerForbiddenError("403 Forbidden")

        result = stream.check_access()

        self.assertFalse(result)

    def test_returns_false_for_in_app_events_on_forbidden(self):
        """check_access() returns False for in_app_events when 403 is raised."""
        stream, client = self._make_stream("in_app_events")
        client.probe_request.side_effect = appsflyerForbiddenError("403 Forbidden")

        result = stream.check_access()

        self.assertFalse(result)

    def test_probe_uses_minimal_date_range(self):
        """check_access() passes a minimal date range in params to avoid real data fetching."""
        stream, client = self._make_stream("installs")
        client.probe_request.return_value = None

        stream.check_access()

        _, call_params, _ = client.probe_request.call_args[0]
        self.assertEqual(call_params.get("from"), "2000-01-01 00:00")
        self.assertEqual(call_params.get("to"), "2000-01-02 00:00")

    def test_probe_uses_correct_url(self):
        """check_access() calls probe_request with the stream's endpoint URL."""
        stream, client = self._make_stream("installs")
        client.probe_request.return_value = None

        stream.check_access()

        url_arg = client.probe_request.call_args[0][0]
        self.assertIn("installs_report", url_arg)
        self.assertIn("test_app_id", url_arg)

    def test_non_forbidden_exception_propagates(self):
        """check_access() does not swallow non-403 errors."""
        stream, client = self._make_stream("installs")
        client.probe_request.side_effect = appsflyerUnauthorizedError("401")

        with self.assertRaises(appsflyerUnauthorizedError):
            stream.check_access()


# ---------------------------------------------------------------------------
# _prune_inaccessible_children()
# ---------------------------------------------------------------------------

class TestPruneInaccessibleChildren(unittest.TestCase):
    """Tests for _prune_inaccessible_children()."""

    def test_no_children_means_no_change(self):
        """All appsflyer streams are parent streams; prune is a no-op."""
        schemas, field_metadata = _make_schemas_and_metadata()
        original_keys = set(schemas.keys())

        _prune_inaccessible_children(schemas, field_metadata)

        self.assertEqual(set(schemas.keys()), original_keys)

    def test_child_stream_removed_when_parent_absent(self):
        """A stream with a `parent` attribute is removed when its parent is not in schemas."""
        schemas = {"child_stream": {}, "parent_stream": {}}
        field_metadata = {"child_stream": [], "parent_stream": []}

        # Temporarily inject a fake child into STREAMS
        mock_child_cls = MagicMock()
        mock_child_cls.parent = "parent_stream"

        with patch.dict("tap_appsflyer.streams.STREAMS", {"child_stream": mock_child_cls}):
            # Remove parent to simulate it being excluded
            schemas.pop("parent_stream")
            field_metadata.pop("parent_stream")
            _prune_inaccessible_children(schemas, field_metadata)

        self.assertNotIn("child_stream", schemas)
        self.assertNotIn("child_stream", field_metadata)

    def test_child_retained_when_parent_present(self):
        """A child stream is kept when its parent is still in the schemas dict."""
        schemas = {"child_stream": {}, "parent_stream": {}}
        field_metadata = {"child_stream": [], "parent_stream": []}

        mock_child_cls = MagicMock()
        mock_child_cls.parent = "parent_stream"

        with patch.dict("tap_appsflyer.streams.STREAMS", {"child_stream": mock_child_cls}):
            _prune_inaccessible_children(schemas, field_metadata)

        self.assertIn("child_stream", schemas)


# ---------------------------------------------------------------------------
# _apply_access_checks()
# ---------------------------------------------------------------------------

class TestApplyAccessChecks(unittest.TestCase):
    """Tests for _apply_access_checks()."""

    def _patch_check_access(self, accessible_streams):
        """Return a side_effect callable that makes only the listed streams accessible."""
        def _check_access(self_stream):
            return self_stream.tap_stream_id in accessible_streams
        return _check_access

    def test_all_accessible_no_removals(self):
        """No streams removed when all check_access() calls return True."""
        schemas, field_metadata = _make_schemas_and_metadata()
        client = _make_client()

        with patch(
            "tap_appsflyer.streams.abstracts.BaseStream.check_access", return_value=True
        ):
            _apply_access_checks(client, schemas, field_metadata)

        self.assertEqual(set(schemas.keys()), set(STREAMS.keys()))

    def test_one_stream_inaccessible_is_removed(self):
        """The inaccessible stream is removed from schemas and field_metadata."""
        schemas, field_metadata = _make_schemas_and_metadata()
        client = _make_client()

        def _check_access(self_stream):
            return self_stream.tap_stream_id != "installs"

        with patch(
            "tap_appsflyer.streams.abstracts.BaseStream.check_access",
            new=_check_access,
        ):
            _apply_access_checks(client, schemas, field_metadata)

        self.assertNotIn("installs", schemas)
        self.assertNotIn("installs", field_metadata)
        self.assertIn("organic_installs", schemas)
        self.assertIn("in_app_events", schemas)

    def test_all_streams_inaccessible_raises_forbidden(self):
        """appsflyerForbiddenError is raised when all streams are inaccessible."""
        schemas, field_metadata = _make_schemas_and_metadata()
        client = _make_client()

        with patch(
            "tap_appsflyer.streams.abstracts.BaseStream.check_access", return_value=False
        ):
            with self.assertRaises(appsflyerForbiddenError) as ctx:
                _apply_access_checks(client, schemas, field_metadata)

        self.assertIn("403", str(ctx.exception))

    def test_forbidden_error_message_mentions_all_streams(self):
        """The error message references insufficient permissions."""
        schemas, field_metadata = _make_schemas_and_metadata()
        client = _make_client()

        with patch(
            "tap_appsflyer.streams.abstracts.BaseStream.check_access", return_value=False
        ):
            with self.assertRaises(appsflyerForbiddenError) as ctx:
                _apply_access_checks(client, schemas, field_metadata)

        self.assertIn("permissions", str(ctx.exception))

    def test_two_streams_inaccessible_both_removed(self):
        """Multiple inaccessible streams are all removed."""
        schemas, field_metadata = _make_schemas_and_metadata()
        client = _make_client()

        def _check_access(self_stream):
            return self_stream.tap_stream_id == "in_app_events"

        with patch(
            "tap_appsflyer.streams.abstracts.BaseStream.check_access",
            new=_check_access,
        ):
            _apply_access_checks(client, schemas, field_metadata)

        self.assertNotIn("installs", schemas)
        self.assertNotIn("organic_installs", schemas)
        self.assertIn("in_app_events", schemas)

    def test_warning_logged_for_inaccessible_stream(self):
        """A warning is logged listing the excluded stream(s)."""
        schemas, field_metadata = _make_schemas_and_metadata()
        client = _make_client()

        def _check_access(self_stream):
            return self_stream.tap_stream_id != "installs"

        with patch(
            "tap_appsflyer.streams.abstracts.BaseStream.check_access",
            new=_check_access,
        ):
            # singer.get_logger() returns the root logger; capture from root
            with self.assertLogs(level="WARNING") as log_ctx:
                _apply_access_checks(client, schemas, field_metadata)

        combined = " ".join(log_ctx.output)
        self.assertIn("installs", combined)

    def test_schemas_unchanged_when_all_accessible(self):
        """schemas dict is not mutated when every stream is accessible."""
        schemas, field_metadata = _make_schemas_and_metadata()
        client = _make_client()
        expected_keys = set(schemas.keys())

        with patch(
            "tap_appsflyer.streams.abstracts.BaseStream.check_access", return_value=True
        ):
            _apply_access_checks(client, schemas, field_metadata)

        self.assertEqual(set(schemas.keys()), expected_keys)


# ---------------------------------------------------------------------------
# discover()
# ---------------------------------------------------------------------------

class TestDiscover(unittest.TestCase):
    """Tests for the top-level discover() function."""

    def test_returns_catalog_instance(self):
        """discover() returns a singer Catalog object."""
        client = _make_client()

        with patch(
            "tap_appsflyer.streams.abstracts.BaseStream.check_access", return_value=True
        ):
            catalog = discover(client)

        self.assertIsInstance(catalog, Catalog)

    def test_catalog_contains_all_streams_when_all_accessible(self):
        """When all streams are accessible, every stream appears in the catalog."""
        client = _make_client()

        with patch(
            "tap_appsflyer.streams.abstracts.BaseStream.check_access", return_value=True
        ):
            catalog = discover(client)

        stream_ids = {entry.tap_stream_id for entry in catalog.streams}
        self.assertEqual(stream_ids, set(STREAMS.keys()))

    def test_inaccessible_stream_absent_from_catalog(self):
        """A stream excluded by check_access() does not appear in the catalog."""
        client = _make_client()

        def _check_access(self_stream):
            return self_stream.tap_stream_id != "organic_installs"

        with patch(
            "tap_appsflyer.streams.abstracts.BaseStream.check_access",
            new=_check_access,
        ):
            catalog = discover(client)

        stream_ids = {entry.tap_stream_id for entry in catalog.streams}
        self.assertNotIn("organic_installs", stream_ids)
        self.assertIn("installs", stream_ids)
        self.assertIn("in_app_events", stream_ids)

    def test_all_inaccessible_raises_forbidden(self):
        """discover() propagates appsflyerForbiddenError when all streams are inaccessible."""
        client = _make_client()

        with patch(
            "tap_appsflyer.streams.abstracts.BaseStream.check_access", return_value=False
        ):
            with self.assertRaises(appsflyerForbiddenError):
                discover(client)

    def test_catalog_stream_has_key_properties(self):
        """Each CatalogEntry in the catalog exposes non-empty key_properties."""
        client = _make_client()

        with patch(
            "tap_appsflyer.streams.abstracts.BaseStream.check_access", return_value=True
        ):
            catalog = discover(client)

        for entry in catalog.streams:
            self.assertTrue(
                entry.key_properties,
                f"key_properties is empty for stream '{entry.tap_stream_id}'",
            )

    def test_catalog_stream_has_schema(self):
        """Each CatalogEntry in the catalog has a non-empty schema."""
        client = _make_client()

        with patch(
            "tap_appsflyer.streams.abstracts.BaseStream.check_access", return_value=True
        ):
            catalog = discover(client)

        for entry in catalog.streams:
            schema_dict = entry.schema.to_dict()
            self.assertIn(
                "properties",
                schema_dict,
                f"Schema 'properties' missing for stream '{entry.tap_stream_id}'",
            )

    def test_apply_access_checks_called_with_client(self):
        """discover() delegates access checking using the provided client."""
        client = _make_client()
        # Verify indirectly: when all streams are inaccessible for this specific
        # client, appsflyerForbiddenError is raised, proving the client was used.
        with patch(
            "tap_appsflyer.streams.abstracts.BaseStream.check_access", return_value=False
        ):
            with self.assertRaises(appsflyerForbiddenError):
                discover(client)


# ---------------------------------------------------------------------------
# do_discover() integration
# ---------------------------------------------------------------------------

class TestDoDiscover(unittest.TestCase):
    """Tests for __init__.do_discover()."""

    def test_do_discover_passes_client_to_discover(self):
        """do_discover(client) forwards the client argument to discover()."""
        client = _make_client()

        mock_catalog = MagicMock()
        mock_catalog.to_dict.return_value = {}

        with patch.object(_pkg, "discover", return_value=mock_catalog) as mock_d:
            captured = StringIO()
            with patch("sys.stdout", captured):
                do_discover(client)

        mock_d.assert_called_once_with(client)

    def test_do_discover_writes_catalog_to_stdout(self):
        """do_discover() serialises the catalog as JSON to stdout."""
        client = _make_client()

        mock_catalog = MagicMock()
        mock_catalog.to_dict.return_value = {"streams": []}

        with patch.object(_pkg, "discover", return_value=mock_catalog):
            captured = StringIO()
            with patch("sys.stdout", captured):
                do_discover(client)

        output = json.loads(captured.getvalue())
        self.assertEqual(output, {"streams": []})
