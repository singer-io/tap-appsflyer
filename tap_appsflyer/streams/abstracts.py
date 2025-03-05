import csv
import datetime
import pytz
import re
import requests

from abc import ABC, abstractmethod
from typing import Any, Dict, Tuple, List

import singer
from singer import (
    Transformer,
    get_bookmark,
    get_logger,
    metrics,
    write_bookmark,
    write_record,
    write_schema,
    utils
)
from singer.utils import strftime, strptime_to_utc

LOGGER = get_logger()


SESSION = requests.Session()

# This order matters
fieldnames = (
        "attributed_touch_type",
        "attributed_touch_time",
        "install_time",
        "event_time",
        "event_name",
        "event_value",
        "event_revenue",
        "event_revenue_currency",
        "event_revenue_usd",
        "event_source",
        "is_receipt_validated",
        "af_prt",
        "media_source",
        "af_channel",
        "af_keywords",
        "campaign",
        "af_c_id",
        "af_adset",
        "af_adset_id",
        "af_ad",
        "af_ad_id",
        "af_ad_type",
        "af_siteid",
        "af_sub_siteid",
        "af_sub1",
        "af_sub2",
        "af_sub3",
        "af_sub4",
        "af_sub5",
        "af_cost_model",
        "af_cost_value",
        "af_cost_currency",
        "contributor1_af_prt",
        "contributor1_media_source",
        "contributor1_campaign",
        "contributor1_touch_type",
        "contributor1_touch_time",
        "contributor2_af_prt",
        "contributor2_media_source",
        "contributor2_campaign",
        "contributor2_touch_type",
        "contributor2_touch_time",
        "contributor3_af_prt",
        "contributor3_media_source",
        "contributor3_campaign",
        "contributor3_touch_type",
        "contributor3_touch_time",
        "region",
        "country_code",
        "state",
        "city",
        "postal_code",
        "dma",
        "ip",
        "wifi",
        "operator",
        "carrier",
        "language",
        "appsflyer_id",
        "advertising_id",
        "idfa",
        "android_id",
        "customer_user_id",
        "imei",
        "idfv",
        "platform",
        "device_type",
        "os_version",
        "app_version",
        "sdk_version",
        "app_id",
        "app_name",
        "bundle_id",
        "is_retargeting",
        "retargeting_conversion_type",
        "af_attribution_lookback",
        "af_reengagement_window",
        "is_primary_attribution",
        "user_agent",
        "http_referrer",
        "original_url",
    )

class BaseStream(ABC):
    """
    A Base Class providing structure and boilerplate for generic streams
    and required attributes for any kind of stream
    ~~~
    Provides:
     - Basic Attributes (stream_name,replication_method,key_properties)
     - Helper methods for catalog generation
     - `sync` and `get_records` method for performing sync
    """

    url_endpoint = ""
    path = ""
    next_page_key = "next_page"
    headers = {"Accept": "application/json"}

    def __init__(self, client=None) -> None:
        self.client = client
        self.params = {}

    @property
    @abstractmethod
    def tap_stream_id(self) -> str:
        """Unique identifier for the stream.

        This is allowed to be different from the name of the stream, in
        order to allow for sources that have duplicate stream names.
        """

    @property
    @abstractmethod
    def replication_method(self) -> str:
        """Defines the sync mode of a stream."""

    @property
    @abstractmethod
    def replication_keys(self) -> str:
        """Defines the replication key for incremental sync mode of a
        stream."""

    @property
    @abstractmethod
    def forced_replication_method(self) -> str:
        """Defines the sync mode of a stream."""

    @property
    @abstractmethod
    def key_properties(self) -> Tuple[str, str]:
        """List of key properties for stream."""

    @property
    def selected_by_default(self) -> bool:
        """Indicates if a node in the schema should be replicated, if a user
        has not expressed any opinion on whether or not to replicate it."""
        return False

    def get_url_endpoint(self) -> str:
        """
        Get the URL endpoint for the stream
        """
        return self.url_endpoint

    @abstractmethod
    def sync(
        self, state: Dict, schema: Dict, stream_metadata: Dict, transformer: Transformer
    ) -> Dict:
        """
        Performs a replication sync for the stream.
        ~~~
        Args:
         - state (dict): represents the state file for the tap.
         - schema (dict): Schema of the stream
         - transformer (object): A Object of the singer.transformer class.

        Returns:
         - bool: The return value. True for success, False otherwise.

        Docs:
         - https://github.com/singer-io/getting-started/blob/master/docs/SYNC_MODE.md
        """

    def parse_source_from_url(self, base_url):
        url_regex = re.compile(base_url + r".*/(\w+)_report/v5")
        match = url_regex.match(base_url)
        if match:
            return match.group(1)
        return None

    def get_records(self) -> List:
        """Interacts with api client interaction and pagination."""
        extraction_url = self.url_endpoint
        page_count = 1

        while True:
            response = self.client.get(
                extraction_url, self.params, self.headers
            )
            if not response:
                LOGGER.warning("No records found on Page %s", page_count)
                break

            with singer.metrics.http_request_timer(self.parse_source_from_url(self.client.base_url)) as timer:
                resp = SESSION.send(response)
                timer.tags[singer.metrics.Tag.http_status_code] = resp.status_code
            return resp

    def write_schema(self, schema, stream_name):
        """
        Write a schema message.
        """
        try:
            write_schema(stream_name, schema, self.key_properties)
        except OSError as err:
            LOGGER.error("OS Error while writing schema for: {}".format(stream_name))
            raise err

class RequestToCsvAdapter:
    def __init__(self, request_data):
        self.request_data_iter = request_data.iter_lines();

    def __iter__(self):
        return self

    def __next__(self):
        return next(self.request_data_iter).decode("utf-8")

class IncrementalStream(BaseStream):
    """Base Class for Incremental Stream."""

    replication_method = "INCREMENTAL"
    forced_replication_method = "INCREMENTAL"
    config_start_key = "start_date"

    @staticmethod
    def get_restricted_start_date(date: str) -> datetime.datetime:
        # https://support.appsflyer.com/hc/en-us/articles/207034366-API-Policy
        restriction_date = utils.now() - datetime.timedelta(days=90)
        start_date = strptime_to_utc(date)

        return max(start_date, restriction_date)

    def get_bookmark(self, state: dict, key: Any = None) -> int:
        """A wrapper for singer.get_bookmark to deal with compatibility for
        bookmark values or start values."""
        get_bookmark_value =  get_bookmark(
            state,
            self.tap_stream_id,
            key or self.replication_keys[0],
            self.client.config.get(self.config_start_key, False),
        )

        if get_bookmark_value:
            return  self.get_restricted_start_date(get_bookmark_value)

        LOGGER.warning("No bookmark value found, using default start date i.e. 30 days")
        return datetime.datetime.now(pytz.utc) - datetime.timedelta(days=30)

    def get_stop(self, start_datetime, stop_time, days=30):
        if isinstance(start_datetime, datetime.datetime):
            # Ensure we are working with datetime objects
            stop_time = min(start_datetime + datetime.timedelta(days=days), stop_time)
            return stop_time
        else:
            raise TypeError(f"Expected start_datetime to be a datetime object, got {type(start_datetime)}")

    def write_bookmark(self, state: dict, key: Any = None, value: Any = None) -> Dict:
        """A wrapper for singer.get_bookmark to deal with compatibility for
        bookmark values or start values."""
        return write_bookmark(
            state, self.tap_stream_id, key or self.replication_keys[0], value
        )

    def xform_boolean_field(self, record, field_name):
        value = record[field_name]
        if value is None:
            return

        if value.lower() == "TRUE".lower():
            record[field_name] = True
        else:
            record[field_name] = False


    def xform_empty_strings_to_none(self, record):
        for key, value in record.items():
            if value == "":
                record[key] = None

    def xform(self, record):
        self.xform_empty_strings_to_none(record)
        self.xform_boolean_field(record, "wifi")
        self.xform_boolean_field(record, "is_retargeting")
        return record

    def sync(
        self, state: Dict, schema: Dict, stream_metadata: Dict, transformer: Transformer
    ) -> Dict:
        self.url_endpoint = self.get_url_endpoint()

        from_datetime = bookmark_date =self.get_bookmark(state)
        to_datetime = self.get_stop(from_datetime, datetime.datetime.now(pytz.utc))

        current_max_bookmark_date = bookmark_date_to_utc = bookmark_date


        self.params["from"] = from_datetime.strftime("%Y-%m-%d %H:%M")
        self.params["to"] = to_datetime.strftime("%Y-%m-%d %H:%M")

        with metrics.record_counter(self.tap_stream_id) as counter:
            request_data = self.get_records()
            csv_data = RequestToCsvAdapter(request_data)
            try:
                first_line = next(csv_data)
                # Push the line back into the iterator
                csv_data = iter([first_line] + list(csv_data))
            except StopIteration:
                LOGGER.warning("No data available in the CSV.")
                return state
            reader = csv.DictReader(csv_data, fieldnames)

            try:
                next(reader)  # Skip the header row
            except StopIteration:
                LOGGER.warning("No data available after header row.")
                return state

            for i, row in enumerate(reader):
                xform_record = self.xform(row)
                transformed_record = transformer.transform(
                    xform_record, schema, stream_metadata
                )
                try:
                    record_timestamp = strptime_to_utc(
                        transformed_record[self.replication_keys[0]]
                    )
                except KeyError as _:
                    LOGGER.error(
                        "Unable to process Record, Exception occurred: %s for stream %s",
                        _,
                        self.__class__,
                    )
                    continue
                if record_timestamp >= bookmark_date_to_utc:
                    write_record(self.tap_stream_id, transformed_record)
                    current_max_bookmark_date = max(
                        current_max_bookmark_date, record_timestamp
                    )
                    counter.increment()

            state = self.write_bookmark(
                state, value=strftime(current_max_bookmark_date)
            )
            return counter.value
