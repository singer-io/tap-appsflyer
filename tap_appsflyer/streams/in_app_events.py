from typing import Dict, Iterator, List

from singer import Transformer, get_logger, metrics, write_record
from singer.utils import strftime, strptime_to_utc

from tap_appsflyer.streams.abstracts import IncrementalStream

LOGGER = get_logger()

class InAppEvents(IncrementalStream):
    tap_stream_id = "in_app_events"
    key_properties = ["contact_id"]
    replication_keys = ["event_time"]
    data_key = "data_key_value_3"
    path = "/api/raw-data/export/app/{app_id}/in_app_events_report/v5"
