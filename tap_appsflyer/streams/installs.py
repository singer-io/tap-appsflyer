from typing import Dict, Iterator, List

from singer import Transformer, get_logger, metrics, write_record
from singer.utils import strftime, strptime_to_utc

from tap_appsflyer.streams.abstracts import IncrementalStream

LOGGER = get_logger()


class Installs(IncrementalStream):
    tap_stream_id = "installs"
    key_properties = ["event_time", "event_name", "appsflyer_id"]
    replication_keys = ["attributed_touch_time"]
    path = "api/raw-data/export/app/{}/installs_report/v5"

    def get_url_endpoint(self) -> str:
        return f"{self.client.base_url}/{self.path.format(self.client.config.get('app_id'))}"
