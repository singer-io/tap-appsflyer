from typing import Dict, Iterator, List

from singer import Transformer, get_logger, metrics, write_record
from singer.utils import strftime, strptime_to_utc

from tap_appsflyer.streams.abstracts import IncrementalStream

LOGGER = get_logger()

class OrganicInstalls(IncrementalStream):
    tap_stream_id = "organic_installs"
    key_properties = ["contact_id"]
    replication_keys = ["event_time"]
    data_key = "data_key_value_2"
    path = "api/raw-data/export/app/{}/organic_installs_report/v5"

    def get_url_endpoint(self) -> str:
        return f"{self.client.base_url}/{self.path.format(self.client.config['app_id'])}"
