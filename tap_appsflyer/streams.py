import singer
from .transform import *
from datetime import datetime, timedelta, timezone

LOGGER = singer.get_logger()
RAW_BOOKMARK_DATE_FORMAT = '%Y-%m-%dT%H:%MZ'
RAW_REPORTS_API_MAX_WINDOW = 90 #days


class Stream:
    def __init__(self,client,config):
        self.client = client
        self.config = config


class RawData(Stream):

    def _get_start_time(self,state,bookmark_format):
        # if start_date is in the config use it, if not, get 90 days ago
        if "start_date" in self.config:
            start_date = datetime.strptime(self.config["start_date"],RAW_BOOKMARK_DATE_FORMAT)
        else:
            start_date = singer.utils.now().replace(hour=0,minute=0,second=0,microsecond=0) - timedelta(days=RAW_REPORTS_API_MAX_WINDOW)

        # get bookmark
        start_time_str = singer.get_bookmark(
            state,
            self.tap_stream_id,
            self.replication_key,
            datetime.strftime(start_date,bookmark_format)
            )
        
        # add timezone UTC 0 without changing the date time
        start_time = datetime.strptime(start_time_str,bookmark_format).\
            replace(tzinfo=timezone.utc)
        
        return start_time
    
    def _get_end_time(self,bookmark_format):
        end_time = None
        if "end_date" in self.config:
            end_time = datetime.strptime(self.config["end_date"],RAW_BOOKMARK_DATE_FORMAT).\
                replace(tzinfo=timezone.utc)
        else:
            end_time = singer.utils.now().replace(second=0,microsecond=0)
        
        return end_time


    """Defines the sync method for all raw data classes as the raw data endpoints have 
        the same structure."""
    def sync(self, state, stream_schema, stream_metadata, transformer):

        # Bookmark is in timezone UTC
        start_time = self._get_start_time(state,RAW_BOOKMARK_DATE_FORMAT)
        end_time = self._get_end_time(RAW_BOOKMARK_DATE_FORMAT)

        for record in self.client.get_raw_data(
            self.report_name,
            self.report_version,
            start_time,
            end_time,     
            RAW_INSTALL_N_IN_APP_FIELDNAMES):

            transformed_record = transformer.transform(
                xform(record),
                stream_schema,
                stream_metadata)
            singer.write_record(self.tap_stream_id,transformed_record,time_extracted=end_time)

        # Convert to bookmark format
        end_time_str = datetime.strftime(end_time,RAW_BOOKMARK_DATE_FORMAT)
        state = singer.write_bookmark(state, self.tap_stream_id, self.replication_key, end_time_str)
        singer.write_state(state)

        return state


class Installs(RawData):

    tap_stream_id = 'installs'
    key_properties = ["event_time","event_name","appsflyer_id"]
    replication_method = 'INCREMENTAL'
    valid_replication_keys = ['event_time']
    replication_key = 'event_time'
    report_name = 'installs_report'
    report_version = 'v5'


class InAppEvents(RawData):

    tap_stream_id = 'in_app_events'
    key_properties = ["event_time","event_name","appsflyer_id"]
    replication_method = 'INCREMENTAL'
    valid_replication_keys = ['event_time']
    replication_key = 'event_time'
    report_name = 'in_app_events_report'
    report_version = 'v5'


STREAMS = {
    'installs': Installs,
    'in_app_events': InAppEvents
}

# This order matters
RAW_INSTALL_N_IN_APP_FIELDNAMES = (
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