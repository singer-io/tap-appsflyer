
import pandas
import io
import datetime
import singer

LOGGER = singer.get_logger()

class DataParser:
    def __init__(self):
        LOGGER.info("Starting transformation...")

    @staticmethod
    def get_event_type_from_file_path(file_path):
        event_type = file_path.split("/")[1]
        if event_type not in (['installs', 'installs_retargeting', 'in_app_events_retargeting', 'in_app_events', 'organic_installs', 'organic_in_app_events']):
            raise ValueError('Unknown event type {}'.format(event_type))
        return event_type

    @staticmethod
    def get_table_name_from_file_key(file_key):
        table_name = file_key.split("/")[-1]
        return table_name

    @staticmethod
    def string_to_datetime(date_time_str):
        return datetime.datetime.strptime(date_time_str, '%Y-%m-%d %H:%M:%S')

    @staticmethod
    def datetime_to_string(date_time_value):
        return date_time_value.strftime('%Y-%m-%dT%H:%M:%S')

    def change_partition_key_format(self, partition_key):
        return self.datetime_to_string(self.string_to_datetime(partition_key))

    def parse_raw_data(self, raw_file_content, raw_file_key):

        list_of_df = {}
        table = 'appsflyer_events'

        raw_file = io.BytesIO(raw_file_content['Body'].read())
        df = pandas.read_csv(raw_file, skiprows=0, dtype=str)

        df["event_type"] = self.get_event_type_from_file_path(raw_file_key)

        # Add partition key column. Requared by the contract
        df["partition_key_dt"] = df["Event Time"].apply(lambda x: self.change_partition_key_format(x))

        # Do some validation if needed and set variable "is_parsed" to True is there is no errors.
        list_of_df.update({table: {"df": df}})

        is_parsed = True
        return list_of_df, is_parsed











