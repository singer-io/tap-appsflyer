#!/usr/bin/env python3

import datetime
import itertools
import os
import re
import sys

import attr
import backoff
import requests
import singer
from singer import utils

from glue_job_libs import general_utils as gu
from glue_job_libs import s3_util as s3

import json

from tap_appsflyer import credentials
LOGGER = singer.get_logger()
SESSION = requests.Session()


CONFIG = {
    "app_id": None,
    "api_token": None,
}


STATE = {}


ENDPOINTS = {
    "installs": "/export/{app_id}/installs_report/v5",
    "in_app_events": "/export/{app_id}/in_app_events_report/v5"
}


s3_client = s3.AWSS3()



def af_datetime_str_to_datetime(s):
    return datetime.datetime.strptime(s.strip(), "%Y-%m-%d %H:%M:%S")


def get_start(app_id, report_type):
    if report_type in STATE["bookmarks"].get(app_id):
        return utils.strptime(STATE["bookmarks"][app_id][report_type])

    if "start_date" in CONFIG:
        return utils.strptime(CONFIG["start_date"])

    return datetime.datetime.now() - datetime.timedelta(days=30)


def get_stop(start_datetime, stop_time, days=30):
    return min(start_datetime + datetime.timedelta(days=days), stop_time)


def get_base_url():
    if "base_url" in CONFIG:
        return CONFIG["base_url"]
    else:
        return "https://hq.appsflyer.com"


def get_url(endpoint, **kwargs):
    if endpoint not in ENDPOINTS:
        raise ValueError("Invalid endpoint {}".format(endpoint))
    else:
        return get_base_url() + ENDPOINTS[endpoint].format(**kwargs)


@attr.s
class Stream(object):
    name = attr.ib()
    sync = attr.ib()


def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)


def update_state_file(state):
    with open(get_abs_path(CONFIG["state_file_path"]), 'w') as f:
        json.dump(state, f)
    s3_client.upload_file_to_s3(CONFIG["job_config_bucket"],  get_abs_path(CONFIG["state_file_path"]), CONFIG["state_file_path"])


def load_schema(entity_name):
    schema = utils.load_json(get_abs_path('schemas/{}.json'.format(entity_name)))
    return schema


def giveup(exc):
    return exc.response is not None and 400 <= exc.response.status_code < 500


def parse_source_from_url(url):
    url_regex = re.compile(get_base_url() + r'.*/(\w+)_report/v5')
    match = url_regex.match(url)
    if match:
        return match.group(1)
    return None


def get_csv_file_name(from_date, to_date, stream_name):
    salt = datetime.datetime.now().strftime('%Y%m%d%H%M%S%f')
    export_file = 'landing/{}/{}{}_{}_{}_{}.csv'.format(stream_name, gu.get_partition_name_by_date(datetime.datetime.now()), stream_name, from_date, to_date, salt)
    output_dir = os.path.dirname(export_file)
    if not os.path.exists(get_abs_path(output_dir)) and output_dir != '':
        os.makedirs(get_abs_path(output_dir))
    return export_file


@backoff.on_exception(backoff.expo,
                      (requests.exceptions.RequestException),
                      max_tries=5,
                      giveup=giveup,
                      factor=2)
@utils.ratelimit(10, 1)
def request(url, params=None):

    params = params or {}
    headers = {}

    if "user_agent" in CONFIG:
        headers["User-Agent"] = CONFIG["user_agent"]

    req = requests.Request("GET", url, params=params, headers=headers).prepare()
    LOGGER.info("GET %s", req.url)

    with singer.metrics.http_request_timer(endpoint=parse_source_from_url(url)) as stats:
        resp = SESSION.send(req)

    if resp.status_code >= 400:
        LOGGER.error("GET %s [%s - %s]", req.url, resp.status_code, resp.content)
        sys.exit(1)

    return resp


class RequestToCsvAdapter:
    def __init__(self, request_data):
        self.request_data_iter = request_data.iter_lines();

    def __iter__(self):
        return self

    def __next__(self):
        return next(self.request_data_iter).decode("utf-8")


def sync_installs():
    stop_time = datetime.datetime.now()
    from_datetime = get_start(CONFIG['app_id'], "installs")
    to_datetime = get_stop(from_datetime, stop_time, 10)

    while from_datetime < stop_time:

        if to_datetime < from_datetime:
            LOGGER.error("to_datetime (%s) is less than from_endtime (%s).", to_datetime, from_datetime)
            return

        params = dict()
        params["from"] = from_datetime.strftime("%Y-%m-%d %H:%M")
        params["to"] = to_datetime.strftime("%Y-%m-%d %H:%M")
        params["api_token"] = CONFIG["api_token"]

        url = get_url("installs", app_id=CONFIG["app_id"])
        request_data = request(url, params)

        output_csv_file = get_csv_file_name(
            from_datetime.strftime("%Y%m%d%H%M"),
            to_datetime.strftime("%Y%m%d%H%M"),
            "installs"
        )
        with open(get_abs_path(output_csv_file), 'wb') as f:
            f.write(request_data.content)

        s3_client.upload_file_to_s3(CONFIG["destination_bucket"], get_abs_path(output_csv_file), output_csv_file)

        bookmark = datetime.datetime.strftime(to_datetime, utils.DATETIME_PARSE)

        # Write out state
        utils.update_state(STATE['bookmarks'][CONFIG['app_id']], "installs", bookmark)
        update_state_file(STATE)

        # Move the timings forward
        from_datetime = to_datetime
        to_datetime = get_stop(from_datetime, stop_time, 10)


def sync_in_app_events():

    stop_time = datetime.datetime.now()
    from_datetime = get_start(CONFIG['app_id'], "in_app_events")
    to_datetime = get_stop(from_datetime, stop_time, 10)

    while from_datetime < stop_time:
        LOGGER.info("Syncing data from %s to %s", from_datetime, to_datetime)
        params = dict()
        params["from"] = from_datetime.strftime("%Y-%m-%d %H:%M")
        params["to"] = to_datetime.strftime("%Y-%m-%d %H:%M")
        params["api_token"] = CONFIG["api_token"]

        url = get_url("in_app_events", app_id=CONFIG["app_id"])
        request_data = request(url, params)

        output_csv_file = get_csv_file_name(
            from_datetime.strftime("%Y%m%d%H%M"),
            to_datetime.strftime("%Y%m%d%H%M"),
            "in_app_events"
        )

        with open(get_abs_path(output_csv_file), 'wb') as f:
            f.write(request_data.content)

        s3_client.upload_file_to_s3(CONFIG["destination_bucket"], get_abs_path(output_csv_file), output_csv_file)

        # Write out state
        bookmark = datetime.datetime.strftime(to_datetime, utils.DATETIME_PARSE)
        utils.update_state(STATE['bookmarks'][CONFIG['app_id']], "in_app_events", bookmark)
        update_state_file(STATE)

        # Move the timings forward
        from_datetime = to_datetime
        to_datetime = get_stop(from_datetime, stop_time, 10)


STREAMS = [
    Stream("installs", sync_installs),
    Stream("in_app_events", sync_in_app_events)
]


def get_streams_to_sync(streams, state):
    target_stream = state.get("this_stream")
    result = streams
    if target_stream:
        result = list(itertools.dropwhile(lambda x: x.name != target_stream, streams))
    if not result:
        raise Exception('Unknown stream {} in state'.format(target_stream))
    return result


def do_sync():
    LOGGER.info("do_sync()")
    streams = get_streams_to_sync(STREAMS, STATE)
    LOGGER.info('Starting sync. Will sync these streams: %s', [stream.name for stream in streams])
    for stream in streams:
        LOGGER.info('Syncing %s', stream.name)
        stream.sync() # pylint: disable=not-callable
    singer.write_state(STATE)
    LOGGER.info("Sync completed")


def main(bucket, config_file, state_file):

    all_apps = [
        'com.transfergo.android',
        'id1110641576'
    ]

    conn_info = credentials.Config()

    local_file = get_abs_path(state_file)
    gu.create_dir_if_not_exists(local_file)
    s3_client.download_file(bucket, state_file, local_file)
    STATE.update(utils.load_json(local_file))

    job_config = s3_client.get_s3_file(bucket=bucket, file=config_file)
    job_config = json.loads(job_config)

    CONFIG["api_token"] = conn_info.access_token
    CONFIG["destination_bucket"] = job_config['destination_bucket']
    CONFIG["state_file_path"] = state_file
    CONFIG["job_config_bucket"] = bucket


    for app in all_apps:
        CONFIG["app_id"] = app
        do_sync()


if __name__ == '__main__':
    main(bucket='aws-glue-scripts-082806765249-eu-west-1', config_file='appsflyer/config/config.json', state_file='appsflyer/config/state.json')
