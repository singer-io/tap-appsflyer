import sys
import csv
import backoff
import itertools
import requests
import singer
import singer.metrics
from singer import transform
from singer import utils
from datetime import datetime,timedelta


LOGGER = singer.get_logger()
SESSION = requests.Session()

API_LIMITS = {
    'in_app_events_report': 31, # days
    'installs_report': 60,
    'uninstalls_report': 60
}


class RequestToCsvAdapter:
    def __init__(self,request_data):
        self.request_data_iter = request_data.iter_lines()

    def __iter__(self):
        return self

    def __next__(self):
        return next(self.request_data_iter).decode("utf-8")

    def giveup(self,exc):
        return exc.response is not None and 400 <= exc.response.status_code < 500


class AppsflyerClient():
    # API rate limits: https://support.appsflyer.com/hc/en-us/articles/207034366-API-Policy

    def __init__(self, config):
        self.config = config
        self.base_url = "https://hq.appsflyer.com"
        self.base_url_path = "export/{app_id}"
    

    def _get_request_intervals(self,report_name,from_datetime,to_datetime):
        # calculate delta in seconds and divide by seconds limit
        if report_name not in API_LIMITS:
            LOGGER.error("API Limit not declared for report name: {0}".format(report_name))
            sys.exit(1)
        
        delta = to_datetime - from_datetime
        delta_secs = delta.total_seconds()
        limit_secs = API_LIMITS[report_name]*60*60*24
        q = int(delta_secs/limit_secs)
        r = delta_secs%limit_secs
        
        # create a list with date intervals
        from_param,to_param=from_datetime,to_datetime
        intervals = []
        while q >0 or r > 0:
            if q > 0:
                to_param = from_param + timedelta(seconds=limit_secs)
                intervals.append({'from': from_param, 'to': to_param})
                from_param = to_param + timedelta(minutes=1)
                q = q - 1
            if q == 0 and r != 0:
                to_param = to_datetime
                intervals.append({'from': from_param, 'to': to_param})
                r = 0
                
        return intervals


    @backoff.on_exception(backoff.expo,
                      (requests.exceptions.RequestException),
                      max_tries=5,
                      # giveup=giveup,
                      factor=2)
    @utils.ratelimit(2, 60)
    def _request(self,url, params=None):

        params = params or {}
        headers = {}

        if "user_agent" in self.config:
            headers["User-Agent"] = self.config["user_agent"]

        req = requests.Request("GET", url, params=params, headers=headers).prepare()
        LOGGER.info("GET {0} | Date interval: from {1} to {2}".format(
            url,params["from"],params["to"]))
        
        resp = SESSION.send(req)
        resp.status_code

        if resp.status_code >= 400:
            LOGGER.error("GET %s [%s - %s]", url, resp.status_code, resp.content)
            sys.exit(1)

        return resp

    
    def _get_url(self,report_name,report_version):
        return "/".join([
            self.base_url,
            self.base_url_path.format(app_id=self.config["app_id"]),
            report_name,
            report_version
        ])

    
    def _parse_raw_api_params(self,from_datetime,to_datetime):
        params = dict()
        params["from"] = from_datetime.strftime("%Y-%m-%d %H:%M")
        params["to"] = to_datetime.strftime("%Y-%m-%d %H:%M")
        params["api_token"] = self.config["api_token"]
        
        return params


    def get_raw_data(self,report_name,report_version,from_datetime,to_datetime,fieldnames):
        # Raw data: https://support.appsflyer.com/hc/en-us/articles/360007530258-Using-Pull-API-raw-data

        req_intervals = self._get_request_intervals(report_name,from_datetime,to_datetime)        
        csv_data_chained = []

        for req_interval in req_intervals:
            url = self._get_url(report_name,report_version)
            params = self._parse_raw_api_params(req_interval['from'],req_interval['to'])

            request_data = self._request(url, params)
            
            csv_data = RequestToCsvAdapter(request_data)
            next(csv_data) # Skip the heading row
            csv_data_chained = itertools.chain(csv_data_chained,csv_data)
        
        reader = csv.DictReader(csv_data_chained, fieldnames)
        
        return reader


    def get_daily_report(self,start_datetime,end_datetime):
        # Agg Data: https://support.appsflyer.com/hc/en-us/articles/207034346-Using-Pull-API-aggregate-data
        pass

