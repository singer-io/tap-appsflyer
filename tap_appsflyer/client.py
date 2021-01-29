import sys
import csv
import backoff
import requests
import singer
import singer.metrics
from singer import transform
from singer import utils


LOGGER = singer.get_logger()
SESSION = requests.Session()


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
        LOGGER.info("GET %s", req.url)

        
        resp = SESSION.send(req)
        resp.status_code

        if resp.status_code >= 400:
            LOGGER.error("GET %s [%s - %s]", req.url, resp.status_code, resp.content)
            sys.exit(1)

        return resp

    
    def _get_url(self,endpoint):
        return self.base_url + endpoint.format(app_id=self.config["app_id"])

    
    def _parse_raw_api_params(self,from_datetime,to_datetime):
        params = dict()
        params["from"] = from_datetime.strftime("%Y-%m-%d %H:%M")
        params["to"] = to_datetime.strftime("%Y-%m-%d %H:%M")
        params["api_token"] = self.config["api_token"]
        
        return params


    def get_raw_data(self,endpoint,from_datetime,to_datetime,fieldnames):
        # Raw data: https://support.appsflyer.com/hc/en-us/articles/360007530258-Using-Pull-API-raw-data
        
        url = self._get_url(endpoint)
        params = self._parse_raw_api_params(from_datetime,to_datetime)

        request_data = self._request(url, params)
        
        csv_data = RequestToCsvAdapter(request_data)
        reader = csv.DictReader(csv_data, fieldnames)

        next(reader) # Skip the heading row
        
        return reader


    def get_daily_report(self,start_datetime,end_datetime):
        # Agg Data: https://support.appsflyer.com/hc/en-us/articles/207034346-Using-Pull-API-aggregate-data
        pass

        



