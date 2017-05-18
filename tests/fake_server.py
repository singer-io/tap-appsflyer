import datetime
import io
import json
import random
import re
import uuid

from bottle import HTTPResponse
from bottle import abort
from bottle import request
from bottle import route
from bottle import run

from faker import Factory


# TODO: Add two-month timeframe limitation

headings = [
    # 0
    "Attributed Touch Type",
    "Attributed Touch Time",
    "Install Time",
    "Event Time",
    "Event Name",
    "Event Value",
    "Event Revenue",
    "Event Revenue Currency",
    "Event Revenue USD",
    "Event Source",
    # 10
    "Is Receipt Validated",
    "Partner",
    "Media Source",
    "Channel",
    "Keywords",
    "Campaign",
    "Campaign ID",
    "Adset",
    "Adset ID",
    "Ad",
    # 20
    "Ad ID",
    "Ad Type",
    "Site ID",
    "Sub Site ID",
    "Sub Param 1",
    "Sub Param 2",
    "Sub Param 3",
    "Sub Param 4",
    "Sub Param 5",
    "Cost Model",
    # 30
    "Cost Value",
    "Cost Currency",
    "Contributor 1 Partner",
    "Contributor 1 Media Source",
    "Contributor 1 Campaign",
    "Contributor 1 Touch Type",
    "Contributor 1 Touch Time",
    "Contributor 2 Partner",
    "Contributor 2 Media Source",
    "Contributor 2 Campaign",
    # 40
    "Contributor 2 Touch Type",
    "Contributor 2 Touch Time",
    "Contributor 3 Partner",
    "Contributor 3 Media Source",
    "Contributor 3 Campaign",
    "Contributor 3 Touch Type",
    "Contributor 3 Touch Time",
    "Region",
    "Country Code",
    "State",
    # 50
    "City",
    "Postal Code",
    "DMA",
    "IP",
    "WIFI",
    "Operator",
    "Carrier",
    "Language",
    "AppsFlyer ID",
    "Advertising ID",
    # 60
    "IDFA",
    "Android ID",
    "Customer User ID",
    "IMEI",
    "IDFV",
    "Platform",
    "Device Type",
    "OS Version",
    "App Version",
    "SDK Version",
    # 70
    "App ID",
    "App Name",
    "Bundle ID",
    "Is Retargeting",
    "Retargeting Conversion Type",
    "Attribution Lookback",
    "Reengagement Window",
    "Is Primary Attribution",
    "User Agent",
    "HTTP Referrer",
    # 80
    "Original URL"
]


def generate_installs_report(app_id, start_datetime, end_datetime):

    fake = Factory.create()

    current_datetime = end_datetime

    yield headings

    for _ in range(200, 300):
        record = [""] * len(headings) # Report has 81 columns

        # Attributed Touch Type, Attributed Touch Time
        record[0] = random.choice(["click", "impression", "tv"])
        record[1] = current_datetime.strftime("%Y-%m-%d %H:%M:%S") # Sorted on this field

        # Install Time & #
        record[2] = (current_datetime + datetime.timedelta(seconds=47)).strftime("%Y-%m-%d %H:%M:%S")

        # Event Time, Event Name, Event Source
        record[3] = record[2]
        record[4] = "install"
        record[9] = random.choice(["SDK", "S2S"])

        # Media Source, Channel, Keywords, Campaign, Campaign ID, Adset, Adset ID
        record[12] = "_".join(fake.words(3))
        record[13] = random.choice(["", "", "", "search"])
        record[14] = " ".join(fake.words(random.randint(1, 4)))
        record[15] = random.choice(["mobile", "web", "social"])
        record[16] = str(random.randint(1000000, 2000000))
        record[17] = random.choice(["mobile", "web", "social"])
        record[18] = str(random.randint(1000000, 2000000))

        # Region, Country Code, State, City, Postal Code
        record[47] = "NA"
        record[48] = "US"
        record[49] = fake.state_abbr()
        record[50] = fake.city()
        record[51] = fake.zipcode()

        # DMA, IP, WIFI
        record[52] = str(random.randint(100, 999))
        record[53] = ".".join([str(random.randint(100, 255))] * 4)
        record[54] = random.choice(["true", "false"])

        # Language
        record[57] = "en-US"

        # AppsFlyer ID
        record[58] = "1493" + str(random.randint(100000, 999999)) + "000-" + str(random.randint(100000, 999999))

        # IDFA, Customer User ID, IDFV
        record[60] = random.choice(["", str(uuid.uuid4())])
        record[62] = str(random.randint(1, 999999))
        record[64] = str(uuid.uuid4())

        # Platform, Device Type, OS Version
        record[65] = "ios"
        record[66] = "iPhone " + random.choice(["5", "6", "6 Plus", "7"])
        record[67] = random.choice(["10.2", "10.2.1", "10.3.1", "9.3.1"])

        # App Version
        record[68] = random.choice(["1.2", "1.3", "1.4"])

        # SDK Version
        record[69] = "v4.4.6.3"

        # App ID, App Name, Bundle ID
        record[70] = app_id
        record[71] = "Acme - Good Stuff"
        record[72] = "com.example.AcmeApp"

        # Is Retargeting
        record[73] = "false"

        # Attribution Lookback
        record[75] = random.choice(["", "7d", "30d"])

        # User Agent, HTTP Referrer, Original URL
        record[78] = "Acme/1 CFNetwork/808.3 Darwin/16.3.0"
        record[79] = random.choice(["", fake.uri()])
        record[80] = random.choice(["", fake.uri()])

        yield record

        current_datetime = current_datetime - datetime.timedelta(minutes=47)

        # Make sure we're not creating records out of the time range.
        if current_datetime < start_datetime:
            return


def generate_in_app_events_report(app_id, start_datetime, end_datetime):

    fake = Factory.create()

    current_datetime = end_datetime

    yield headings

    for _ in range(200, 300):
        record = [""] * len(headings) # Report has 81 columns

        # Attributed Touch Type, Attributed Touch Time
        record[0] = random.choice(["click", "impression", "tv"])
        record[1] = (current_datetime - datetime.timedelta(days=47)).strftime("%Y-%m-%d %H:%M:%S")

        # Install Time
        record[2] = record[1]

        # Event Time, Event Name, Event Source
        record[3] = current_datetime.strftime("%Y-%m-%d %H:%M:%S") # Sorted on this field
        record[4] = "af_purchase"

        # Event Value, Event Revenue, Event Revenue USD
        revenue = round(random.uniform(50.0, 2000.0), 2)
        record[5] = json.dumps({'af_revenue': revenue })
        record[6] = str(revenue)
        record[8] = str(revenue)

        # Event Source
        record[9] = random.choice(["SDK", "S2S"])

        # Media Source, Keywords, Campaign, Campaign ID, Adset, Adset ID
        record[12] = "_".join(fake.words(3))
        record[14] = " ".join(fake.words(random.randint(1, 4)))
        record[15] = random.choice(["mobile", "web", "social"])
        record[16] = str(random.randint(1000000, 2000000))
        record[17] = random.choice(["mobile", "web", "social"])
        record[18] = str(random.randint(1000000, 2000000))

        # Region, Country Code, State, City, Postal Code
        record[47] = "NA"
        record[48] = "US"
        record[49] = fake.state_abbr()
        record[50] = fake.city()
        record[51] = fake.zipcode()

        # DMA, IP, WIFI
        record[52] = str(random.randint(100, 999))
        record[53] = ".".join([str(random.randint(100, 255))] * 4)
        record[54] = random.choice(["true", "false"])

        # Language
        record[57] = "en-US"

        # AppsFlyer ID
        record[58] = "1493" + str(random.randint(100000, 999999)) + "000-" + str(random.randint(100000, 999999))

        # IDFA, Customer User ID, IDFV
        record[60] = random.choice(["", str(uuid.uuid4())])
        record[62] = str(random.randint(1, 999999))
        record[64] = str(uuid.uuid4())

        # Platform, Device Type, OS Version
        record[65] = "ios"
        record[66] = "iPhone " + random.choice(["5", "6", "6 Plus", "7"])
        record[67] = random.choice(["10.2", "10.2.1", "10.3.1", "9.3.1"])

        # App Version
        record[68] = random.choice(["1.2", "1.3", "1.4"])

        # SDK Version
        record[69] = "v4.4.6.3"

        # App ID, App Name, Bundle ID
        record[70] = app_id
        record[71] = "Acme - Good Stuff"
        record[72] = "com.example.AcmeApp"

        # Is Retargeting
        record[73] = "false"

        # Attribution Lookback
        record[75] = random.choice(["", "7d", "30d"])

        # Is Primary Attribution
        record[77] = "true"

        # User Agent, HTTP Referrer, Original URL
        record[78] = "Acme/1 CFNetwork/808.3 Darwin/16.3.0"
        record[79] = random.choice(["", fake.uri()])
        record[80] = random.choice(["", fake.uri()])

        yield record

        current_datetime = current_datetime - datetime.timedelta(minutes=47)

        # Make sure we're not creating records out of the time range.
        if current_datetime < start_datetime:
            return


def validate_app_id(app_id):
    m = re.match(r"\Aid[0-9]{10}\Z", app_id)
    if m is None:
        print("Invalid app_id")
        abort(500, "Invalid app_id")


def validate_api_token(api_token):
    m = re.match(r"\A[0-9a-f-]{36}\Z", api_token)
    if m is None:
        print("Invalid api_token")
        abort(500, "Invalid api_token")


@route("/export/<app_id>/installs_report/v5")
def get_installs_report(app_id):

    validate_app_id(app_id)
    validate_api_token(request.query.api_token)

    start_datetime, end_datetime = extract_from_and_to_datetimes(request)

    output = io.StringIO()

    for row in generate_installs_report(app_id, start_datetime, end_datetime):
        output.write(",".join(row) + "\n")

    body = output.getvalue()
    output.close()

    headers = dict()
    # "Date" and "Content-Length" get set for us
    headers["Content-Disposition"] = "attachment;filename=id1234567890_installs_2017-04-26_2017-04-27_HONHH.csv"
    # Not sure this is correct, but it's what the API returns
    headers["Content-Type"] = "application/vnd.ms-excel; charset=UTF-8"

    return HTTPResponse(body, **headers)


@route("/export/<app_id>/in_app_events_report/v5")
def get_in_app_events_report_report(app_id):

    validate_app_id(app_id)
    validate_api_token(request.query.api_token)

    start_datetime, end_datetime = extract_from_and_to_datetimes(request)

    output = io.StringIO()

    for row in generate_in_app_events_report(app_id, start_datetime, end_datetime):
        output.write(",".join(row) + "\n")

    body = output.getvalue()
    output.close()

    headers = dict()
    # "Date" and "Content-Length" get set for us
    headers["Content-Disposition"] = "attachment;filename=id1234567890_installs_2017-04-26_2017-04-27_HONHH.csv"
    # Not sure this is correct, but it's what the API returns
    headers["Content-Type"] = "application/vnd.ms-excel; charset=UTF-8"

    return HTTPResponse(body, **headers)


def extract_from_and_to_datetimes(request):

    if "from" in request.query:
        from_datetime = extract_datetime_from_param(request.query["from"])
    else:
        abort(500, "Invalid 'from' param")

    if "to" in request.query:
        to_datetime = extract_datetime_from_param(request.query["to"])
    else:
        abort(500, "Invalid 'to' param")

    return [from_datetime, to_datetime]



def extract_datetime_from_param(param):
    if param.find(":") > 0:
        d = datetime.datetime.strptime(param, "%Y-%m-%d %H:%M")
    else:
        d = datetime.datetime.strptime(param, "%Y-%m-%d")
    return d


run(host='localhost', port=8080, debug=True)
