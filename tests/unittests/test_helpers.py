import datetime
import pytz

import unittest

from singer import utils
from tap_appsflyer import clean_config, get_restricted_start_date

class TestGetRestrictedStartDate(unittest.TestCase):
    def test_get_restricted_start_date(self):
        # Define the start date
        date = (datetime.datetime.now() + datetime.timedelta(days=1)).strftime("%Y-%m-%d %H-%M-%S")

        # Set the restriction_date to 90 days ago from the current UTC date
        restriction_date = datetime.datetime.now(pytz.utc) - datetime.timedelta(days=90)

        # Calculate the expected result
        start_date = utils.strptime_to_utc(date)
        expected_result = max(start_date, restriction_date)

        # Call the function under test
        result = get_restricted_start_date(date)

        # Assert that the result matches the expected result
        self.assertEqual(result, expected_result)

def test_clean_config():
    test_cases = [
        {'case': {'app_id': ' 123456789 '}, 'expected': '123456789'},
        {'case': {'app_id': '7898901  '}, 'expected': '7898901'},
        {'case': {'app_id': '  90234-0823jsjfsfsuf'}, 'expected': '90234-0823jsjfsfsuf'},
        {'case': {'app_id': 'fajslfaw084578fsdfj'}, 'expected': 'fajslfaw084578fsdfj'},
        {'case': {'app_id': ' abc def ghi  '}, 'expected': 'abc def ghi'},
    ]

    for test_case in test_cases:
        config = clean_config(test_case['case'])
        assert config['app_id'] == test_case['expected']
