import datetime
import pytz

import unittest

from singer import utils
from tap_appsflyer.streams.abstracts import IncrementalStream


class TestGetRestrictedStartDate(unittest.TestCase):
    def test_get_restricted_start_date(self):
        # Define the start date
        date = (datetime.datetime.now() + datetime.timedelta(days=1)).strftime("%Y-%m-%dT%H:%M:%S")

        # Set the restriction_date to 90 days ago from the current UTC date
        restriction_date = datetime.datetime.now(pytz.utc) - datetime.timedelta(days=90)

        # Calculate the expected result
        start_date = utils.strptime_to_utc(date)
        expected_result = max(start_date, restriction_date)

        # Call the function under test
        result = IncrementalStream.get_restricted_start_date(date)

        # Assert that the result matches the expected result
        self.assertEqual(result, expected_result)
