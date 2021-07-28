import datetime
from unittest.mock import patch

from tap_appsflyer import get_restricted_start_date

MOCKED_DATE = datetime.datetime(2021, 7, 1, 1, 1, 1, 369251)

class MockedDatetime(datetime.datetime):
    def now():
        return MOCKED_DATE

@patch('datetime.datetime', MockedDatetime)
def test_get_restricted_start_date():
    test_cases = [
        {'case': '2018-01-01T00:00:00Z', 'expected': datetime.datetime(2021, 4, 2, 1, 1, 1, 369251)},
        {'case': '2021-07-27T00:00:00Z', 'expected': datetime.datetime(2021, 7, 27, 0, 0)},
        {'case': '2021-04-02T00:00:00Z', 'expected': datetime.datetime(2021, 4, 2, 1, 1, 1, 369251)},
        {'case': '2021-05-23T12:34:56Z', 'expected': datetime.datetime(2021, 5, 23, 12, 34, 56)},
    ]

    for test_case in test_cases:
        date = get_restricted_start_date(test_case['case'])

        assert date == test_case['expected']
