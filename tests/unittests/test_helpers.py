import datetime
from unittest.mock import patch

from tap_appsflyer import clean_config, get_restricted_start_date

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
