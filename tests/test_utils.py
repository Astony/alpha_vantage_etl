import pytest
from unittest.mock import patch
from requests import RequestException
from src.kafka_modules.utils import get_parameters_for_api_slice, get_stocks_per_month, URL_TEMPLATE


class FakeResponse:
    """Fake response class for tests"""

    def __init__(self, ok: bool, text: str, status_code: int, content: str = None):
        self.ok = ok
        self.text = text
        self.status_code = status_code
        self.content = content


def get_ok_resp(url: str):
    """Return response with ok status"""
    return FakeResponse(ok=True, text=url, status_code=201)


def get_not_ok_resp(url: str):
    """Return response with ok status"""
    return FakeResponse(ok=False, text=url, status_code=401, content='Error')


@pytest.mark.parametrize(
    ['months_number', 'result'], [(1, 'year1month1'), (12, 'year1month12'), (13, 'year2month1'), (24, 'year2month12')])
def test_get_parameters_for_api_slice(months_number, result):
    assert get_parameters_for_api_slice(months_number) == result


def test_get_stocks_per_month_ok_status():
    params = {'api_key': 'test_key', 'company_name': 'test_name', 'months_number': 3, 'time_interval': '60min'}
    with patch('src.kafka_modules.utils.requests.get', get_ok_resp):
        result = get_stocks_per_month(**params)
        assert result == URL_TEMPLATE.format('test_name', '60min', 'year1month3', 'test_key')


def test_get_stocks_per_month_not_ok_status():
    params = {'api_key': 'test_key', 'company_name': 'test_name', 'months_number': 3, 'time_interval': '60min'}
    with patch('src.kafka_modules.utils.requests.get', get_not_ok_resp):
        with pytest.raises(
                RequestException,
                match="Can't get stocks info. Status code is {} \n{}".format(401, "Error")
        ):
            get_stocks_per_month(**params)
