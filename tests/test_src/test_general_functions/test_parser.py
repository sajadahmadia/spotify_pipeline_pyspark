from unittest.mock import Mock, patch
from src.general_functions.parser import make_api_request
import requests
import pytest


@patch("src.general_functions.parser.requests.get")
def test_make_api_request_happy_path(req_get):
    """Happy path test """
    mock_response = Mock()
    mock_response.json.return_value = {"some_key": 12}
    req_get.return_value = mock_response

    result = make_api_request('https://abc.com', {"a": "b"}, 10)
    assert result == {"some_key": 12}


@patch("time.sleep", return_value=None)
@patch("src.general_functions.parser.requests.get")
def test_make_api_request_timeout(req_get, mock_sleep):
    req_get.side_effect = requests.exceptions.Timeout()
    with pytest.raises(requests.exceptions.Timeout):
        result = make_api_request('https://abc.com', {"a": "b"}, 10)
    req_get.assert_called_with(
        'https://abc.com', headers={"a": "b"}, timeout=10)
