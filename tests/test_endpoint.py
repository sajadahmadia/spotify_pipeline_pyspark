import pytest
from src.access_token_generator import generate_temp_token
from utils.config import token_url
import requests
from unittest.mock import patch, Mock


@pytest.fixture
def url():
    return token_url


def test_generate_access_token(url):
    token = generate_temp_token(url)
    assert token is not None
    assert isinstance(token, str)


def test_generate_access_token_with_http_error(url):
    # Mock the requests.post to simulate an API error
    with patch('requests.post') as mock_post:
        # Create a fake response that raises an error
        mock_response = Mock()
        mock_response.raise_for_status.side_effect = requests.HTTPError(
            "401 Unauthorized")
        mock_post.return_value = mock_response

        # Check that your function raises the error
        with pytest.raises(requests.HTTPError):
            generate_temp_token(url)
