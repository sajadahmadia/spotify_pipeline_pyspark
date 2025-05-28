import pytest
from src.access_token_generator import generate_temp_token
from utils.config import token_url


def test_generate_temp_token_success(mocker):
    # Mock the requests.post method
    mock_post = mocker.patch('requests.post')
    mock_post.return_value.json.return_value = {
        'access_token': 'mocked_access_token'}
    mock_post.return_value.raise_for_status.return_value = None

    # Call the function and assert the result
    access_token = generate_temp_token(token_url)
    assert access_token == 'mocked_access_token'
    mock_post.assert_called_once_with(
        token_url, data={'grant_type': 'client_credentials'}, headers=mocker.ANY)


def test_generate_temp_token_failure(mocker):
    # Mock the requests.post method to raise an exception
    mock_post = mocker.patch('requests.post')
    mock_post.side_effect = Exception('Mocked exception')

    # Call the function and assert that it raises an exception
    with pytest.raises(Exception):
        generate_temp_token(token_url)
    mock_post.assert_called_once_with(
        token_url, data={'grant_type': 'client_credentials'}, headers=mocker.ANY)
