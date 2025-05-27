import requests
import base64
from dotenv import load_dotenv
import os
from utils.logger import get_logger


def generate_temp_token(token_url):

    logger = get_logger()
    load_dotenv()

    client_id = os.getenv("client_id")
    client_secret = os.getenv("client_secret")
    credentials = f"{client_id}:{client_secret}"
    client_creds_b64 = base64.b64encode(credentials.encode('ascii')).decode()
    token_data = {"grant_type": "client_credentials"}

    token_headers = {"Authorization": f"Basic {client_creds_b64}"}
    req = requests.post(token_url, data=token_data, headers=token_headers)
    req.raise_for_status()
    try:
        token_response = req.json()
        access_token = token_response['access_token']

        return access_token
    except Exception as e:
        logger.exception(f'unexpected error in creating access token {e}')
        raise


if __name__ == '__main__':
    # Local debug/test usage only
    from utils.config import token_url
    token = generate_temp_token(token_url)
    print(token)
