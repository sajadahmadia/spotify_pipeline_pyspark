import requests
import base64
from dotenv import load_dotenv
import os


def generate_temp_token(token_url):

    load_dotenv()
    client_id = os.getenv("client_id")
    client_secret = os.getenv("client_secret")
    credentials = f"{client_id}:{client_secret}"
    client_creds_b64 = base64.b64encode(credentials.encode('ascii')).decode()
    # print(client_creds_b64)
    token_data = {"grant_type": "client_credentials"}

    token_headers = {"Authorization": f"Basic {client_creds_b64}"}

    print(token_headers)

    req = requests.post(token_url, data=token_data, headers=token_headers)
    token_response = req.json()
    access_token = token_response['access_token']

    return access_token
