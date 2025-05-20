import requests as re
import base64

token_url = "https://accounts.spotify.com/api/token"
endpoint = "albums"
id = "4aawyAB9vmqN3uQ7FjRGTy"
client_id = "3ce5963b24db4cb4acae60120181ef74"
client_secret = "e41717857a21483c8a2cddcce6617a6a"

credentials = f"{client_id}:{client_secret}"
client_creds_b64 = base64.b64encode(credentials.encode('ascii')).decode()
# print(client_creds_b64)

token_data = {
    "grant_type": "client_credentials"
}

token_headers = {
    "Authorization": f"Basic {client_creds_b64}"
}

print(token_headers)

req = re.post(token_url, data=token_data, headers=token_headers)
token_response = req.json()

print(req.status_code)

access_token = token_response['access_token']
access_token_expiray = token_response['expires_in']

# print(token_response)
# curl --request GET \
#     'https://api.spotify.com/v1/tracks/2TpxZ7JUBn3uw46aR7qd6V' \
#      --header "Authorization: Bearer BQBT31EGs6q-YpAjzpjcNNHfi2Dmvc_Azrb4b6wmXivJlmXmSp-7wiUL8-n6tnyMKl2oQUcHkTAJoyIfi1xTdS_RGTgJKEL_1m1RNtXIkfrt_vm04o-dE0VRQp1lHqTeVBgGixQAfQY"

base_url = "https://api.spotify.com/v1"
endpoint = "albums"
id = "4aawyAB9vmqN3uQ7FjRGTy"

headers = {
    "Authorization": f"Bearer {access_token}"
}


try:
    sample_album = re.get(url=f"{base_url}/{endpoint}/{id}", headers=headers)
    print(sample_album.json())
except:
    print(f"{base_url}/{endpoint}/{id}")


artist_followers = re.get('https://api.spotify.com/v1/artists/0TnOYISbd1XYRBk9myaseg',
                          headers=headers).json()

print(artist_followers.get('followers', {}).get('total', {}))
print(artist_followers.get('popularity', {}))
