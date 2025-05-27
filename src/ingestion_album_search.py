# create a file that every day, gets the album releases of the last 7 days
from datetime import datetime, timedelta
from src.access_token_generator import generate_temp_token
from utils.config import base_rul, new_releases_endpoint
import requests
import json


def ingestion(days, access_token):
    last_7_days = (datetime.now() - timedelta(days)).date().isoformat()
    results = []

    headers = {"Authorization": f"Bearer {access_token}"}

    url = f'{base_rul}/{new_releases_endpoint}'

    while url:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        data = response.json()

        for album in data.get('albums', {}).get('items', []):
            print(f"{album['name']} — {album['release_date']}")
            if album['release_date'] >= last_7_days:
                results.append(album)
        url = data.get('albums', {}).get('next')
        print(url)

    print(len(results))

    with open(f'/Users/sajad/Documents/GitHub/spotify_databricks/data/new_albums_{last_7_days}.json', 'w') as output_file:
        json.dump(results, output_file, indent=2)
