import argparse

from src.pipelines.album_release.ingestion_album_search import ingestion
from src.access_token_generator import generate_temp_token
from utils.config import token_url


def run():
    """Entry point"""
    parser = argparse.ArgumentParser(
        description="arguments used in the ingestion layer")

    parser.add_argument(
        "--days",
        required=False,
        default=7,
        type=int,
        help="number of the last days to look back and read the data"
    )

    args = parser.parse_args()

    access_token = generate_temp_token(token_url)
    ingestion(args.days, access_token)


if __name__ == '__main__':
    run()
