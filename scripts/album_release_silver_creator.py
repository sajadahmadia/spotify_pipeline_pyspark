from src.pipelines.album_release.silver_layer.bronze_to_silver_albums import transform_albums
from utils.config import BRONZE_PATH, SILVER_PATH


if __name__ == "__main__":
    transform_albums(BRONZE_PATH, f'{SILVER_PATH}/albums')
