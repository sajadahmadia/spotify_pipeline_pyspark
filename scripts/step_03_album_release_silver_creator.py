from src.pipelines.album_release.silver_layer.bronze_to_silver_albums import transform_albums
from src.pipelines.album_release.silver_layer.bronze_to_silver_artists import transform_artists
from src.pipelines.album_release.silver_layer.bronze_to_silver_images import transform_images
from datetime import datetime
from src.general_functions.spark_manager import get_spark
from utils.logger import get_logger
from utils.config import (BRONZE_PATH,
                          SILVER_PATH,
                          SILVER_BRIDGE_ARTISTS_ALBUMS,
                          SILVER_FACT_ALBUMS,
                          SILVER_BRIDGE_IMAGES_ALBUMS,
                          SILVER_DIM_ARTISTS,
                          SILVER_DIM_IMAGES)


logger = get_logger()
spark = get_spark()


def main():
    """Bronze to Silver transformation pipeline for Spotify album data."""
    start_time = datetime.now()

    try:
        # 1. Transform fact table first
        logger.info("="*50)
        logger.info("STEP 1: Creating Fact Albums Table")
        logger.info("="*50)
        transform_albums(BRONZE_PATH, f'{SILVER_PATH}/{SILVER_FACT_ALBUMS}')

        # 2. Transform dimensions and bridges
        logger.info("="*50)
        logger.info("STEP 2: Creating Artist Dimensions")
        logger.info("="*50)
        transform_artists(BRONZE_PATH, f'{SILVER_PATH}/{SILVER_DIM_ARTISTS}',
                          f'{SILVER_PATH}/{SILVER_BRIDGE_ARTISTS_ALBUMS}')

        logger.info("="*50)
        logger.info("STEP 3: Creating Image Dimensions")
        logger.info("="*50)
        transform_images(BRONZE_PATH, f'{SILVER_PATH}/{SILVER_DIM_IMAGES}',
                         f'{SILVER_PATH}/{SILVER_BRIDGE_IMAGES_ALBUMS}')

        # 3. Validate results
        logger.info("="*50)
        logger.info("STEP 4: Validating Results")
        logger.info("="*50)
        # Add validation logic here

        duration = datetime.now() - start_time
        logger.info(f"Pipeline completed successfully in {duration}")

    except Exception as e:
        logger.exception("Pipeline failed")


if __name__ == "__main__":
    main()
