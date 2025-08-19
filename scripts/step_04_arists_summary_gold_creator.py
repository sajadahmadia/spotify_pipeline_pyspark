from src.pipelines.album_release.gold_layer.gold_artist_album_summary import create_artist_album_summary
from src.pipelines.album_release.gold_layer.gold_artist_albums_growth import create_artist_album_growth
from utils.config import (SILVER_PATH,
                          SILVER_BRIDGE_ARTISTS_ALBUMS,
                          SILVER_FACT_ALBUMS,
                          SILVER_DIM_ARTISTS,
                          GOLD_ARTISTS_SUMMARY,
                          GOLD_ARTISTS_GROWTH,
                          GOLD_PATH)


if __name__ == "__main__":
    create_artist_album_summary(
        SILVER_PATH,
        GOLD_PATH,
        GOLD_ARTISTS_SUMMARY,
        SILVER_FACT_ALBUMS,
        SILVER_DIM_ARTISTS,
        SILVER_BRIDGE_ARTISTS_ALBUMS,
    )

    create_artist_album_growth(SILVER_PATH,
                               SILVER_FACT_ALBUMS,
                               SILVER_BRIDGE_ARTISTS_ALBUMS,
                               SILVER_DIM_ARTISTS,
                               GOLD_PATH,
                               GOLD_ARTISTS_GROWTH,
                               ['artist_id', 'release_year'],
                               ['release_year']
                               )
