from src.general_functions.spark_manager import get_spark
from utils.logger import get_logger
from src.general_functions.upsert_into_path import upsert


from pyspark.sql import functions as F
from pyspark.sql.window import Window

from utils.config import (SILVER_PATH,
                          SILVER_BRIDGE_ARTISTS_ALBUMS,
                          SILVER_DIM_ARTISTS,
                          SILVER_FACT_ALBUMS)
from src.general_functions.read_path_into_spark import read_data

spark = get_spark()
logger = get_logger(__name__)


def create_artist_album_growth(spark,
                               base_silver_path: str,
                               fact_albums_path: str,
                               bridge_album_artist_path: str,
                               dim_artists_path: str,
                               output_path: str,
                               upsert_primary_key_cols: list[str],
                               partition_by_cols: list[str]
                               ):
    try:
        # join dim and fact artists and albums -- base df
        fact_df = read_data(spark, f'{base_silver_path}/{fact_albums_path}')
        bridge_df = read_data(
            spark, f'{base_silver_path}/{bridge_album_artist_path}')
        dim_artist = read_data(spark, f'{base_silver_path}/{dim_artists_path}')
        logger.info('all dataframes read successfully')
    except Exception as e:
        logger.exception(f'problem in reading the tables: {e}')
        raise
        
    try:
        # count number of tracks per artist in each year
        joined_df = fact_df\
            .join(bridge_df, on='album_id')\
            .join(dim_artist, on='artist_id')\
            .groupBy(['artist_id', 'artist_name', 'release_year']).agg(F.count('album_id').alias('num_albums'))
        # window function
        window_spec = Window.partitionBy('artist_id').orderBy('release_year')

        # growth metrics columns
        growth_columns = {
            'previous_year_num_albums': F.lag('num_albums').over(window_spec),
            'growth_album_release': F.when(
                F.lag('num_albums').over(window_spec) > 0,
                (F.col('num_albums') - F.lag('num_albums').over(window_spec)) *
                100 / F.lag('num_albums').over(window_spec)
            ),
            'years_since_last_release': F.coalesce(
                F.col('release_year') - F.lag('release_year').over(window_spec),
                F.lit(0)
            )
        }

        # aggregation metric columns
        agg_columns = {
            'cumulative_albums': F.sum('num_albums').over(window_spec),
            'rolling_avg_3y': F.avg('num_albums').over(window_spec.rowsBetween(-2, 0)),
            'rolling_max_3y': F.max('num_albums').over(window_spec.rowsBetween(-2, 0))
        }

        result_df = joined_df.withColumns(growth_columns).withColumns(agg_columns)
        logger.info('aggregation logic applied successfully')
    except Exception as e:
        logger.exception(f'encountered error in the aggregation part: {e}')
        raise
        
    # write into path
    try:
        upsert(result_df,
            output_path,
            upsert_primary_key_cols,
            partition_by=partition_by_cols,
            enable_schema_evolution=False)
    except Exception as e:
        logger.exception(f'encountered an error when writing the final results: {e}')
        raise


