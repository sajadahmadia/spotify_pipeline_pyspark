from pyspark.sql import functions as F
from src.general_functions.spark_manager import get_spark, stop_spark
from src.general_functions.read_path_into_spark import read_data
from src.general_functions.write_into_path import writer
from utils.logger import get_logger
from typing import Dict

spark = get_spark()
logger = get_logger()
DEFAULT_SILVER_WRITE_OPTIONS = {"overwriteSchema": "true"}


def transform_albums(
    read_path: str,
    write_path: str,
    write_mode: str = 'overwrite',
    write_options: Dict[str, str] = DEFAULT_SILVER_WRITE_OPTIONS
) -> None:
    """_summary_

    Args:
        read_path (str): _description_
        write_path (str): _description_
    """

    df = read_data(spark, read_path, 'delta')

    # initial_count
    count = df.count()
    logger.info(f'read {count} records at path {read_path}')

    df = df.select(
        [F.col('id').alias('album_id'),
         F.col('name').alias('album_name'),
         F.col('release_date').cast('date').alias('release_date'),
         F.col('release_date_precision'),
         F.col('total_tracks'),
         F.col('type').alias('object_type'),
         'album_type',
         'available_markets',
         F.col('external_urls.spotify').alias('spotify_url'),
         'href',
         'uri'
         ]
    ).filter(F.col('album_id').isNotNull()).dropDuplicates(['album_id'])\
        .withColumns(
            {
                'release_year': F.year('release_date'),
                'release_month': F.month('release_date'),
                'release_weekday': F.dayofweek('release_date'),
                'available_markets_count': F.size('available_markets')
            }
    ).distinct()

    df_quality = df.select(
        F.sum(F.when(F.col('album_id').isNull(), 1).otherwise(
            0)).alias('null_album_ids')
    )

    logger.info(
        f'after processing, encountred {df_quality.select("null_album_ids").collect()[0][0]} rows with null album id')
    writer(df, write_path, mode=write_mode,
           options=write_options)
