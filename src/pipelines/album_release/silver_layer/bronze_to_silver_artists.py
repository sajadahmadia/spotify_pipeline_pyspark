from pyspark.sql import functions as F
from src.general_functions.spark_manager import get_spark
from src.general_functions.read_path_into_spark import read_data
from src.general_functions.write_into_path import writer
from utils.logger import get_logger
from typing import Dict

spark = get_spark()
logger = get_logger()
DEFAULT_SILVER_WRITE_OPTIONS = {"overwriteSchema": "true"}



def transform_artists(
    read_path: str,
    write_path: str,
    write_mode: str = 'overwrite',
    write_options: Dict[str, str] = DEFAULT_SILVER_WRITE_OPTIONS
) -> None:
    try:
        df = read_data(spark, read_path)
    except Exception as e:
        logger.exception(f"undexpected error happended: {e}")
    
    
    df_artists = df.select([F.explode('artists').alias('artists'), 'id'])\
                        .select([F.col('artists.external_urls.spotify'),
                                                        'artists.href',
                                                        F.col('artists.id').alias('artists_id'),
                                                        'artists.name',
                                                        'artists.type',
                                                        'artists.uri',
                                                        F.col('id').alias('album_id')
                                                        ])# type: ignore
    