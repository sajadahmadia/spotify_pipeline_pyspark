from pyspark.sql.types import *
from pyspark.sql import SparkSession
from utils.logger import get_logger

logger = get_logger()


def read_data(read_path, fmt, spark):
    try:
        spark_df = spark.read.options('format', fmt).path(read_path)
        return spark_df
    except Exception as e:
        logger.exception(
            f'unexpected error in reading the data at path: {read_data}')
    finally:
        logger.info('read data function call ended')
