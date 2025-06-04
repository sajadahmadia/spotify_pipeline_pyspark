from utils.logger import get_logger
from pyspark.sql import SparkSession, DataFrame

logger = get_logger()


def read_data(
        spark: SparkSession,
        read_path: str,
        fmt: str = 'delta',
        options: dict | None = None) -> DataFrame:
    try:
        logger.info(
            f'start reading data at path : {read_path}, formt : {fmt}, options : {options}')
        reader = spark.read
        if options:
            reader = reader.options(**options)

        spark_df = reader.format(fmt).load(read_path)
        logger.info(f'successfully read data at {read_path}')
        return spark_df

    except Exception as e:
        logger.exception(
            f'unexpected error in reading the data at path: {read_path}')
        raise

    finally:
        logger.info('read function call ended')
