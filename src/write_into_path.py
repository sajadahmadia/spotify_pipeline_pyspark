from utils.logger import get_logger
from pyspark.sql import DataFrame
from typing import Optional, Dict

logger = get_logger()


def writer(
        df: DataFrame,
        save_path: str,
        mode: str = 'overwrite',
        partition_by: Optional[list] = None,
        fmt: str = 'delta',
        options: Optional[Dict[str, str]] = None) -> None:
    try:
        logger.info(
            f'start writing data to path : {save_path}, format : {fmt}, options : {options}')
        writer = df.write.mode(mode)
        if options:
            writer = writer.options(**options)

        if partition_by:
            writer = writer.partitionBy(*partition_by)

        writer.format(fmt).save(save_path)
        logger.info(f'successfully wrote data to {save_path}')

    except Exception as e:
        logger.exception(
            f'unexpected error in saving the data to path: {save_path} as {e}')
        raise

    finally:
        logger.info('write function call ended')
