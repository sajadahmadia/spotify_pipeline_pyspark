from utils.logger import get_logger
from pyspark.sql import DataFrame
from typing import Optional, Dict
from delta.tables import DeltaTable



logger = get_logger()
spark = get_spark()

def upsert(
        df_new: DataFrame,
        output_path: str,
        primary_key_cols = list[str],
        # partition_by: Optional[list] = None,
        ) -> None:
    
    if DeltaTable.isDeltaTable(spark, output_path):
        df_new.write.format('delta').mode('overwrite').save(output_path)
    
    delta_table = DeltaTable.forPath(spark, output_path)
    merge_condition = " && ".join([f"target.{col} == source.{col}" for col in primary_key_cols])
    
    delta_table.alias("target").merge(
    df_new.alias("source"),
    merge_condition)\
        .whenMatchedUpdateAll()\
        .whenNotMatchedInsertAll()\
        .execute()
    