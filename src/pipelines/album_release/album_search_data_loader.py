from pyspark.sql.types import *
from pyspark.sql import SparkSession

album_search_schema = StructType([]
                                 StructField('album_type', StringType(), True),
                                 StructField
                                 )
