from pyspark.sql import functions as F
from src.general_functions.spark_manager import get_spark
from src.general_functions.read_path_into_spark import read_data
from src.general_functions.write_into_path import writer


spark = get_spark()

def transformation(
    read_path: str,
    write_path: str
) -> None:
    """_summary_

    Args:
        read_path (str): _description_
        write_path (str): _description_
    """

    df = read_data(spark, read_path, 'delta')


    df = df.select(
        [F.col('id').alias('album_id'),
        F.col('name').alias('album_name'),
        F.col('release_date').cast('date').alias('release_date'),
        F.col('release_date_precision'),
        F.col('total_tracks'),
        F.col('type').alias('object_type'),
        'album_type',
        'available_markets',
        'external_urls.spotify',
        'href',
        'uri'
        ]
    ).filter(F.col('album_id').isNotNull()).dropDuplicates(['album_id'])
    
    writer(df, write_path)

if __name__ == "__main__":
    transformation('data/bronze','data/silver/albums')
    df = spark.read.load('data/silver/albums')
    print(df.printSchema())
    print('\n\n')
    df.show()