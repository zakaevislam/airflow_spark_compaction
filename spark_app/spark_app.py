import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)

logger = logging.getLogger(__name__)


def download_gist_module():
    import requests

    gist_url = "https://gist.github.com/zakaevislam/f6c9fb6361c822f135f6cfcae968b827"
    raw_url = f"https://gist.githubusercontent.com/raw/{gist_url.split('/')[-1]}"
    response = requests.get(raw_url)

    with open('./generate_small_files.py', 'w', encoding='utf-8') as f:
        f.write(response.text)


download_gist_module()


import os
from pyspark.sql import SparkSession
from generate_small_files import generateSmallFiles, calculate_directory_size


def main(target_size_mb_per_files: int, data_path: str):
    total_size_mb = 300

    logging.info(f"Generating small files with total size {total_size_mb} mb")
    generateSmallFiles(total_size_mb=total_size_mb, save_path=data_path)
    spark = SparkSession.builder \
        .appName("compact_app") \
        .config("spark.jars.packages", "org.postgresql:postgresql:42.6.0") \
        .config("spark.driver.memory", "4g") \
        .getOrCreate()

    logging.info(f"Compacting small files to approximately size {target_size_mb_per_files} mb")
    compact(
        target_size_mb_per_files=target_size_mb_per_files,
        data_path=data_path,
        spark=spark
    )
    spark.stop()


def compact(target_size_mb_per_files: int, data_path: str, spark: SparkSession):
    current_data_path_size_mb = calculate_directory_size(data_path) / 1024 ** 2
    target_qty_files = int(current_data_path_size_mb / target_size_mb_per_files)
    spark.read.parquet(data_path).repartition(target_qty_files) \
        .write.mode("overwrite") \
        .option("compression", "uncompressed") \
        .parquet(data_path)

    actual_size_mb = calculate_directory_size(data_path) / 1024 ** 2
    actual_qty_files = len([f for f in os.listdir(data_path) if f[-8:] == '.parquet'])
    actual_avg_file_size_mb = actual_size_mb / actual_qty_files

    metadata_df = spark.createDataFrame([(
        os.path.abspath(data_path),
        actual_qty_files,
        actual_avg_file_size_mb
    )], ["data_path", "number_of_files", "average_files_size"])

    postgres_url = f"jdbc:postgresql://{os.getenv('PG_HOST', 'host.docker.internal')}:5432/airflow"
    postgres_properties = {
        "user": "airflow",
        "password": "airflow",
        "driver": "org.postgresql.Driver"
    }
    metadata_df.write \
        .mode("append") \
        .jdbc(url=postgres_url, table="file_metadata", properties=postgres_properties)


if __name__ == "__main__":
    main(
        target_size_mb_per_files=int(os.environ['TARGET_SIZE_MB_PER_FILES']),
        data_path=os.environ['DATA_PATH']
    )