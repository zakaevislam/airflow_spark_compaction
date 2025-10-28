"""
Spark Parquet Data Generator
This module provides utilities for generating Parquet files of specified sizes
using Apache Spark. It includes functions for creating test data, measuring
file sizes, and generating large datasets for testing and development purposes.
"""
import os
import logging
import uuid

from pyspark.sql import SparkSession, DataFrame

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)

logger = logging.getLogger(__name__)


def create_spark_session() -> SparkSession:
    """Create and configure a Spark session.
    Returns:
        SparkSession: Configured Spark session with PostgreSQL driver
        and 4GB driver memory allocation.
    """
    spark = SparkSession.builder \
        .appName("data_generation_app") \
        .config("spark.jars.packages", "org.postgresql:postgresql:42.6.0") \
        .config("spark.driver.memory", "4g") \
        .getOrCreate()
    return spark


def create_sample_dataframe(spark: SparkSession, rows: int = 1) -> DataFrame:
    """Create a test DataFrame with repeated string data.
    Creates a DataFrame with 10 rows, each containing a string of
    1000 '1' characters in a column named 'id'.
    Returns:
        DataFrame: Spark DataFrame with sample test data.
    """
    return spark.createDataFrame([(str(uuid.uuid4()), ) for i in range(rows)], ["uuid"])


def calculate_directory_size(directory_path: str) -> int:
    """Calculate total size of all files in a directory recursively.
    Args:
        directory_path: Path to the directory to calculate size for.
    Returns:
        int: Total size of all files in the directory in bytes.
    """
    total_size = 0
    for dirpath, dirnames, filenames in os.walk(directory_path):
        for filename in filenames:
            filepath = os.path.join(dirpath, filename)
            total_size += os.path.getsize(filepath)
    return total_size


def get_df_size_mb_in_parquet(
        qty_files: int,
        df: DataFrame,
        save_path: str = './test_single_record'
) -> float:
    """Get DataFrame size in megabytes when written as Parquet files.
    Writes DataFrame to Parquet format with specified number of files
    and measures the resulting directory size.
    Args:
        qty_files: Number of partition files to create.
        df: Spark DataFrame to measure.
        save_path: Temporary directory path for measurement.
    Returns:
        float: Size of the written Parquet files in megabytes.
    """
    df.repartition(qty_files).write.mode("overwrite") \
        .option("compression", "uncompressed") \
        .parquet(save_path)
    size_mb = calculate_directory_size(save_path) / 1024 ** 2
    return size_mb


def generateSmallFiles(total_size_mb: int = 300, save_path: str = './parquet_files'):
    """
    Generate multiple small Parquet files to achieve the specified total size.
    This function creates Parquet files containing sample data until the target
    total size is reached. It uses a two-step approach: first estimating the
    required number of files based on a sample, then generating the actual files.
    Args:
        total_size_mb: Target total size for all generated Parquet files in megabytes.
                      Defaults to 300 MB.
        save_path: Output directory path where Parquet files will be saved.
                  Defaults to './parquet_files'.
    Steps:
        1. Creates a sample DataFrame to estimate size per file
        2. Calculates required number of files to reach target size
        3. Generates the final DataFrame with appropriate number of rows
        4. Writes data to Parquet format with calculated number of partitions
        5. Logs actual generated size and file count
    Note:
        - Uses uncompressed Parquet format for accurate size control
        - Overwrites existing files in the output directory
        - Actual size may vary slightly due to Parquet formatting overhead
    Example:
        generate_small_files(total_size_mb=500, save_path='./output')
        # Generates approximately 500 MB of Parquet files in ./output directory
    """
    spark = None
    try:
        spark = create_spark_session()
        len_df_for_estimate_size_in_parquet = 1000
        estimate_df = create_sample_dataframe(spark=spark, rows=len_df_for_estimate_size_in_parquet)

        qty_files = int(total_size_mb / get_df_size_mb_in_parquet(1, estimate_df))
        target_df_rows = qty_files*len_df_for_estimate_size_in_parquet

        df = create_sample_dataframe(spark=spark, rows=target_df_rows)
        df.repartition(qty_files).write.mode("overwrite") \
            .option("compression", "uncompressed") \
            .parquet(save_path)
        actual_size_files_mb = int(calculate_directory_size(save_path) / 1024 ** 2)
        logging.info(f"generated small files total size in mb: {actual_size_files_mb}")
        logging.info(f"total qty files : {qty_files}")
    finally:
        if spark:
            spark.stop()