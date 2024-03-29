import os
from pyspark.sql import SparkSession


def get_spark_session():
    """Get spark session"""
    return SparkSession.builder.getOrCreate()


def get_the_last_execution_data(company_dir: str) -> list:
    """Get the folder with last execution results"""
    max_ts_root = ''
    data = []
    if not os.path.exists(company_dir):
        raise ValueError("The dir with company raw data doesn't exist")
    for root, dirs, files in list(os.walk(company_dir))[1:]:
        if root > max_ts_root:
            max_ts_root = root
            data = files
    return [os.path.join(max_ts_root, filename) for filename in data]
