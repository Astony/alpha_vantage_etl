import logging
import os.path
import argparse

from confluent_kafka.admin import AdminClient
from pyspark.sql import SparkSession, DataFrame, functions as F

from kafka_modules import StockConsumer, StockProducer,\
    create_new_topic, check_topic_exist, DEFAULT_ADMIN_CLIENT_PARAMS, DEFAULT_PRODUCER_PARAMS,\
    DEFAULT_CONSUMER_PARAMS
from spark_modules import read_execution_data_csv, get_the_last_execution_data, transform_stocks,\
    save_sdf_to_postgress, DEFAULT_SPARK_PARAMS


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("debug.log"),
        logging.StreamHandler()
    ]
)


logger = logging.getLogger(__name__)


def extract_data(api_key: str, company_name: str, months_number: int = 2, time_interval='60min'):
    """Extract data"""
    admin_client = AdminClient(DEFAULT_ADMIN_CLIENT_PARAMS['config'])
    if not check_topic_exist(topic=company_name, client=admin_client):
        create_new_topic(topic=company_name, client=admin_client, num_partitions=months_number)

    stock_producer = StockProducer(DEFAULT_PRODUCER_PARAMS)
    stock_producer.produce_stocks(
        topic=company_name, api_key=api_key, months_number=months_number, time_interval=time_interval
    )

    stock_consumer = StockConsumer(DEFAULT_CONSUMER_PARAMS)
    stock_consumer.consume_stocks(topic=company_name, messages_number=months_number)


def transform_data(company_name: str) -> DataFrame:
    """Transform data"""
    session = SparkSession.builder.getOrCreate()
    filepaths = get_the_last_execution_data(os.path.join(DEFAULT_CONSUMER_PARAMS['save_dir_path'], company_name))
    stocks_sdf = read_execution_data_csv(session, filepaths)
    stocks_sdf = transform_stocks(stocks_sdf).withColumn('COMPANY', F.lit(company_name))
    return stocks_sdf


def load_data(result_sdf: DataFrame, db_password: str, db_username: str):
    """Load data"""
    db_configs = DEFAULT_SPARK_PARAMS['postgress_configs']
    save_sdf_to_postgress(result_sdf, db_configs, password=db_password, user=db_username)


def main():
    """Entry point"""
    parser = argparse.ArgumentParser()
    parser.add_argument('--api_key', type=str, help='Insert api key from alpha_vantage.com')
    parser.add_argument('--company_name', type=str, help='Insert company name')
    parser.add_argument('--months_number', type=int, default=2, help='Insert month number')
    parser.add_argument('--time_interval', type=str, default='60min', help='time interval between price measurements')
    parser.add_argument('--db_username', type=str)
    parser.add_argument('--db_password', type=str)
    args = parser.parse_args()

    extract_data(
        api_key=args.api_key, company_name=args.company_name,
        months_number=args.months_number, time_interval=args.time_interval
    )

    stocks = transform_data(company_name=args.company_name)

    load_data(stocks, db_password=args.db_password, db_username=args.db_username)


if __name__ == '__main__':
    main()
