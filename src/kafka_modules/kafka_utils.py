import requests
from confluent_kafka.admin import NewTopic, AdminClient
from .kafka_params import DEFAULT_TOPIC_PARAMS
import logging


logger = logging.getLogger(__name__)


URL_TEMPLATE = 'https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY_EXTENDED' \
               '&symbol={}' \
               '&interval={}' \
               '&slice={}' \
               '&apikey={}'


def get_parameters_for_api_slice(months_number: int) -> str:
    """Return year{}month{} string for slice parameter in get request

    :param months_number: The index number of the month for which we want to get the data.
    """
    if not isinstance(months_number, int) and months_number > 24:
        raise ValueError("Incorrect number of months")
    year = 1 if months_number <= 12 else 2
    month = months_number if months_number <= 12 else months_number - 12
    return 'year{}month{}'.format(year, month)


def get_stocks_per_month(company_name: str, api_key: str, time_interval: str, months_number: int):
    """Get stocks information

    :param company_name: Name of the company we want to get stocks data.
    :param api_key: Key for Alpha Vantage API.
    :param time_interval: Time interval between stocks info within one day.
    :param months_number: The index number of the month for which we want to get the data.
    """
    months_slice = get_parameters_for_api_slice(months_number)
    url = URL_TEMPLATE.format(company_name, time_interval, months_slice, api_key)
    response = requests.get(url)
    if not response.ok:
        raise requests.RequestException("Can't get stocks info. Status code is {} \n{}".format(
            response.status_code, response.content)
        )
    return response.text


def check_topic_exist(topic: str, client: AdminClient):
    """Check if topic exists in kafka metadata"""
    topics = client.list_topics().topics
    logger.info('Topic metadata is %s', topics)
    return topic in topics


def create_new_topic(topic: str, num_partitions: int, client: AdminClient) -> None:
    """Create new kafka topic

    :param topic: Name of topic. It is equal to company name.
    :param num_partitions: Number of topic's partitions. It is equal to number of months.

    """
    params = {
        'topic': topic,
        'num_partitions': num_partitions,
        **DEFAULT_TOPIC_PARAMS
    }
    topic_obj = NewTopic(**params)
    res_dict = client.create_topics([topic_obj])
    res_dict[topic].result()
    logger.info("Create new kafka topic with params %s", params)


def delete_topics(topics: list, client: AdminClient):
    """Delete kafka topic"""
    fs = client.delete_topics(topics, operation_timeout=30)
    for topic, f in fs.items():
        try:
            f.result()  # The result itself is None
            logger.info("Topic %s deleted", topic)
        except Exception as e:
            logger.info("Failed to delete topic %s: %s", topic, str(e))
