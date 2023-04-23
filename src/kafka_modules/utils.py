import requests
from confluent_kafka.admin import NewTopic, AdminClient
from confluent_kafka import KafkaException
from kafka_modules.kafka_params import DEFAULT_TOPIC_PARAMS, DEFAULT_ADMIN_CLIENT_PARAMS
from utils.logger import get_logger


logger = get_logger()

URL_TEMPLATE = 'https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY_EXTENDED' \
               '&symbol={}' \
               '&interval={}' \
               '&slice={}' \
               '&apikey={}'


def get_parameters_for_api_slice(months_number: int) -> str:
    """Return year{}month{} string for slice parameter in get request"""
    if not isinstance(months_number, int) and months_number > 24:
        raise ValueError("Incorrect number of months")
    year = 1 if months_number <= 12 else 2
    month = months_number if months_number <= 12 else months_number - 12
    return 'year{}month{}'.format(year, month)


def get_stocks_per_month(company_name: str, api_key: str, time_interval: str, months_number: int):
    """Get stocks information"""
    months_slice = get_parameters_for_api_slice(months_number)
    url = URL_TEMPLATE.format(company_name, time_interval, months_slice, api_key)
    response = requests.get(url)
    if not response.ok:
        raise requests.RequestException("Can't get stocks info. Status code is {} \n{}".format(
            response.status_code, response.content)
        )
    return response.text


def get_admin_client(conf: dict = DEFAULT_ADMIN_CLIENT_PARAMS) -> AdminClient:
    """Get adminn client instance"""
    logger.info('Create admin client with params %s', conf)
    return AdminClient(conf)


def create_new_topic(topic: str, num_partitions: int, client: AdminClient) -> None:
    """Create new kafka topic"""
    params = {
        'topic': topic,
        'num_partitions': num_partitions,
        **DEFAULT_TOPIC_PARAMS
    }
    topic_obj = NewTopic(**params)
    res_dict = client.create_topics([topic_obj])
    res_dict[topic].result()
    logger.info("Create new kafka topic with params %s", params)


def check_topic_exist(client: AdminClient, topic_name: str):
    """Check if topic exists in kafka metadata"""
    topics = client.list_topics().topics
    logger.info('Topic metadata is %s', topics)
    if topic_name not in topics:
        raise KafkaException('Topic {} has not been created'.format(topic_name))
