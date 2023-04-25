from confluent_kafka import Producer
import logging
from src.kafka_modules.kafka_utils import get_stocks_per_month
from src.kafka_modules.kafka_params import DEFAULT_PRODUCER_PARAMS


logger = logging.getLogger(__name__)


class StockProducer(Producer):
    """Stock producer class"""

    def __init__(self, producer_configs: dict):
        super().__init__(producer_configs['config'])
        self._metadata = {param: value for param, value in producer_configs.items() if param != 'config'}

    @staticmethod
    def _stock_default_callback(err, msg):
        """Default callback function for producer"""
        if err:
            logger.error('Error: {}'.format(err))
        else:
            message = 'Produced message on topic {} with value of {}\n'.format(
                msg.topic(), msg.value().decode('utf-8')[:20]
            )
            logger.info(message)

    def produce_stocks(self, topic: str, api_key: str, months_number: int, time_interval: str = '60min'):
        """Produce stocks

        :param topic: Name of topic. It is equal to company name.
        :param api_key: Key for Alpha Vantage API.
        :param months_number: The number of months for which we want to retrieve data.
        :param time_interval: Time interval between stocks info within one day.
        """
        for number in range(1, months_number + 1):
            data_per_month = get_stocks_per_month(
                company_name=topic, api_key=api_key, time_interval=time_interval, months_number=number
            )
            self.produce(
                topic=topic, key=str(number), value=data_per_month, callback=self._stock_default_callback
            )
            self.poll(1)
