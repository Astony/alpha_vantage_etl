from confluent_kafka import Producer
import logging
from .utils import get_stocks_per_month
from.kafka_params import DEFAULT_PRODUCER_PARAMS


logger = logging.getLogger(__name__)


class StockProducer:
    """Kafka producer wrapper"""

    def __init__(self, conf: dict = DEFAULT_PRODUCER_PARAMS):
        self._conf = conf
        self._producer = self._get_kafka_producer(conf)

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

    @staticmethod
    def _get_kafka_producer(conf: dict) -> Producer:
        """Get kafka producer instance"""
        producer = Producer(conf)
        logger.info("Kafka producer has been created with config {}".format(conf))
        return producer

    def produce_stocks(self, topic: str, api_key: str, months_number: int, time_interval: str = '60min'):
        """Produce stocks"""
        for number in range(1, months_number + 1):
            logger.info('Send month number %s to get_stocks_per_month', number)
            data_per_month = get_stocks_per_month(
                company_name=topic, api_key=api_key, time_interval=time_interval, months_number=number
            )
            self._producer.produce(
                topic=topic, key=str(number), value=data_per_month, callback=self._stock_default_callback
            )
            self._producer.poll(1)
