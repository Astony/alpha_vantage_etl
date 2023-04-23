from confluent_kafka import Consumer
from src.utils.logger import get_logger
from.kafka_params import DEFAULT_CONSUMER_PARAMS


logger = get_logger()


class StockConsumer:
    """Kafka consumer wrapper"""

    def __init__(self, conf: dict = DEFAULT_CONSUMER_PARAMS):
        self._conf = conf
        self._consumer = self._get_kafka_producer(conf)

    @staticmethod
    def _get_kafka_producer(conf: dict) -> Consumer:
        """Get kafka producer instance"""
        consumer = Consumer(conf)
        logger.info("Kafka consumer has been created with config {}".format(conf))
        return consumer

    def consume_stocks(self, topic: str, messages_number: int):
        """Produce stocks"""
        self._consumer.subscribe([topic])
        for number in range(1, messages_number + 1):
            message = self._consumer.poll()
            data = message.value().decode('utf-8')
            with open('{}_{}.csv'.format(topic, number), 'w', newline='') as csvfile:
                csvfile.write(data)
