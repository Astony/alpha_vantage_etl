from confluent_kafka import Consumer
import logging
from src.kafka_modules.kafka_params import DEFAULT_CONSUMER_PARAMS


logger = logging.getLogger(__name__)


class StockConsumer(Consumer):
    """Stocks consumer class"""

    def __init__(self, config: dict = DEFAULT_CONSUMER_PARAMS):
        super().__init__(config)

    @staticmethod
    def _send_data_to_sink(data: str) -> None:
        """Send data to sink"""
        logger.info(data)

    def consume_stocks(self, topic: str, messages_number: int) -> None:
        """Produce stocks

        :param topic: Name of topic. It is equal to company name.
        :param messages_number: Number of messages which were sent by producer. This value is equal number of months.
        """
        self.subscribe([topic])
        for number in range(1, messages_number + 1):
            message = self.poll()
            data = message.value().decode('utf-8')
            self._send_data_to_sink(data)
