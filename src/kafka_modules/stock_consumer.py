from confluent_kafka import Consumer
import logging
import time
import os


logger = logging.getLogger(__name__)


class StockConsumer(Consumer):
    """Stocks consumer class"""

    def __init__(self, consumer_configs: dict):
        super().__init__(consumer_configs['config'])
        self._metadata = {param: value for param, value in consumer_configs.items() if param != 'config'}

    def save_to_local_sink(self, data: str, company_name: str, file_name):
        """Save result to local csv file"""
        save_path = self.get_save_path_with_target_name(company_name)
        if not os.path.exists(save_path):
            os.makedirs(save_path)
        save_path = os.path.join(save_path, file_name)
        logger.info('Save file %s', save_path)
        with open(save_path, 'w') as csvfile:
            csvfile.write(data)

    @staticmethod
    def console_sink(data: str):
        """Print data on console for debug purposes"""
        logger.info(data)

    def _send_data_to_sink(self, data, **kwargs) -> None:
        """Send data to sink"""
        sinks = {
            'debug': self.console_sink,
            'local': self.save_to_local_sink
        }
        return sinks[self._metadata['sink_type']](data, **kwargs)

    def get_save_path_with_target_name(self, company_name: str):
        """Update save path with timestamp and target name"""
        ts = str(int(time.time()))
        return os.path.join(self._metadata['save_dir_path'], company_name, ts)

    def consume_stocks(self, topic: str, messages_number: int) -> None:
        """Produce stocks

        :param topic: Name of topic. It is equal to company name.
        :param messages_number: Number of messages which were sent by producer. This value is equal number of months.
        """
        self.subscribe([topic])
        for number in range(1, messages_number + 1):
            message = self.poll()
            data = message.value().decode('utf-8')
            self._send_data_to_sink(data, company_name=topic, file_name='part_{}.csv'.format(number))
