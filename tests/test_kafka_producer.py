from unittest.mock import patch

from src.kafka_modules.stock_producer import StockProducer
from src.kafka_modules.kafka_params import DEFAULT_PRODUCER_PARAMS


def test_produce_stocks():
    params = {
        'topic': 'test_topic',
        'api_key': 'test_key',
        'time_interval': '60min',
        'months_number': 12
    }
    with patch('src.kafka_modules.stock_producer.get_stocks_per_month', lambda *args, **kwargs: 'test_data'):
        with patch.object(StockProducer, 'produce') as mock_produce:
            with patch.object(StockProducer, 'poll'):
                StockProducer(DEFAULT_PRODUCER_PARAMS).produce_stocks(**params)
                assert mock_produce.call_count == 12
