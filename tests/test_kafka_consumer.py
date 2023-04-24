from unittest.mock import patch, call

from src.kafka_modules.stock_consumer import StockConsumer


def test_consume_stocks(fake_message):
    with patch.object(StockConsumer, 'subscribe'):
        with patch.object(StockConsumer, 'poll', lambda *args, **kwargs: fake_message()):
            with patch.object(StockConsumer, '_send_data_to_sink') as mock_result:
                StockConsumer().consume_stocks('test_topic', 1)
                assert mock_result.call_args_list == [call('fake_value')]
