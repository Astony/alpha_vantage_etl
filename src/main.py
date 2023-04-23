from src.kafka_modules import StockConsumer, StockProducer, create_new_topic, get_admin_client, check_topic_exist

key = 'UH54HHWXXPWM0M0R'

client = get_admin_client()
create_new_topic(topic='IBM', num_partitions=2, client=client)
check_topic_exist(client, 'IBM')
producer = StockProducer()
producer.produce_stocks(topic='IBM', api_key=key, months_number=2)
consumer = StockConsumer()
consumer.consume_stocks(topic='IBM', messages_number=2)