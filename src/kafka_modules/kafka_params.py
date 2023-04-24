DEFAULT_TOPIC_PARAMS = {
    'replication_factor': 1
}

DEFAULT_PRODUCER_PARAMS = {
    'bootstrap.servers': 'localhost:9092'
}

DEFAULT_ADMIN_CLIENT_PARAMS = {
    'bootstrap.servers': 'localhost:9092'
}

DEFAULT_CONSUMER_PARAMS = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'python-consumer',
    'auto.offset.reset': 'earliest'
}
