DEFAULT_TOPIC_PARAMS = {
    'replication_factor': 1
}

DEFAULT_PRODUCER_PARAMS = {
    'config': {'bootstrap.servers': 'host.docker.internal:9092'}
}

DEFAULT_ADMIN_CLIENT_PARAMS = {
    'config': {'bootstrap.servers': 'host.docker.internal:9092'}
}

DEFAULT_CONSUMER_PARAMS = {
    'config': {
        'bootstrap.servers': 'host.docker.internal:9092',
        'group.id': 'python-consumer',
        'auto.offset.reset': 'earliest'
    },
    'sink_type': 'local',
    'save_dir_path': 'raw_data'
}
