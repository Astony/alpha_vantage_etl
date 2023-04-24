import logging
import time


def get_logger(date_format: str = '%Y-%m-%d %H:%M:%S', file_name: str = 'log.txt'):
    ts = str(int(time.time()))
    file_name, file_format = file_name.split('.')
    file_name_with_ts = '{}_{}.{}'.format(file_name, ts, file_format)
    logging.basicConfig(format='%(asctime)s %(message)s',
                        datefmt=date_format,
                        filename=file_name_with_ts,
                        filemode='w')

    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    return logger
