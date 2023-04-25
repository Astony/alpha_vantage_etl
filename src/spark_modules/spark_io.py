from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, FloatType, IntegerType, DateType

from src.spark_modules.spark_utils import get_spark_session

DEFAULT_SCHEMA = StructType([
    StructField('time', DateType(), False),
    StructField('open', FloatType(), False),
    StructField('high', FloatType(), False),
    StructField('low', FloatType(), False),
    StructField('close', FloatType(), False),
    StructField('volume', IntegerType(), False),
])


class Columns:
    """Columns holder class"""
    TIME = 'TIME'
    OPEN = 'OPEN'
    HIGH = 'HIGH'
    LOW = 'LOW'
    CLOSE = 'CLOSE'
    VOLUME = 'VOLUME'
    DATE_FORMAT = 'yy-MMM-dd HH:mm:ss'
    PROFIT = 'PROFIT'


def read_execution_data_csv(filepaths: list, schema: StructType = DEFAULT_SCHEMA) -> DataFrame:
    """Read execution data from given filepaths"""
    session = get_spark_session()
    sdf = None
    for filepath in filepaths:
        if sdf is None:
            sdf = session.read.csv(filepath, schema)
        else:
            sdf = sdf.union(session.read.csv(filepath, schema))
    return sdf

