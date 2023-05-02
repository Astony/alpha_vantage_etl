import logging
from sqlalchemy import create_engine

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType, StructField, FloatType, IntegerType, TimestampType


logger = logging.getLogger(__name__)


DEFAULT_SCHEMA = StructType([
    StructField('time', TimestampType(), False),
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


def read_execution_data_csv(session: SparkSession, filepaths: list, schema: StructType = DEFAULT_SCHEMA) -> DataFrame:
    """Read execution data from given filepaths"""
    logger.info("Start to read raw company data from paths %s", filepaths)
    sdf = None
    for filepath in filepaths:
        if sdf is None:
            sdf = session.read.csv(filepath, schema)
        else:
            sdf = sdf.union(session.read.csv(filepath, schema))
    return sdf


def save_sdf_to_local(sdf: DataFrame, path: str):
    """Save spark dataframe to local storage"""
    logger.info("Start to save insights data")
    sdf.write.parquet(path)


def save_sdf_to_postgress(sdf: DataFrame, db_params: dict, user: str, password: str):
    """Save sdf to postgress table via pandas"""
    logger.info("Start to save data to db")
    df = sdf.toPandas()
    engine = create_engine(
        'postgresql://{}:{}@{}:{}/{}'.format(
            user, password, db_params['host'],
            db_params['port'], db_params['database']
        )
    )
    df.to_sql(name=db_params['table_name'], con=engine, if_exists='append', index=False)
