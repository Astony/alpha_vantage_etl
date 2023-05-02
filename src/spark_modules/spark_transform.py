import logging

from pyspark.sql import DataFrame, functions as F, Window

from .spark_io import Columns


logger = logging.getLogger(__name__)


def cast_uppercase_to_columns(sdf: DataFrame):
    """Cast upper to columns"""
    mapped_colums = {column: column.upper() for column in sdf.columns}
    logger.info("Apply upper mapping to columns: %s", mapped_colums)
    for column in mapped_colums:
        sdf = sdf.withColumnRenamed(column, mapped_colums[column])
    return sdf


def clean_values(sdf: DataFrame) -> DataFrame:
    """Common data transformations"""
    logger.info("Start to filter incorrect values")
    sdf = sdf.dropDuplicates()
    sdf = sdf.dropna()
    return sdf


def get_month_profit(sdf: DataFrame) -> DataFrame:
    """Get statistic with profit per month"""
    logger.info('Start to get month profit')
    sdf = sdf.select([Columns.TIME, Columns.CLOSE]).cache()
    sdf = sdf.withColumn('MONTH', F.month(F.col('TIME')))\
        .withColumn('YEAR', F.year(F.col('TIME')))

    window = Window.partitionBy(['YEAR', 'MONTH'])

    max_dates = sdf.withColumn('MAX_DATE_IN_MONTH_YEAR', F.max(Columns.TIME).over(window))\
        .filter(F.col(Columns.TIME) == F.col('MAX_DATE_IN_MONTH_YEAR'))\
        .withColumnRenamed(Columns.CLOSE, 'END_PRICE')

    min_dates = sdf.withColumn('MIN_DATE_IN_MONTH_YEAR', F.min(Columns.TIME).over(window))\
        .filter(F.col(Columns.TIME) == F.col('MIN_DATE_IN_MONTH_YEAR'))\
        .withColumnRenamed(Columns.CLOSE, 'START_PRICE')

    month_profit_sdf = max_dates.join(min_dates, on=['YEAR', 'MONTH'])

    month_profit_sdf = month_profit_sdf.withColumn(
        Columns.PROFIT,
        F.when(
            F.col('START_PRICE') >= F.col('END_PRICE'),
            (F.col('START_PRICE') / F.col('END_PRICE') - F.lit(1)) * F.lit(-100)
        )
        .when(
            F.col('START_PRICE') < F.col('END_PRICE'),
            (F.col('END_PRICE') / F.col('START_PRICE') - F.lit(1)) * F.lit(100)
        )
    ).select(['YEAR', 'MONTH', Columns.PROFIT])

    return month_profit_sdf


def transform_stocks(stocks_sdf: DataFrame) -> DataFrame:
    """Apply all transformations and get processed stocks"""
    logger.info('Start to transform raw stocks')
    stocks_sdf = cast_uppercase_to_columns(stocks_sdf)
    stocks_sdf = clean_values(stocks_sdf)
    profit_sdf = get_month_profit(stocks_sdf).cache()
    logger.info('Number of rows in profit sdf: %s', profit_sdf.count())
    return profit_sdf
