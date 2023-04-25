import logging

from pyspark.sql import DataFrame, functions as F

from src.spark_modules.spark_io import Columns


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
    sdf = sdf.dropDuplicates()
    sdf = sdf.dropna()
    sdf = sdf.withColumn(Columns.TIME, F.to_date(Columns.TIME, 'dd-MM-yy HH:mm:ss'))
    return sdf


def get_month_profit(sdf: DataFrame) -> DataFrame:
    """Get statistic with profit per month"""
    sdf = sdf.select([Columns.TIME, Columns.CLOSE]).cache()
    sdf = sdf.withColumn('MONTH', F.month(F.col('TIME'))).withColumn('YEAR', F.year(F.col('TIME')))
    grouped_data = sdf.groupBy(['YEAR', 'MONTH'])\
        .agg(F.max(Columns.CLOSE).alias('max'), F.min(Columns.CLOSE).alias('min'))
    grouped_data = grouped_data.withColumn(Columns.PROFIT, ((F.col('max') - F.col('min')) / F.col('max')) * F.lit(100))
    grouped_data = grouped_data.withColumn('MONTH', F.date_format(F.to_date("MONTH", "MM"), "MMM"))
    return grouped_data.select(['YEAR', 'MONTH', Columns.PROFIT])


def transform_stocks(stocks_sdf: DataFrame) -> DataFrame:
    """Apply all transformations and get processed stocks"""
    stocks_sdf = cast_uppercase_to_columns(stocks_sdf)
    stocks_sdf = clean_values(stocks_sdf)
    return get_month_profit(stocks_sdf)