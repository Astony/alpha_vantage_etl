import pytest
import sys
import os
import datetime

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, FloatType

from src.spark_modules.spark_io import Columns



class FakeResponse:
    """Fake response class for tests"""

    def __init__(self, ok: bool, text: str, status_code: int, content: str = None):
        self.ok = ok
        self.text = text
        self.status_code = status_code
        self.content = content


@pytest.fixture()
def fake_response():
    return FakeResponse


class FakeMessage:
    """Fake response class for tests"""

    def value(self):
        return 'fake_value'.encode('utf-8')


@pytest.fixture()
def fake_message():
    return FakeMessage


@pytest.fixture()
def get_ok_resp(fake_response):
    """Return response with ok status"""
    def inner(url: str):
        return fake_response(ok=True, text=url, status_code=201)
    return inner


@pytest.fixture()
def get_not_ok_resp(fake_response):
    """Return response with ok status"""
    def inner(url: str):
        return fake_response(ok=False, text=url, status_code=401, content='Error')
    return inner


@pytest.fixture(scope='session')
def spark_session():
    os.environ['PYSPARK_PYTHON'] = sys.executable
    os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
    return SparkSession.builder.getOrCreate()


class FakeCSV:
    def __init__(self, session:SparkSession):
        self.session = session
        self.call_count = 0

    def csv(self, path, schema):
        self.call_count += 1
        return self.session.createDataFrame(data=[(self.call_count, path),], schema=schema)


class FakeSession:
    def __init__(self, session: SparkSession):
        self.session = session
        self.fake_csv = FakeCSV(session)

    @property
    def read(self):
        return self.fake_csv


@pytest.fixture()
def fake_session(spark_session):
    return FakeSession(spark_session)


@pytest.fixture()
def test_schema_io():
    return StructType([
        StructField('id', IntegerType(), True),
        StructField('path', StringType(), True),
    ])


@pytest.fixture()
def expected_sdf_io(spark_session, test_schema_io):
    data = [(1, 'path_1'), (2, 'path_2')]
    return spark_session.createDataFrame(data, test_schema_io)


def make_not_empty_dir(subdir_name, filename):
    path = os.path.join(os.getcwd(), 'test_dir', subdir_name)
    os.makedirs(path)
    with open(os.path.join(path, filename), 'w') as file:
        file.write('Create a new text file!')
    return path, os.path.join(path, filename)


@pytest.fixture()
def create_test_folder():
    path1, file1 = make_not_empty_dir('100', '1.txt')
    path2, file2 = make_not_empty_dir('200', '2.txt')
    yield
    os.remove(file1)
    os.remove(file2)
    os.rmdir(path1)
    os.rmdir(path2)
    os.rmdir(os.path.join(os.getcwd(), 'test_dir'))


@pytest.fixture()
def test_schema_io():
    return StructType([
        StructField('id', IntegerType(), True),
        StructField('path', StringType(), True),
    ])


@pytest.fixture()
def test_schema_transform():
    return StructType([
        StructField(Columns.TIME, DateType(), True),
        StructField(Columns.CLOSE, IntegerType(), True),
    ])


@pytest.fixture()
def test_sdf_transform(spark_session, test_schema_transform):
    data = [
        (datetime.datetime(day=1, month=1, year=2007), 100),
        (datetime.datetime(day=15, month=1, year=2007), 150),
        (datetime.datetime(day=25, month=1, year=2007), 200),
        (datetime.datetime(day=1, month=2, year=2007), 200),
        (datetime.datetime(day=25, month=2, year=2007), 100),

    ]
    return spark_session.createDataFrame(data, test_schema_transform)


@pytest.fixture()
def expected_schema_transform():
    return StructType([
        StructField('YEAR', IntegerType(), True),
        StructField('MONTH', IntegerType(), True),
        StructField(Columns.PROFIT, FloatType(), True),
    ])


@pytest.fixture()
def expected_sdf_transform(spark_session, expected_schema_transform):
    data = [
        (2007, 1, 100.0),
        (2007, 2, -100.0)
    ]
    return spark_session.createDataFrame(data, expected_schema_transform)
