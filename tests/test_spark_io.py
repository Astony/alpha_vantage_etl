from src.spark_modules.spark_io import read_execution_data_csv


def test_read_execution_data_csv(spark_session, fake_session, expected_sdf_io, test_schema_io):
    filepaths = ['path_1', 'path_2']
    result_sdf = read_execution_data_csv(fake_session, filepaths, test_schema_io)
    assert sorted(result_sdf.collect()) == sorted(expected_sdf_io.collect())

