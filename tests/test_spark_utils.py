from src.spark_modules.spark_utils import get_the_last_execution_data

import os


def test_get_the_last_execution_data(create_test_folder):
    current_dir = os.getcwd()
    assert get_the_last_execution_data(os.path.join(current_dir, 'test_dir')) == [os.path.join(current_dir, 'test_dir', '200', '2.txt')]