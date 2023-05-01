from src.spark_modules.spark_transform import cast_uppercase_to_columns, get_month_profit


def test_cast_uppercase_to_columns(expected_sdf_io):
    result = cast_uppercase_to_columns(expected_sdf_io).columns
    assert sorted(result) == sorted(['ID', 'PATH'])


def test_get_month_profit(test_sdf_transform, expected_sdf_transform):
    result = get_month_profit(test_sdf_transform)
    assert sorted(result.collect()) == sorted(expected_sdf_transform.collect())