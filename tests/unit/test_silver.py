
# tests/test_silver_checks.py

from checks.silver_checks import (
    check_not_null,
    check_duplicates
)


def test_not_null(spark):
    df = spark.createDataFrame(
        [(1, 10), (2, None)],
        "order_id BIGINT, store_id INT",
    )

    result = check_not_null(
        df, ["order_id", "store_id"]
    )

    assert result == 1


def test_duplicates(spark):
    df = spark.createDataFrame(
        [(1,), (1,), (2,)],
        "order_id BIGINT",
    )

    result = check_duplicates(
        df, ["order_id"]
    )

    assert result == 1



