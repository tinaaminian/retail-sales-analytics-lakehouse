""" Tests for CDC cleaning and validation with fake data. """

from decimal import Decimal

from pyspark.sql import functions as F

from transformations.customers_products_cdc import (
    clean_customer_cdc,
    clean_products_cdc,
)
from transformations.silver_quality import (
    valid_condition,
)


def test_clean_customer_cdc(spark):
    data = [
        (
            "UPDATE", "2", "2026-01-01 10:00:00",
            "101", " ALICE@GMAIL.COM ",
            " gold ", " ontario ", " Toronto "
        )
    ]

    columns = [
        "operation", "sequence_num", "event_ts",
        "customer_id", "email", "tier",
        "province", "city"
    ]

    df = spark.createDataFrame(data, columns)

    result = clean_customer_cdc(df).first()

    assert result.operation == "UPDATE"
    assert result.sequence_num == 2
    assert result.customer_id == 101
    assert result.email == "alice@gmail.com"
    assert result.tier == "GOLD"
    assert result.province == "ONTARIO"
    assert result.city == "Toronto"


def test_clean_products_cdc(spark):
    data = [
        (
            "UPDATE", "3", "2026-01-01 11:00:00",
            "201", " electronics ", "19.99"
        )
    ]

    columns = [
        "operation", "sequence_num", "event_ts",
        "product_id", "category", "price"
    ]

    df = spark.createDataFrame(data, columns)

    result = clean_products_cdc(df).first()

    assert result.operation == "UPDATE"
    assert result.sequence_num == 3
    assert result.product_id == 201
    assert result.category == "Electronics"
    assert result.price == Decimal("19.99")


def test_customer_cdc_quality_rules(spark):
    # A small, representative rule set for testing the helper.
    rules = {
        "valid_customer_id": "customer_id IS NOT NULL",
        "valid_sequence": "sequence_num IS NOT NULL",
    }

    data = [
        (101, 1),
        (None, 2),
        (103, None),
    ]

    df = spark.createDataFrame(
        data,
        "customer_id BIGINT, sequence_num BIGINT"
    )

    valid_df = df.filter(F.expr(valid_condition(rules)))

    assert valid_df.count() == 1
    assert valid_df.first().customer_id == 101