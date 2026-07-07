"""Tests for data-quality validation."""
import pytest
from pyspark.sql import SparkSession
from quality.validate import validate_orders


@pytest.fixture(scope="module")
def spark():
    s = (SparkSession.builder.master("local[1]").appName("test")
         .config("spark.driver.extraJavaOptions", "-Djava.security.manager=allow")
         .getOrCreate())
    s.sparkContext.setLogLevel("ERROR")
    yield s
    s.stop()


def make_orders(spark, rows):
    cols = ["order_id", "customer_id", "customer_city", "product_id", "quantity", "order_date"]
    return spark.createDataFrame(rows, cols)


def test_valid_row_passes(spark):
    orders = make_orders(spark, [(1, "C001", "Austin", "P001", 2, "2026-01-01")])
    clean, rejected = validate_orders(orders, ["P001"])
    assert clean.count() == 1
    assert rejected.count() == 0


def test_missing_customer_rejected(spark):
    orders = make_orders(spark, [(1, "", "Austin", "P001", 2, "2026-01-01")])
    clean, rejected = validate_orders(orders, ["P001"])
    assert clean.count() == 0
    assert rejected.count() == 1
    assert rejected.collect()[0]["reject_reason"] == "missing_customer_id"


def test_invalid_quantity_rejected(spark):
    orders = make_orders(spark, [(1, "C001", "Austin", "P001", 0, "2026-01-01")])
    _, rejected = validate_orders(orders, ["P001"])
    assert rejected.collect()[0]["reject_reason"] == "invalid_quantity"


def test_unknown_product_rejected(spark):
    orders = make_orders(spark, [(1, "C001", "Austin", "P999", 2, "2026-01-01")])
    _, rejected = validate_orders(orders, ["P001"])
    assert rejected.collect()[0]["reject_reason"] == "unknown_product"


def test_mixed_batch_splits_correctly(spark):
    orders = make_orders(spark, [
        (1, "C001", "Austin", "P001", 2, "2026-01-01"),   # ok
        (2, "", "Austin", "P001", 2, "2026-01-01"),        # bad
        (3, "C002", "Austin", "P001", 3, "2026-01-01"),   # ok
    ])
    clean, rejected = validate_orders(orders, ["P001"])
    assert clean.count() == 2
    assert rejected.count() == 1
