"""Tests for star-schema dimensional modeling."""
import pytest
from pyspark.sql import SparkSession
from modeling.star_schema import (
    build_dim_product, build_dim_customer, build_dim_date, build_fact_sales,
)


@pytest.fixture(scope="module")
def spark():
    s = (SparkSession.builder.master("local[1]").appName("test")
         .config("spark.driver.extraJavaOptions", "-Djava.security.manager=allow")
         .getOrCreate())
    s.sparkContext.setLogLevel("ERROR")
    yield s
    s.stop()


def sample_products(spark):
    return spark.createDataFrame(
        [("P001", "Laptop", "Electronics", 1200.0),
         ("P002", "Phone", "Electronics", 800.0)],
        ["product_id", "product_name", "category", "unit_price"])


def sample_orders(spark):
    return spark.createDataFrame(
        [(1, "C001", "Austin", "P001", 2, "2026-01-01"),
         (2, "C002", "Seattle", "P002", 1, "2026-01-02"),
         (3, "C001", "Austin", "P001", 3, "2026-01-01")],
        ["order_id", "customer_id", "customer_city", "product_id", "quantity", "order_date"])


def test_dim_product_has_surrogate_keys(spark):
    dp = build_dim_product(sample_products(spark))
    keys = [r["product_key"] for r in dp.collect()]
    assert sorted(keys) == [1, 2]  # sequential surrogate keys


def test_dim_customer_deduplicates(spark):
    dc = build_dim_customer(sample_orders(spark))
    assert dc.count() == 2  # C001 appears twice in orders -> one dim row


def test_fact_has_measures_and_keys(spark):
    orders = sample_orders(spark)
    products = sample_products(spark)
    dp = build_dim_product(products)
    dc = build_dim_customer(orders)
    dd = build_dim_date(orders)
    fact = build_fact_sales(orders, dp, dc, dd)

    assert fact.count() == 3
    cols = set(fact.columns)
    assert {"product_key", "customer_key", "date_key", "quantity", "revenue"}.issubset(cols)

    # revenue = quantity * unit_price; order 1: 2 * 1200 = 2400
    row1 = fact.filter(fact.order_id == 1).collect()[0]
    assert row1["revenue"] == 2400.0
