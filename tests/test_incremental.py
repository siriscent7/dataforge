"""Tests for incremental loading + upsert."""
import shutil
import pytest
from pyspark.sql import SparkSession
from load.incremental import filter_new_records, upsert_fact


@pytest.fixture(scope="module")
def spark():
    s = (SparkSession.builder.master("local[1]").appName("test")
         .config("spark.driver.extraJavaOptions", "-Djava.security.manager=allow")
         .getOrCreate())
    s.sparkContext.setLogLevel("ERROR")
    yield s
    s.stop()


def orders_df(spark, ids):
    rows = [(i, f"C{i:03d}", "Austin", "P001", 1, "2026-01-01") for i in ids]
    return spark.createDataFrame(
        rows, ["order_id", "customer_id", "customer_city", "product_id", "quantity", "order_date"])


def fact_df(spark, rows):
    return spark.createDataFrame(
        rows, ["order_id", "product_key", "customer_key", "date_key", "quantity", "revenue"])


def test_filter_new_records(spark):
    orders = orders_df(spark, [1, 2, 3, 4, 5])
    new = filter_new_records(orders, last_order_id=3)
    ids = sorted(r["order_id"] for r in new.collect())
    assert ids == [4, 5]  # only orders past the watermark


def test_upsert_inserts_then_updates(spark, tmp_path):
    path = str(tmp_path / "fact")

    # first load: 2 rows
    f1 = fact_df(spark, [(1, 1, 1, 1, 2, 100.0), (2, 1, 1, 1, 1, 50.0)])
    upsert_fact(spark, f1, path)

    # second load: order 2 updated (revenue 50 -> 75), order 3 new
    f2 = fact_df(spark, [(2, 1, 1, 1, 1, 75.0), (3, 1, 1, 1, 3, 300.0)])
    result = upsert_fact(spark, f2, path)

    rows = {r["order_id"]: r["revenue"] for r in result.collect()}
    assert len(rows) == 3            # 1, 2, 3 (no duplicate of order 2)
    assert rows[2] == 75.0           # updated value wins
    assert rows[3] == 300.0          # new row inserted
    assert rows[1] == 100.0          # untouched row preserved

    shutil.rmtree(path, ignore_errors=True)
