"""Tests for SCD Type 2 dimension history."""
import pytest
from pyspark.sql import SparkSession
from modeling.scd import initialize_scd, apply_scd_type2


@pytest.fixture(scope="module")
def spark():
    s = (SparkSession.builder.master("local[1]").appName("test")
         .config("spark.driver.extraJavaOptions", "-Djava.security.manager=allow")
         .getOrCreate())
    s.sparkContext.setLogLevel("ERROR")
    yield s
    s.stop()


def dim(spark, rows):
    return spark.createDataFrame(rows, ["customer_key", "customer_id", "city"])


def test_initialize_marks_all_current(spark):
    scd = initialize_scd(dim(spark, [(1, "C001", "Austin")]), "2026-01-01")
    row = scd.collect()[0]
    assert row["is_current"] is True
    assert row["expiry_date"] is None


def test_unchanged_keeps_single_row(spark):
    scd = initialize_scd(dim(spark, [(1, "C001", "Austin")]), "2026-01-01")
    incoming = dim(spark, [(1, "C001", "Austin")])
    result = apply_scd_type2(scd, incoming, "customer_id", ["city"], "2026-06-01")
    assert result.count() == 1  # no change -> still one row


def test_change_creates_new_version(spark):
    scd = initialize_scd(dim(spark, [(1, "C001", "Austin")]), "2026-01-01")
    incoming = dim(spark, [(1, "C001", "Denver")])  # city changed
    result = apply_scd_type2(scd, incoming, "customer_id", ["city"], "2026-06-01")

    assert result.count() == 2  # old (expired) + new (current)
    current = result.filter(result.is_current == True).collect()  # noqa: E712
    expired = result.filter(result.is_current == False).collect()  # noqa: E712
    assert current[0]["city"] == "Denver"
    assert expired[0]["city"] == "Austin"
    assert expired[0]["expiry_date"] is not None


def test_new_customer_inserted(spark):
    scd = initialize_scd(dim(spark, [(1, "C001", "Austin")]), "2026-01-01")
    incoming = dim(spark, [(1, "C001", "Austin"), (2, "C002", "Seattle")])
    result = apply_scd_type2(scd, incoming, "customer_id", ["city"], "2026-06-01")
    ids = sorted(r["customer_id"] for r in result.filter(result.is_current == True).collect())  # noqa: E712
    assert ids == ["C001", "C002"]
