"""Incremental load + upsert into a Parquet fact table."""
import os
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def filter_new_records(orders: DataFrame, last_order_id: int) -> DataFrame:
    """Return only records newer than the watermark (incremental delta)."""
    return orders.filter(F.col("order_id") > last_order_id)


def upsert_fact(spark: SparkSession, new_fact: DataFrame, fact_path: str) -> DataFrame:
    """
    Upsert new fact rows into an existing Parquet fact table.
      - if the table doesn't exist yet, write the new rows
      - otherwise union existing + new, then dedup by order_id keeping the
        latest (new rows win) -> update-or-insert semantics, idempotent on re-run
    """
    if not os.path.exists(fact_path):
        new_fact.write.mode("overwrite").parquet(fact_path)
        return new_fact

    existing = spark.read.parquet(fact_path)

    # Tag rows so new ones win on conflict, then keep one row per order_id.
    combined = (
        existing.withColumn("_priority", F.lit(0))
        .unionByName(new_fact.withColumn("_priority", F.lit(1)))
    )

    from pyspark.sql.window import Window
    w = Window.partitionBy("order_id").orderBy(F.col("_priority").desc())
    deduped = (
        combined
        .withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .drop("_priority", "_rn")
    )

    # write to a temp path then swap (avoid reading+writing the same path)
    tmp = fact_path + "_tmp"
    deduped.write.mode("overwrite").parquet(tmp)
    result = spark.read.parquet(tmp)
    result.write.mode("overwrite").parquet(fact_path)
    import shutil
    shutil.rmtree(tmp, ignore_errors=True)
    return spark.read.parquet(fact_path)


def write_partitioned(fact_with_date, out_path: str):
    """
    Write the fact table partitioned by year/month for partition pruning.
    Expects the fact to include 'year' and 'month' columns.
    """
    (fact_with_date
        .write
        .mode("overwrite")
        .partitionBy("year", "month")
        .parquet(out_path))
