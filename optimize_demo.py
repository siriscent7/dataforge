"""Demonstrates partitioning + partition pruning for query optimization."""
from pyspark.sql import functions as F

from etl.spark_session import get_spark
from ingestion.ingest import load_orders, load_products
from quality.validate import validate_orders
from modeling.star_schema import (
    build_dim_product, build_dim_customer, build_dim_date, build_fact_sales,
)

PARTITIONED_PATH = "data/output/fact_sales_partitioned"


def main():
    spark = get_spark()
    try:
        orders = load_orders(spark, "data/raw/orders.csv")
        products = load_products(spark, "data/raw/products.csv")

        valid_ids = [r["product_id"] for r in products.select("product_id").collect()]
        clean, _ = validate_orders(orders, valid_ids)

        dim_product = build_dim_product(products)
        dim_customer = build_dim_customer(clean)
        dim_date = build_dim_date(clean)
        fact = build_fact_sales(clean, dim_product, dim_customer, dim_date)

        # add year/month for partitioning (join date dim back in)
        fact_with_date = (
            fact.join(dim_date.select("date_key", "year", "month"), on="date_key")
        )

        # write partitioned by year/month
        (fact_with_date.write.mode("overwrite")
            .partitionBy("year", "month")
            .parquet(PARTITIONED_PATH))
        print(f"Wrote partitioned fact table to {PARTITIONED_PATH}")
        print("Partitions on disk (year=/month=):")
        import os
        for root, dirs, _ in os.walk(PARTITIONED_PATH):
            for d in dirs:
                if "=" in d:
                    print("  ", os.path.relpath(os.path.join(root, d), PARTITIONED_PATH))

        # --- query that benefits from pruning ---
        part = spark.read.parquet(PARTITIONED_PATH)
        print("\n=== Query with partition filter (month = 1) ===")
        jan = part.filter((F.col("year") == 2026) & (F.col("month") == 1))
        print(f"Rows in Jan 2026: {jan.count()}")

        print("\n=== Physical plan (note PartitionFilters -> pruning) ===")
        jan.explain(mode="formatted")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
