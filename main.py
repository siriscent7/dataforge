"""DataForge pipeline entry point (incremental)."""
from pyspark.sql.functions import sum as F_sum
from pyspark.sql import functions as F

from etl.spark_session import get_spark
from ingestion.ingest import load_orders, load_products
from quality.validate import validate_orders
from modeling.star_schema import (
    build_dim_product, build_dim_customer, build_dim_date, build_fact_sales,
)
from load.watermark import Watermark
from load.incremental import filter_new_records, upsert_fact

FACT_PATH = "data/output/fact_sales"


def main():
    spark = get_spark()
    try:
        orders = load_orders(spark, "data/raw/orders.csv")
        products = load_products(spark, "data/raw/products.csv")

        # --- incremental: only process orders newer than the watermark ---
        wm = Watermark()
        last_id = wm.read()
        print(f"Watermark: last processed order_id = {last_id}")

        new_orders = filter_new_records(orders, last_id)
        print(f"New orders this run: {new_orders.count()}")

        if new_orders.count() == 0:
            print("Nothing new to process. Pipeline is up to date.")
            return

        # --- validate ---
        valid_ids = [r["product_id"] for r in products.select("product_id").collect()]
        clean, rejected = validate_orders(new_orders, valid_ids)
        print(f"Clean: {clean.count()}, Rejected: {rejected.count()}")

        # --- build star schema for the new batch ---
        dim_product = build_dim_product(products)
        dim_customer = build_dim_customer(clean)
        dim_date = build_dim_date(clean)
        fact_new = build_fact_sales(clean, dim_product, dim_customer, dim_date)

        # --- upsert into the warehouse fact table ---
        fact = upsert_fact(spark, fact_new, FACT_PATH)
        print(f"Fact table now has {fact.count()} rows after upsert.")

        # --- advance the watermark ---
        max_id = clean.agg(F.max("order_id")).collect()[0][0]
        if max_id is not None:
            wm.write(int(max_id))
            print(f"Watermark advanced to {max_id}.")

        # --- analytical query ---
        print("\n=== Revenue by category ===")
        (fact.join(dim_product, on="product_key")
             .groupBy("category")
             .agg(F_sum("revenue").alias("total_revenue"))
             .orderBy("total_revenue", ascending=False)
             .show(truncate=False))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
