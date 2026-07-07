"""DataForge pipeline entry point."""
from etl.spark_session import get_spark
from ingestion.ingest import load_orders, load_products
from quality.validate import validate_orders
from modeling.star_schema import (
    build_dim_product, build_dim_customer, build_dim_date, build_fact_sales,
)


def main():
    spark = get_spark()
    try:
        orders = load_orders(spark, "data/raw/orders.csv")
        products = load_products(spark, "data/raw/products.csv")
        print(f"Ingested {orders.count()} raw orders.\n")

        # --- Phase 2: validate ---
        valid_ids = [r["product_id"] for r in products.select("product_id").collect()]
        clean, rejected = validate_orders(orders, valid_ids)
        print(f"Clean: {clean.count()}, Rejected: {rejected.count()}\n")

        # --- Phase 3: build star schema ---
        dim_product = build_dim_product(products)
        dim_customer = build_dim_customer(clean)
        dim_date = build_dim_date(clean)
        fact_sales = build_fact_sales(clean, dim_product, dim_customer, dim_date)

        print("=== dim_product ===")
        dim_product.show(truncate=False)
        print("=== dim_customer (sample) ===")
        dim_customer.show(5, truncate=False)
        print("=== dim_date (sample) ===")
        dim_date.orderBy("date_key").show(5, truncate=False)
        print("=== fact_sales (sample) ===")
        fact_sales.show(5, truncate=False)

        # --- an analytical query on the star schema ---
        print("=== Revenue by category ===")
        (fact_sales
            .join(dim_product, on="product_key")
            .groupBy("category")
            .agg(F_sum("revenue").alias("total_revenue"),
                 F_sum("quantity").alias("total_units"))
            .orderBy("total_revenue", ascending=False)
            .show(truncate=False))

        # write the star schema out (warehouse-style, parquet)
        dim_product.write.mode("overwrite").parquet("data/output/dim_product")
        dim_customer.write.mode("overwrite").parquet("data/output/dim_customer")
        dim_date.write.mode("overwrite").parquet("data/output/dim_date")
        fact_sales.write.mode("overwrite").parquet("data/output/fact_sales")
        print("\nStar schema written to data/output/ (parquet)")
    finally:
        spark.stop()


# import here to keep the top clean
from pyspark.sql.functions import sum as F_sum

if __name__ == "__main__":
    main()
