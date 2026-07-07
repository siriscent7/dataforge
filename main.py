"""DataForge pipeline entry point."""
from etl.spark_session import get_spark
from ingestion.ingest import load_orders, load_products
from quality.validate import validate_orders


def main():
    spark = get_spark()
    try:
        orders = load_orders(spark, "data/raw/orders.csv")
        products = load_products(spark, "data/raw/products.csv")

        print(f"Ingested {orders.count()} raw orders.\n")

        # collect valid product ids for referential-integrity checks
        valid_product_ids = [r["product_id"] for r in products.select("product_id").collect()]

        clean, rejected = validate_orders(orders, valid_product_ids)

        print(f"=== Clean orders: {clean.count()} ===")
        clean.show(5, truncate=False)

        print(f"=== Rejected orders: {rejected.count()} ===")
        rejected.select("order_id", "customer_id", "product_id",
                        "quantity", "reject_reason").show(truncate=False)

        # quarantine the rejected rows to disk
        rejected.write.mode("overwrite").option("header", True).csv("data/output/quarantine")
        print("Rejected rows quarantined to data/output/quarantine/")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
