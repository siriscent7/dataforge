"""DataForge pipeline entry point."""
from etl.spark_session import get_spark
from ingestion.ingest import load_orders, load_products


def main():
    spark = get_spark()
    try:
        orders = load_orders(spark, "data/raw/orders.csv")
        products = load_products(spark, "data/raw/products.csv")

        print("=== Orders schema ===")
        orders.printSchema()
        print(f"Orders row count: {orders.count()}")
        print("\n=== Sample orders ===")
        orders.show(5, truncate=False)

        print("=== Products ===")
        products.show()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
