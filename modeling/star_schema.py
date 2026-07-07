"""Builds a star schema (fact + dimension tables) from clean orders."""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def build_dim_product(products: DataFrame) -> DataFrame:
    """Product dimension with a surrogate key."""
    w = Window.orderBy("product_id")
    return (
        products
        .withColumn("product_key", F.row_number().over(w))
        .select("product_key", "product_id", "product_name", "category", "unit_price")
    )


def build_dim_customer(orders: DataFrame) -> DataFrame:
    """Customer dimension derived from distinct customers in the orders."""
    distinct = orders.select("customer_id", "customer_city").distinct()
    w = Window.orderBy("customer_id")
    return (
        distinct
        .withColumn("customer_key", F.row_number().over(w))
        .select("customer_key", "customer_id", F.col("customer_city").alias("city"))
    )


def build_dim_date(orders: DataFrame) -> DataFrame:
    """Date dimension with year/month/day parts."""
    distinct = orders.select("order_date").distinct()
    w = Window.orderBy("order_date")
    return (
        distinct
        .withColumn("date_key", F.row_number().over(w))
        .withColumn("year", F.year("order_date"))
        .withColumn("month", F.month("order_date"))
        .withColumn("day", F.dayofmonth("order_date"))
        .select("date_key", F.col("order_date").alias("date"), "year", "month", "day")
    )


def build_fact_sales(orders: DataFrame,
                     dim_product: DataFrame,
                     dim_customer: DataFrame,
                     dim_date: DataFrame) -> DataFrame:
    """
    The fact table: one row per order, with foreign keys to each dimension
    and computed measures (quantity, revenue).
    """
    return (
        orders
        # join to product dim to get product_key + unit_price for revenue
        .join(dim_product.select("product_key", "product_id", "unit_price"),
              on="product_id", how="inner")
        # join to customer dim to get customer_key
        .join(dim_customer.select("customer_key", "customer_id"),
              on="customer_id", how="inner")
        # join to date dim to get date_key
        .join(dim_date.select("date_key", "date"),
              orders["order_date"] == F.col("date"), how="inner")
        # compute the revenue measure
        .withColumn("revenue", F.col("quantity") * F.col("unit_price"))
        .select(
            "order_id",
            "product_key",
            "customer_key",
            "date_key",
            "quantity",
            "revenue",
        )
    )
