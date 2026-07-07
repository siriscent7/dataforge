"""Data-quality validation: splits input into clean and quarantined rows."""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def validate_orders(orders: DataFrame, valid_product_ids: list[str]):
    """
    Applies data-quality rules to the orders DataFrame.
    Returns (clean_df, rejected_df). Rejected rows carry a 'reject_reason'.

    Rules:
      - customer_id must be present (not null/empty)
      - quantity must be > 0
      - product_id must exist in the product dimension (referential integrity)
    """
    # Build a single reason column: first failing rule wins.
    reason = (
        F.when(
            F.col("customer_id").isNull() | (F.trim(F.col("customer_id")) == ""),
            F.lit("missing_customer_id"),
        )
        .when(
            F.col("quantity").isNull() | (F.col("quantity") <= 0),
            F.lit("invalid_quantity"),
        )
        .when(
            ~F.col("product_id").isin(valid_product_ids),
            F.lit("unknown_product"),
        )
        .otherwise(F.lit(None))
    )

    tagged = orders.withColumn("reject_reason", reason)

    clean = tagged.filter(F.col("reject_reason").isNull()).drop("reject_reason")
    rejected = tagged.filter(F.col("reject_reason").isNotNull())

    return clean, rejected
