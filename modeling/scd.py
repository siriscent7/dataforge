"""Slowly Changing Dimension (SCD) Type 2 logic.

Preserves dimension history: when a tracked attribute changes, the old row is
expired (is_current=false, expiry_date set) and a new current row is inserted.
"""
from datetime import date, datetime
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DateType


def initialize_scd(dim: DataFrame, load_date: str) -> DataFrame:
    """Turn a plain dimension into an SCD2 table (all rows current)."""
    return (
        dim
        .withColumn("effective_date", F.lit(load_date).cast("date"))
        .withColumn("expiry_date", F.lit(None).cast("date"))
        .withColumn("is_current", F.lit(True))
    )


def _to_date(value):
    """Coerce a value to a datetime.date (or None) for Spark schema stability."""
    if value is None:
        return None
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()
    # assume ISO string "YYYY-MM-DD"
    return datetime.strptime(str(value), "%Y-%m-%d").date()


def apply_scd_type2(
    current_scd: DataFrame,
    incoming: DataFrame,
    business_key: str,
    tracked_cols: list,
    load_date: str,
) -> DataFrame:
    """
    Merge incoming dimension rows into an existing SCD2 table.
      - unchanged  -> keep existing current row
      - changed    -> expire old current row + insert a new current row
      - new key    -> insert as current row
    """
    schema = current_scd.schema           # reuse the exact schema (fixes type inference)
    scd_cols = current_scd.columns

    history_rows = current_scd.filter(F.col("is_current") == False)   # noqa: E712
    current_rows = current_scd.filter(F.col("is_current") == True)     # noqa: E712

    current_map = {r[business_key]: r.asDict() for r in current_rows.collect()}
    incoming_map = {r[business_key]: r.asDict() for r in incoming.collect()}

    spark = current_scd.sparkSession
    out_rows = []  # list of tuples in scd_cols order

    def row_tuple(d):
        """Build a tuple in scd_cols order, coercing date columns."""
        vals = []
        for c in scd_cols:
            v = d.get(c)
            if isinstance(schema[c].dataType, DateType):
                v = _to_date(v)
            vals.append(v)
        return tuple(vals)

    for key, inc in incoming_map.items():
        cur = current_map.get(key)

        if cur is None:
            # brand-new key -> current row
            out_rows.append(row_tuple(_make_current(inc, scd_cols, load_date)))
            continue

        changed = any(cur.get(c) != inc.get(c) for c in tracked_cols)
        if not changed:
            out_rows.append(row_tuple(cur))  # keep as-is
        else:
            expired = dict(cur)
            expired["expiry_date"] = load_date
            expired["is_current"] = False
            out_rows.append(row_tuple(expired))
            out_rows.append(row_tuple(_make_current(inc, scd_cols, load_date)))

    # current keys not present in incoming stay unchanged
    for key, cur in current_map.items():
        if key not in incoming_map:
            out_rows.append(row_tuple(cur))

    if out_rows:
        rebuilt = spark.createDataFrame(out_rows, schema=schema)  # explicit schema!
        return history_rows.unionByName(rebuilt)

    return history_rows


def _make_current(inc: dict, scd_cols: list, load_date: str) -> dict:
    row = {c: inc.get(c) for c in scd_cols if c in inc}
    row["effective_date"] = load_date
    row["expiry_date"] = None
    row["is_current"] = True
    return row
