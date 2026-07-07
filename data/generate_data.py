"""Generates sample retail CSV data, including some intentionally bad rows."""
import csv
import random
from pathlib import Path

CITIES = ["San Jose", "Seattle", "Austin", "Denver", "Boston"]
PRODUCTS = [
    ("P001", "Laptop", "Electronics", 1200.0),
    ("P002", "Phone", "Electronics", 800.0),
    ("P003", "Desk", "Furniture", 350.0),
    ("P004", "Chair", "Furniture", 150.0),
    ("P005", "Notebook", "Stationery", 5.0),
]


def generate(rows: int, out_path: str):
    Path(out_path).parent.mkdir(parents=True, exist_ok=True)
    random.seed(42)
    with open(out_path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["order_id", "customer_id", "customer_city",
                    "product_id", "quantity", "order_date"])
        for i in range(1, rows + 1):
            prod = random.choice(PRODUCTS)
            w.writerow([
                i,
                f"C{random.randint(1, 50):03d}",
                random.choice(CITIES),
                prod[0],
                random.randint(1, 5),
                f"2026-{random.randint(1,3):02d}-{random.randint(1,28):02d}",
            ])

        # --- intentionally bad rows for data-quality testing ---
        w.writerow([rows + 1, "C010", "Austin", "P001", 0, "2026-02-01"])       # quantity = 0 (invalid)
        w.writerow([rows + 2, "", "Seattle", "P002", 2, "2026-02-02"])          # missing customer_id
        w.writerow([rows + 3, "C011", "Denver", "P999", 1, "2026-02-03"])       # unknown product
        w.writerow([rows + 4, "C012", "Boston", "P003", -5, "2026-02-04"])      # negative quantity
    print(f"Wrote {rows + 4} rows (incl. 4 bad) to {out_path}")


if __name__ == "__main__":
    generate(1000, "data/raw/orders.csv")
    with open("data/raw/products.csv", "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["product_id", "product_name", "category", "unit_price"])
        for p in PRODUCTS:
            w.writerow(p)
    print("Wrote product dimension to data/raw/products.csv")
