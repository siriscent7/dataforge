"""Generates sample retail CSV data for the pipeline."""
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
    print(f"Wrote {rows} rows to {out_path}")


if __name__ == "__main__":
    generate(1000, "data/raw/orders.csv")
    with open("data/raw/products.csv", "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["product_id", "product_name", "category", "unit_price"])
        for p in PRODUCTS:
            w.writerow(p)
    print("Wrote product dimension to data/raw/products.csv")
