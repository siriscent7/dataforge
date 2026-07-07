"""Tracks the incremental-load watermark (last processed order_id)."""
import json
from pathlib import Path


class Watermark:
    """Persists the highest order_id processed so far, so each run only
    handles new records (incremental loading)."""

    def __init__(self, path: str = "data/output/_watermark.json"):
        self.path = Path(path)

    def read(self) -> int:
        if not self.path.exists():
            return 0  # nothing processed yet
        with open(self.path) as f:
            return json.load(f).get("last_order_id", 0)

    def write(self, last_order_id: int):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with open(self.path, "w") as f:
            json.dump({"last_order_id": last_order_id}, f)
