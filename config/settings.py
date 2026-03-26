from datetime import date
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent

DATA_CONFIG = {
    "date_range": {
        "start": date(2026, 1, 15),
        "end": date(2026, 3, 22),
    },
    "volumes": {
        "orders_per_day_range": (800, 1500),
        "restaurants": 150,
        "riders": 300,
        "cities": [
            "Mumbai", "Delhi", "Bangalore", "Chennai",
            "Hyderabad", "Pune", "Kolkata", "Ahmedabad"
        ],
    },
    "defect_rates": {
        "null_rate": 0.02,
        "duplicate_rate": 0.005,
        "late_event_rate": 0.03,
        "orphan_rate": 0.01,
        "sla_breach_rate": 0.12,
    },
}

PATHS = {
    "raw": PROJECT_ROOT / "data" / "raw",
    "bronze": PROJECT_ROOT / "data" / "bronze",
    "silver": PROJECT_ROOT / "data" / "silver",
    "warehouse": PROJECT_ROOT / "data" / "warehouse" / "analytics.duckdb",
}

REFUND_REASONS = {
    "Delay": 0.35,
    "Missing_Items": 0.30,
    "Cancellation": 0.20,
    "Wrong_Order": 0.10,
    "Quality_Issue": 0.05,
}
