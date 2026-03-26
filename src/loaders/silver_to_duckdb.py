import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

import duckdb
from config.settings import PATHS


def load_silver_to_duckdb():
    silver_path = PATHS["silver"]
    db_path = str(PATHS["warehouse"])

    Path(db_path).parent.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect(db_path)

    tables = [
        "orders", "order_items", "delivery_events",
        "restaurants", "riders", "refunds", "support_tickets",
    ]

    print("Loading silver tables into DuckDB...")

    for table in tables:
        parquet_path = silver_path / table
        if not parquet_path.exists():
            print(f"  WARNING: {parquet_path} not found, skipping")
            continue
        con.execute(f"DROP TABLE IF EXISTS {table}")
        con.execute(f"""
            CREATE TABLE {table} AS
            SELECT * FROM read_parquet('{parquet_path}/**/*.parquet')
        """)
        count = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        print(f"  Loaded {table}: {count} rows")

    print("\nDuckDB warehouse tables:")
    tables_in_db = con.execute("SHOW TABLES").fetchall()
    for t in tables_in_db:
        print(f"  - {t[0]}")

    con.close()
    print(f"\nWarehouse saved to {db_path}")


if __name__ == "__main__":
    load_silver_to_duckdb()
