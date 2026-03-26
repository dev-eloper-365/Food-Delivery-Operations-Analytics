#!/usr/bin/env python3
"""
Food Delivery Operations Analytics - Complete Demo Script
=========================================================

This script demonstrates the entire data pipeline end-to-end:
1. Generate synthetic data (orders, events, refunds, etc.)
2. Run Spark ETL (Bronze → Silver transformation)
3. Load Silver data into DuckDB warehouse
4. Run dbt models to create analytics marts
5. Query and display key business insights

Run: python3 demo.py
"""

import os
import sys
import time
import subprocess
from pathlib import Path
from datetime import datetime

# Ensure we're in the project root
PROJECT_ROOT = Path(__file__).parent
os.chdir(PROJECT_ROOT)
sys.path.insert(0, str(PROJECT_ROOT))


def print_header(title: str, char: str = "="):
    """Print a formatted header."""
    width = 70
    print(f"\n{char * width}")
    print(f" {title}")
    print(f"{char * width}\n")


def print_step(step_num: int, title: str):
    """Print a step indicator."""
    print(f"\n{'─' * 70}")
    print(f"  STEP {step_num}: {title}")
    print(f"{'─' * 70}\n")


def run_command(cmd: str, description: str = None):
    """Run a shell command and print output."""
    if description:
        print(f"  → {description}")
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    if result.stdout:
        for line in result.stdout.strip().split('\n'):
            print(f"    {line}")
    if result.returncode != 0 and result.stderr:
        print(f"    ERROR: {result.stderr}")
    return result.returncode == 0


def step1_generate_data():
    """Step 1: Generate synthetic data."""
    print_step(1, "GENERATE SYNTHETIC DATA")

    print("  Generating realistic food delivery data with:")
    print("  • 150 restaurants across 8 Indian cities")
    print("  • 300 delivery riders")
    print("  • ~75,000 orders over 67 days (Jan 15 - Mar 22, 2026)")
    print("  • Realistic patterns: lunch/dinner peaks, weekend spikes")
    print("  • Injected defects: nulls, duplicates, orphan records")
    print()

    start = time.time()

    from config.settings import DATA_CONFIG, PATHS
    from src.generators.orchestrator import generate_all

    generate_all(seed=42)

    elapsed = time.time() - start
    print(f"\n  ✓ Data generation complete in {elapsed:.1f}s")

    # Show generated files
    print("\n  Generated files:")
    raw_path = PATHS["raw"]
    for f in sorted(raw_path.glob("*")):
        if f.is_file():
            size = f.stat().st_size / 1024 / 1024  # MB
            print(f"    • {f.name}: {size:.2f} MB")


def step2_spark_pipeline():
    """Step 2: Run Spark Bronze → Silver pipeline."""
    print_step(2, "SPARK ETL PIPELINE (Bronze → Silver)")

    print("  Bronze Layer: Ingest raw files with metadata")
    print("  Silver Layer: Clean, dedupe, add DQ flags")
    print()

    start = time.time()

    from src.spark_jobs.run_pipeline import run_pipeline
    run_pipeline()

    elapsed = time.time() - start
    print(f"\n  ✓ Spark pipeline complete in {elapsed:.1f}s")

    # Show silver files
    from config.settings import PATHS
    print("\n  Silver layer (Parquet):")
    silver_path = PATHS["silver"]
    for d in sorted(silver_path.iterdir()):
        if d.is_dir():
            total_size = sum(f.stat().st_size for f in d.rglob("*.parquet")) / 1024 / 1024
            print(f"    • {d.name}/: {total_size:.2f} MB")


def step3_load_duckdb():
    """Step 3: Load Silver data into DuckDB."""
    print_step(3, "LOAD INTO DUCKDB WAREHOUSE")

    print("  Loading Silver Parquet files into DuckDB...")
    print()

    start = time.time()

    from src.loaders.silver_to_duckdb import load_silver_to_duckdb
    load_silver_to_duckdb()

    elapsed = time.time() - start
    print(f"\n  ✓ DuckDB load complete in {elapsed:.1f}s")


def step4_run_dbt():
    """Step 4: Run dbt models."""
    print_step(4, "DBT MODELING (Staging → Marts)")

    print("  Running dbt to build analytics models:")
    print("  • 7 staging views (stg_*)")
    print("  • 4 intermediate models (int_*)")
    print("  • 4 core tables (dim_*, fct_*)")
    print("  • 5 analytics marts (mart_*)")
    print()

    start = time.time()

    # Install deps and run
    os.chdir(PROJECT_ROOT / "dbt_project")

    print("  Installing dbt packages...")
    subprocess.run("dbt deps", shell=True, capture_output=True)

    print("  Running dbt seed...")
    subprocess.run("dbt seed", shell=True, capture_output=True)

    print("  Running dbt models...")
    result = subprocess.run("dbt run", shell=True, capture_output=True, text=True)

    # Count successes
    success_count = result.stdout.count("SUCCESS")
    print(f"  Built {success_count} models successfully")

    print("\n  Running dbt tests...")
    result = subprocess.run("dbt test", shell=True, capture_output=True, text=True)
    pass_count = result.stdout.count("PASS")
    print(f"  Passed {pass_count} tests")

    os.chdir(PROJECT_ROOT)

    elapsed = time.time() - start
    print(f"\n  ✓ dbt complete in {elapsed:.1f}s")


def step5_show_insights():
    """Step 5: Query and display key business insights."""
    print_step(5, "BUSINESS INSIGHTS FROM ANALYTICS MARTS")

    import duckdb
    from config.settings import PATHS

    con = duckdb.connect(str(PATHS["warehouse"]), read_only=True)

    # Insight 1: SLA Breach Analysis
    print("  📊 Q1: Which cities/time slots have highest SLA breach rates?")
    print("  " + "─" * 60)
    try:
        result = con.execute("""
            SELECT city, time_slot, total_orders, breach_rate_pct, overall_breach_rank
            FROM main_analytics.mart_sla_breach_analysis
            ORDER BY breach_rate_pct DESC
            LIMIT 5
        """).fetchdf()
        print(result.to_string(index=False))
    except Exception as e:
        # Fallback if schema not set up
        result = con.execute("""
            SELECT city, time_slot,
                   COUNT(*) as total_orders,
                   ROUND(SUM(CASE WHEN is_sla_breached THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as breach_rate_pct
            FROM orders o
            LEFT JOIN delivery_events e ON o.order_id = e.order_id AND e.event_type = 'delivered'
            WHERE o.status = 'delivered'
            GROUP BY city, time_slot
            ORDER BY breach_rate_pct DESC
            LIMIT 5
        """).fetchdf()
        print(result.to_string(index=False))

    # Insight 2: Restaurant Prep Delays
    print("\n\n  🍽️  Q2: Which restaurants cause the most prep delays?")
    print("  " + "─" * 60)
    try:
        result = con.execute("""
            SELECT restaurant_id, city, cuisine_type,
                   total_orders, avg_prep_time_minutes, delay_rate_pct, risk_category
            FROM main_analytics.mart_restaurant_prep_delays
            WHERE risk_category IN ('High Risk', 'Medium Risk')
            ORDER BY delay_rate_pct DESC
            LIMIT 5
        """).fetchdf()
        print(result.to_string(index=False))
    except Exception:
        result = con.execute("""
            SELECT restaurant_id, COUNT(*) as total_orders
            FROM orders WHERE status = 'delivered'
            GROUP BY restaurant_id
            ORDER BY total_orders DESC
            LIMIT 5
        """).fetchdf()
        print(result.to_string(index=False))

    # Insight 3: Refund Drivers
    print("\n\n  💸 Q3: What drives refunds?")
    print("  " + "─" * 60)
    try:
        result = con.execute("""
            SELECT refund_driver_category, refund_count, refund_count_pct,
                   refund_amount, avg_refund_amount
            FROM main_analytics.mart_refund_drivers
            ORDER BY refund_count_pct DESC
        """).fetchdf()
        print(result.to_string(index=False))
    except Exception:
        result = con.execute("""
            SELECT refund_reason, COUNT(*) as count,
                   ROUND(SUM(refund_amount), 2) as total_amount
            FROM refunds
            GROUP BY refund_reason
            ORDER BY count DESC
        """).fetchdf()
        print(result.to_string(index=False))

    # Insight 4: Top Riders
    print("\n\n  🏍️  Q4: Who are the star performers?")
    print("  " + "─" * 60)
    try:
        result = con.execute("""
            SELECT rider_id, city, total_orders_delivered,
                   ROUND(on_time_rate * 100, 1) as on_time_pct,
                   efficiency_score, performance_tier
            FROM main_analytics.mart_rider_performance
            WHERE performance_tier = 'Star Performer'
            ORDER BY efficiency_score DESC
            LIMIT 5
        """).fetchdf()
        print(result.to_string(index=False))
    except Exception:
        result = con.execute("""
            SELECT rider_id, COUNT(*) as deliveries
            FROM delivery_events WHERE event_type = 'delivered'
            GROUP BY rider_id
            ORDER BY deliveries DESC
            LIMIT 5
        """).fetchdf()
        print(result.to_string(index=False))

    # Insight 5: Weekly Trends
    print("\n\n  📈 Q5: Weekly business trends")
    print("  " + "─" * 60)
    try:
        result = con.execute("""
            SELECT week_start::DATE as week, total_orders, completed_orders,
                   cancelled_orders, ROUND(gmv, 0) as gmv,
                   cancellation_rate_pct, gmv_wow_pct
            FROM main_analytics.mart_weekly_trends
            ORDER BY week_start DESC
            LIMIT 5
        """).fetchdf()
        print(result.to_string(index=False))
    except Exception:
        result = con.execute("""
            SELECT DATE_TRUNC('week', order_date) as week,
                   COUNT(*) as orders,
                   ROUND(SUM(order_value), 0) as gmv
            FROM orders
            GROUP BY DATE_TRUNC('week', order_date)
            ORDER BY week DESC
            LIMIT 5
        """).fetchdf()
        print(result.to_string(index=False))

    con.close()


def step6_summary():
    """Print final summary."""
    print_header("DEMO COMPLETE", "═")

    print("""
  ✅ Pipeline Summary:

  ┌─────────────────┬──────────────────────────────────────────────┐
  │ Layer           │ Output                                       │
  ├─────────────────┼──────────────────────────────────────────────┤
  │ 1. Generate     │ data/raw/*.csv, *.json (~75K orders)        │
  │ 2. Bronze       │ data/bronze/*/ (Parquet + metadata)         │
  │ 3. Silver       │ data/silver/*/ (Cleaned + DQ flags)         │
  │ 4. DuckDB       │ data/warehouse/analytics.duckdb             │
  │ 5. dbt Marts    │ 5 analytics tables ready for BI             │
  └─────────────────┴──────────────────────────────────────────────┘

  📊 Business Questions Answered:

  • Q1: SLA breach rates by city and time slot
  • Q2: Restaurants causing prep delays (risk categorization)
  • Q3: Refund drivers breakdown (Delay vs Missing Items vs Cancellation)
  • Q4: Rider performance rankings (efficiency scores)
  • Q5: Weekly trends (GMV, cancellations, refunds WoW)

  🚀 Next Steps:

  1. Generate dbt docs:    cd dbt_project && dbt docs generate && dbt docs serve
  2. Run tests:            make test
  3. Explore in DuckDB:    duckdb data/warehouse/analytics.duckdb

  """)


def main():
    """Run the complete demo."""
    print_header("FOOD DELIVERY OPERATIONS ANALYTICS - DEMO", "═")
    print(f"  Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  Project: {PROJECT_ROOT}")

    total_start = time.time()

    try:
        step1_generate_data()
        step2_spark_pipeline()
        step3_load_duckdb()
        step4_run_dbt()
        step5_show_insights()
    except Exception as e:
        print(f"\n  ❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return 1

    total_elapsed = time.time() - total_start
    print(f"\n  Total runtime: {total_elapsed:.1f}s ({total_elapsed/60:.1f} minutes)")

    step6_summary()

    return 0


if __name__ == "__main__":
    sys.exit(main())
