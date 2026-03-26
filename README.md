# Food Delivery Operations Analytics

End-to-end data pipeline for a food delivery platform, from synthetic data generation through Spark ETL, DuckDB warehouse, and dbt analytics marts.

**[View dbt Documentation (DAG Lineage, Model Docs, Tests)](https://dev-eloper-365.github.io/Food-Delivery-Operations-Analytics/)**

## Architecture

```
Raw CSV/JSON → Spark (Bronze → Silver) → DuckDB → dbt (Staging → Marts)
```

| Layer | Tool | Output |
|-------|------|--------|
| Generate | Python | ~75K orders, 150 restaurants, 300 riders across 8 Indian cities |
| Bronze | PySpark | Raw Parquet with ingestion metadata |
| Silver | PySpark | Cleaned, deduped, DQ-flagged Parquet |
| Warehouse | DuckDB | Analytical database |
| Marts | dbt | 5 analytics tables for business insights |

## dbt Models

- **7 staging views** — thin wrappers over source tables
- **4 intermediate models** — order timelines, refund joins, rider metrics, prep times
- **4 core tables** — `dim_restaurants`, `dim_riders`, `dim_date`, `fct_orders`
- **5 analytics marts**:
  - `mart_sla_breach_analysis` — SLA breach rates by city/time slot
  - `mart_restaurant_prep_delays` — restaurant risk categorization
  - `mart_refund_drivers` — refund root cause breakdown
  - `mart_rider_performance` — rider efficiency scores and tiers
  - `mart_weekly_trends` — GMV, cancellations, refunds WoW

## Quick Start

```bash
# Install dependencies
make setup

# Run the full pipeline end-to-end
make demo

# Or run steps individually
make generate   # synthetic data
make spark      # Bronze → Silver ETL
make load       # Silver → DuckDB
make dbt        # dbt models + tests

# Explore results
duckdb data/warehouse/analytics.duckdb

# Serve dbt docs locally
make docs
```

## Tech Stack

Python, PySpark, DuckDB, dbt-core, dbt-duckdb
