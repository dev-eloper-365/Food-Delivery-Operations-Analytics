# Food Delivery Operations Analytics - Technical Architecture

## 1. System Overview

### Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           TARJ ANALYTICS PIPELINE                           │
└─────────────────────────────────────────────────────────────────────────────┘

┌──────────────┐     ┌──────────────┐     ┌──────────────┐     ┌──────────────┐
│   GENERATE   │     │  RAW FILES   │     │   PYSPARK    │     │   DUCKDB     │
│   (Python)   │────►│  (CSV/JSON)  │────►│  (Bronze →   │────►│  (Warehouse) │
│              │     │              │     │   Silver)    │     │              │
└──────────────┘     └──────────────┘     └──────────────┘     └──────────────┘
       │                    │                    │                    │
       │                    │                    │                    │
       ▼                    ▼                    ▼                    ▼
  Faker + NumPy      data/raw/*.csv      data/bronze/        data/warehouse/
                     data/raw/*.json     data/silver/        analytics.duckdb
                                         (Parquet)
                                                                     │
                                                                     ▼
                                                            ┌──────────────┐
                                                            │     dbt      │
                                                            │  (Staging →  │
                                                            │   Marts)     │
                                                            └──────────────┘
                                                                     │
                                                                     ▼
                                                            ┌──────────────┐
                                                            │    MARTS     │
                                                            │  (Analytics) │
                                                            └──────────────┘
```

### Data Flow

```
[1. Generate]     [2. Ingest]        [3. Clean]         [4. Model]        [5. Serve]
    │                 │                  │                  │                 │
    ▼                 ▼                  ▼                  ▼                 ▼
┌────────┐      ┌──────────┐       ┌──────────┐       ┌──────────┐      ┌─────────┐
│ Python │      │  Bronze  │       │  Silver  │       │   dbt    │      │  Marts  │
│Generators│───►│ Parquet  │──────►│ Parquet  │──────►│  Models  │─────►│  Tables │
│        │      │ + Meta   │       │ + DQ     │       │          │      │         │
└────────┘      └──────────┘       └──────────┘       └──────────┘      └─────────┘
    │                │                  │                  │                 │
   raw/           bronze/            silver/          DuckDB             DuckDB
  *.csv           orders/            orders/          staging/           marts/
  *.json         events/            events/        intermediate/       analytics/
```

---

## 2. Project Structure

```
tarj/
├── pyproject.toml                    # Python dependencies
├── Makefile                          # Orchestration commands
├── .gitignore
│
├── config/
│   ├── __init__.py
│   └── settings.py                   # Central configuration
│
├── data/
│   ├── raw/                          # Generated source files
│   │   ├── orders.csv
│   │   ├── order_items.csv
│   │   ├── delivery_events.json
│   │   ├── restaurants.csv
│   │   ├── riders.csv
│   │   ├── refunds.csv
│   │   └── support_tickets.csv
│   ├── bronze/                       # Spark parquet (raw + metadata)
│   │   ├── orders/
│   │   ├── order_items/
│   │   ├── delivery_events/
│   │   ├── restaurants/
│   │   ├── riders/
│   │   ├── refunds/
│   │   └── support_tickets/
│   ├── silver/                       # Cleaned parquet
│   │   └── [same structure as bronze]
│   └── warehouse/
│       └── analytics.duckdb          # DuckDB database
│
├── src/
│   ├── __init__.py
│   ├── generators/                   # Data generation
│   │   ├── __init__.py
│   │   ├── base.py
│   │   ├── orders.py
│   │   ├── order_items.py
│   │   ├── delivery_events.py
│   │   ├── restaurants.py
│   │   ├── riders.py
│   │   ├── refunds.py
│   │   ├── support_tickets.py
│   │   └── orchestrator.py
│   │
│   ├── spark_jobs/
│   │   ├── __init__.py
│   │   ├── common/
│   │   │   ├── __init__.py
│   │   │   ├── schemas.py
│   │   │   └── quality_checks.py
│   │   ├── bronze/
│   │   │   ├── __init__.py
│   │   │   └── ingest_raw.py
│   │   ├── silver/
│   │   │   ├── __init__.py
│   │   │   ├── clean_orders.py
│   │   │   └── clean_delivery_events.py
│   │   └── run_pipeline.py
│   │
│   └── loaders/
│       ├── __init__.py
│       └── silver_to_duckdb.py
│
├── dbt_project/
│   ├── dbt_project.yml
│   ├── profiles.yml
│   ├── packages.yml
│   │
│   ├── models/
│   │   ├── staging/
│   │   │   ├── _staging.yml
│   │   │   ├── stg_orders.sql
│   │   │   ├── stg_order_items.sql
│   │   │   ├── stg_delivery_events.sql
│   │   │   ├── stg_restaurants.sql
│   │   │   ├── stg_riders.sql
│   │   │   ├── stg_refunds.sql
│   │   │   └── stg_support_tickets.sql
│   │   │
│   │   ├── intermediate/
│   │   │   ├── _intermediate.yml
│   │   │   ├── int_order_delivery_timeline.sql
│   │   │   ├── int_order_refund_joined.sql
│   │   │   ├── int_rider_order_metrics.sql
│   │   │   └── int_restaurant_prep_times.sql
│   │   │
│   │   └── marts/
│   │       ├── core/
│   │       │   ├── _core.yml
│   │       │   ├── dim_restaurants.sql
│   │       │   ├── dim_riders.sql
│   │       │   ├── dim_date.sql
│   │       │   └── fct_orders.sql
│   │       └── analytics/
│   │           ├── _analytics.yml
│   │           ├── mart_sla_breach_analysis.sql
│   │           ├── mart_restaurant_prep_delays.sql
│   │           ├── mart_refund_drivers.sql
│   │           ├── mart_rider_performance.sql
│   │           └── mart_weekly_trends.sql
│   │
│   ├── tests/
│   │   └── assert_refund_pct_sums_to_100.sql
│   │
│   ├── macros/
│   │   ├── datediff_minutes.sql
│   │   └── time_slot_bucket.sql
│   │
│   └── seeds/
│       └── time_slots.csv
│
├── tests/
│   ├── __init__.py
│   ├── conftest.py
│   ├── test_generators/
│   │   └── test_data_quality.py
│   └── test_spark_jobs/
│       └── test_silver_transforms.py
│
├── notebooks/
│   ├── 01_data_exploration.ipynb
│   └── 02_validate_marts.ipynb
│
└── docs/
    ├── SOLUTION.md
    └── ARCHITECTURE.md
```

---

## 3. Data Schemas

### 3.1 Raw Layer Schemas

#### orders.csv
| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| order_id | STRING | No | Primary key, format: ORD-XXXXXXXXXXXX |
| customer_id | STRING | Yes | Customer identifier |
| restaurant_id | STRING | No | FK to restaurants |
| city | STRING | Yes | City name |
| order_ts | TIMESTAMP | No | Order placement time |
| promised_delivery_ts | TIMESTAMP | Yes | Promised delivery time |
| status | STRING | Yes | delivered, cancelled, in_progress |
| order_value | DOUBLE | Yes | Order amount in INR |
| payment_mode | STRING | Yes | UPI, Card, COD, Wallet |

#### delivery_events.json
| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| order_id | STRING | No | FK to orders |
| rider_id | STRING | Yes | FK to riders (null for pre-assignment events) |
| event_type | STRING | No | Event name (see event sequence) |
| event_ts | TIMESTAMP | No | Event occurrence time |
| latitude | DOUBLE | Yes | GPS latitude |
| longitude | DOUBLE | Yes | GPS longitude |

**Event Type Sequence**:
```
1. order_confirmed
2. restaurant_accepted
3. food_prep_started
4. food_ready
5. rider_assigned
6. rider_picked_up
7. out_for_delivery
8. delivered / delivery_failed / cancelled
```

#### order_items.csv
| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| order_id | STRING | No | FK to orders |
| item_id | STRING | No | Item identifier |
| quantity | INTEGER | Yes | Quantity ordered |
| item_price | DOUBLE | Yes | Price per item |
| cuisine_type | STRING | Yes | Cuisine category |

#### restaurants.csv
| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| restaurant_id | STRING | No | Primary key |
| city | STRING | Yes | City location |
| cuisine_type | STRING | Yes | Primary cuisine |
| rating_band | STRING | Yes | A, B, C, D rating |
| onboarding_date | DATE | Yes | Date joined platform |

#### riders.csv
| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| rider_id | STRING | No | Primary key |
| city | STRING | Yes | Operating city |
| shift_type | STRING | Yes | morning, evening, night |
| joining_date | DATE | Yes | Date joined |

#### refunds.csv
| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| refund_id | STRING | No | Primary key |
| order_id | STRING | No | FK to orders |
| refund_ts | TIMESTAMP | Yes | Refund processing time |
| refund_reason | STRING | Yes | Reason category |
| refund_amount | DOUBLE | Yes | Refund amount in INR |

#### support_tickets.csv
| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| ticket_id | STRING | No | Primary key |
| order_id | STRING | No | FK to orders |
| ticket_type | STRING | Yes | Issue category |
| created_ts | TIMESTAMP | Yes | Ticket creation time |
| resolution_status | STRING | Yes | open, resolved, escalated |

### 3.2 Bronze Layer Additions

Every bronze table adds these metadata columns:

| Column | Type | Description |
|--------|------|-------------|
| _source_file | STRING | Input file path |
| _ingested_at | TIMESTAMP | Ingestion timestamp |
| _batch_id | STRING | Processing batch identifier |
| _corrupt_record | STRING | Raw data if schema parsing failed |

### 3.3 Silver Layer Additions

Every silver table adds data quality flags:

| Column | Type | Description |
|--------|------|-------------|
| _dq_has_null_customer | BOOLEAN | Customer ID was null |
| _dq_invalid_order_value | BOOLEAN | Order value <= 0 or null |
| _dq_orphan_order | BOOLEAN | FK doesn't exist in parent table |
| _dq_invalid_sequence | BOOLEAN | Event arrived out of sequence |

**Derived Columns (Orders)**:
- `order_date`: DATE extracted from order_ts
- `order_hour`: INTEGER hour component
- `time_slot`: STRING (morning/lunch/evening/dinner)
- `day_of_week`: INTEGER (1=Sunday, 7=Saturday)
- `is_weekend`: BOOLEAN

---

## 4. Data Generation

### 4.1 Configuration

```python
# config/settings.py
from datetime import date
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent

DATA_CONFIG = {
    "date_range": {
        "start": date(2026, 1, 15),  # 67 days before current date
        "end": date(2026, 3, 22),     # Yesterday
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
```

### 4.2 Base Generator (Full Code)

```python
# src/generators/base.py
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Optional, List
import pandas as pd
import numpy as np
from faker import Faker


@dataclass
class GeneratorConfig:
    """Configuration for data generators."""
    seed: int = 42
    null_rate: float = 0.02
    duplicate_rate: float = 0.005

    def __post_init__(self):
        np.random.seed(self.seed)
        Faker.seed(self.seed)


class BaseGenerator(ABC):
    """Abstract base class for all data generators."""

    def __init__(self, config: GeneratorConfig):
        self.config = config
        self.faker = Faker("en_IN")

    @abstractmethod
    def generate(self) -> pd.DataFrame:
        """Generate the dataset. Must be implemented by subclasses."""
        pass

    def inject_nulls(
        self,
        df: pd.DataFrame,
        columns: List[str],
        rate: Optional[float] = None
    ) -> pd.DataFrame:
        """
        Inject null values into specified columns.

        Args:
            df: Input DataFrame
            columns: Columns to inject nulls into
            rate: Override default null rate

        Returns:
            DataFrame with nulls injected
        """
        rate = rate or self.config.null_rate
        df = df.copy()

        for col in columns:
            if col in df.columns:
                mask = np.random.random(len(df)) < rate
                df.loc[mask, col] = None

        return df

    def inject_duplicates(
        self,
        df: pd.DataFrame,
        rate: Optional[float] = None
    ) -> pd.DataFrame:
        """
        Inject duplicate rows at specified rate.

        Args:
            df: Input DataFrame
            rate: Override default duplicate rate

        Returns:
            DataFrame with duplicates added
        """
        rate = rate or self.config.duplicate_rate
        n_dups = int(len(df) * rate)

        if n_dups == 0:
            return df

        dup_indices = np.random.choice(df.index, n_dups, replace=True)
        duplicates = df.loc[dup_indices].copy()

        return pd.concat([df, duplicates], ignore_index=True)

    def save(self, df: pd.DataFrame, path: str, format: str = "csv") -> None:
        """
        Save DataFrame to file.

        Args:
            df: DataFrame to save
            path: Output file path
            format: 'csv' or 'json'
        """
        if format == "csv":
            df.to_csv(path, index=False)
        elif format == "json":
            df.to_json(path, orient="records", indent=2)
        else:
            raise ValueError(f"Unknown format: {format}")
```

### 4.3 Orders Generator (Full Code)

```python
# src/generators/orders.py
import uuid
from datetime import datetime, timedelta
from typing import Tuple, List
import pandas as pd
import numpy as np

from .base import BaseGenerator, GeneratorConfig


class OrdersGenerator(BaseGenerator):
    """Generate orders dataset with realistic patterns."""

    # Hour distribution weights (8AM to midnight)
    HOUR_WEIGHTS = [
        1, 2, 3, 8, 10, 6,   # 8-13 (lunch peak at 12-13)
        3, 3, 4, 7, 10, 8,   # 14-19 (dinner peak at 19-20)
        6, 4, 2, 1           # 20-23
    ]

    STATUS_WEIGHTS = {
        "delivered": 0.85,
        "cancelled": 0.10,
        "in_progress": 0.05,
    }

    PAYMENT_MODES = {
        "UPI": 0.55,
        "Card": 0.20,
        "COD": 0.15,
        "Wallet": 0.10,
    }

    def __init__(
        self,
        config: GeneratorConfig,
        restaurants_df: pd.DataFrame,
        date_range: Tuple[datetime, datetime],
        cities: List[str],
        orders_per_day_range: Tuple[int, int] = (800, 1500),
        sla_breach_rate: float = 0.12,
    ):
        super().__init__(config)
        self.restaurants = restaurants_df
        self.start_date, self.end_date = date_range
        self.cities = cities
        self.orders_per_day_range = orders_per_day_range
        self.sla_breach_rate = sla_breach_rate

        # Pre-compute restaurant city mapping for efficiency
        self._restaurant_by_city = restaurants_df.groupby("city").apply(
            lambda x: x.to_dict("records")
        ).to_dict()

    def generate(self) -> pd.DataFrame:
        """Generate complete orders dataset."""
        orders = []
        current = self.start_date

        while current <= self.end_date:
            # Volume varies by day of week (weekends higher)
            base_volume = np.random.randint(*self.orders_per_day_range)
            dow_multiplier = 1.3 if current.weekday() >= 5 else 1.0
            daily_orders = int(base_volume * dow_multiplier)

            for _ in range(daily_orders):
                order = self._generate_single_order(current)
                orders.append(order)

            current += timedelta(days=1)

        df = pd.DataFrame(orders)

        # Inject realistic defects
        df = self.inject_nulls(df, ["payment_mode"])
        df = self.inject_duplicates(df)

        # Remove internal column before saving
        return df.drop(columns=["_will_breach_sla"])

    def _generate_single_order(self, date: datetime) -> dict:
        """Generate a single order record."""
        # Select hour with lunch/dinner peaks
        hour = self._weighted_hour_selection()
        minute = np.random.randint(0, 60)
        second = np.random.randint(0, 60)
        order_ts = datetime(
            date.year, date.month, date.day,
            hour, minute, second
        )

        # Select city and restaurant
        city = np.random.choice(self.cities)
        restaurant = self._select_restaurant(city)

        # SLA: 30-45 minutes promise
        sla_minutes = np.random.randint(30, 45)
        promised_ts = order_ts + timedelta(minutes=sla_minutes)

        # Determine if this will breach SLA (used by delivery events generator)
        will_breach = np.random.random() < self.sla_breach_rate

        # Status distribution
        status = np.random.choice(
            list(self.STATUS_WEIGHTS.keys()),
            p=list(self.STATUS_WEIGHTS.values())
        )

        # Order value: log-normal distribution (Rs 150-800 typical)
        order_value = round(np.random.lognormal(5.5, 0.6), 2)
        order_value = max(50, min(order_value, 5000))  # Clamp to realistic range

        # Payment mode
        payment_mode = np.random.choice(
            list(self.PAYMENT_MODES.keys()),
            p=list(self.PAYMENT_MODES.values())
        )

        return {
            "order_id": f"ORD-{uuid.uuid4().hex[:12].upper()}",
            "customer_id": f"CUST-{np.random.randint(10000, 99999)}",
            "restaurant_id": restaurant["restaurant_id"],
            "city": city,
            "order_ts": order_ts.isoformat(),
            "promised_delivery_ts": promised_ts.isoformat(),
            "status": status,
            "order_value": order_value,
            "payment_mode": payment_mode,
            "_will_breach_sla": will_breach,  # Internal flag
        }

    def _weighted_hour_selection(self) -> int:
        """Select hour with realistic distribution."""
        hours = list(range(8, 24))
        weights = np.array(self.HOUR_WEIGHTS) / sum(self.HOUR_WEIGHTS)
        return np.random.choice(hours, p=weights)

    def _select_restaurant(self, city: str) -> dict:
        """Select a restaurant from the given city."""
        city_restaurants = self._restaurant_by_city.get(city, [])
        if not city_restaurants:
            # Fallback to any restaurant if city has none
            return self.restaurants.sample(1).to_dict("records")[0]
        return np.random.choice(city_restaurants)
```

### 4.4 Delivery Events Generator (Full Code)

```python
# src/generators/delivery_events.py
import uuid
from datetime import timedelta
from typing import List, Dict, Any
import pandas as pd
import numpy as np

from .base import BaseGenerator, GeneratorConfig


class DeliveryEventsGenerator(BaseGenerator):
    """Generate delivery events with realistic sequences and late arrivals."""

    # Standard event sequence
    EVENT_SEQUENCE = [
        "order_confirmed",
        "restaurant_accepted",
        "food_prep_started",
        "food_ready",
        "rider_assigned",
        "rider_picked_up",
        "out_for_delivery",
        "delivered",
    ]

    # Typical time between events (minutes)
    EVENT_DELTAS = {
        "order_confirmed": (0, 0),      # Immediate
        "restaurant_accepted": (1, 3),   # 1-3 min after order
        "food_prep_started": (2, 5),     # 2-5 min after accept
        "food_ready": (10, 25),          # Prep takes 10-25 min
        "rider_assigned": (1, 5),        # 1-5 min to find rider
        "rider_picked_up": (5, 15),      # Rider travel time
        "out_for_delivery": (0, 1),      # Immediate after pickup
        "delivered": (10, 20),           # Delivery takes 10-20 min
    }

    # Delhi/Mumbai coordinates for realistic GPS
    CITY_COORDS = {
        "Mumbai": (19.076, 72.877),
        "Delhi": (28.613, 77.209),
        "Bangalore": (12.971, 77.594),
        "Chennai": (13.082, 80.270),
        "Hyderabad": (17.385, 78.486),
        "Pune": (18.520, 73.856),
        "Kolkata": (22.572, 88.363),
        "Ahmedabad": (23.022, 72.571),
    }

    def __init__(
        self,
        config: GeneratorConfig,
        orders_df: pd.DataFrame,
        riders_df: pd.DataFrame,
        late_event_rate: float = 0.03,
        orphan_rate: float = 0.01,
    ):
        super().__init__(config)
        self.orders = orders_df
        self.riders = riders_df
        self.late_event_rate = late_event_rate
        self.orphan_rate = orphan_rate

        # Pre-compute riders by city
        self._riders_by_city = riders_df.groupby("city").apply(
            lambda x: x["rider_id"].tolist()
        ).to_dict()

    def generate(self) -> List[Dict[str, Any]]:
        """Generate all delivery events."""
        events = []

        for _, order in self.orders.iterrows():
            order_events = self._generate_order_events(order)
            events.extend(order_events)

        # Inject orphan events (invalid order_ids)
        events = self._inject_orphans(events)

        return events

    def _generate_order_events(self, order: pd.Series) -> List[Dict]:
        """Generate event sequence for a single order."""
        events = []
        order_id = order["order_id"]
        city = order.get("city", "Mumbai")
        status = order.get("status", "delivered")
        will_breach = order.get("_will_breach_sla", False)

        # Determine event chain based on status
        if status == "cancelled":
            # Cancelled orders have partial chains
            n_events = np.random.randint(1, 4)
            event_chain = self.EVENT_SEQUENCE[:n_events] + ["cancelled"]
        else:
            event_chain = self.EVENT_SEQUENCE.copy()

        # Select rider for this order
        city_riders = self._riders_by_city.get(city, [])
        rider_id = np.random.choice(city_riders) if city_riders else None

        # Base coordinates for this city
        base_lat, base_lon = self.CITY_COORDS.get(city, (19.076, 72.877))

        # Track current timestamp
        current_ts = pd.to_datetime(order["order_ts"])

        for event_type in event_chain:
            # Calculate time delta (with potential delay for breached orders)
            delta = self._get_event_delta(event_type, will_breach)
            current_ts += timedelta(minutes=delta)

            # Determine if this event arrives late
            ingestion_delay = 0
            if np.random.random() < self.late_event_rate:
                ingestion_delay = np.random.randint(60, 24 * 60)  # 1-24 hours

            # Rider ID only for rider-related events
            event_rider_id = None
            if event_type in ["rider_assigned", "rider_picked_up",
                             "out_for_delivery", "delivered"]:
                event_rider_id = rider_id

            event = {
                "order_id": order_id,
                "rider_id": event_rider_id,
                "event_type": event_type,
                "event_ts": current_ts.isoformat(),
                "latitude": self._jitter_coord(base_lat, 0.02),
                "longitude": self._jitter_coord(base_lon, 0.02),
                "_ingestion_delay_minutes": ingestion_delay,  # For testing
            }
            events.append(event)

        return events

    def _get_event_delta(self, event_type: str, will_breach: bool) -> int:
        """Get time delta for event, with extra delay for breached orders."""
        min_delta, max_delta = self.EVENT_DELTAS.get(event_type, (1, 5))
        delta = np.random.randint(min_delta, max_delta + 1)

        # Add extra delay for breached orders (in prep or delivery)
        if will_breach and event_type in ["food_ready", "delivered"]:
            delta += np.random.randint(10, 30)

        return delta

    def _jitter_coord(self, coord: float, range_km: float) -> float:
        """Add small random jitter to coordinate."""
        # ~0.01 degrees ≈ 1.1 km
        jitter = np.random.uniform(-range_km * 0.01, range_km * 0.01)
        return round(coord + jitter, 6)

    def _inject_orphans(self, events: List[Dict]) -> List[Dict]:
        """Inject orphan events with invalid order_ids."""
        n_orphans = int(len(events) * self.orphan_rate)
        orphan_indices = np.random.choice(len(events), n_orphans, replace=False)

        for i in orphan_indices:
            events[i]["order_id"] = f"ORD-INVALID-{uuid.uuid4().hex[:8].upper()}"

        return events
```

### 4.5 Refunds Generator (Full Code)

```python
# src/generators/refunds.py
import uuid
from datetime import timedelta
import pandas as pd
import numpy as np

from .base import BaseGenerator, GeneratorConfig


class RefundsGenerator(BaseGenerator):
    """Generate refunds linked to orders with reason distribution."""

    REFUND_REASONS = {
        "Delay": 0.35,
        "Missing_Items": 0.30,
        "Cancellation": 0.20,
        "Wrong_Order": 0.10,
        "Quality_Issue": 0.05,
    }

    # Refund amount as percentage of order value by reason
    REFUND_PERCENTAGES = {
        "Cancellation": (1.0, 1.0),      # Full refund
        "Missing_Items": (0.2, 0.5),      # Partial refund
        "Wrong_Order": (0.5, 1.0),        # Significant refund
        "Delay": (0.1, 0.3),              # Small compensation
        "Quality_Issue": (0.3, 0.7),      # Varies
    }

    def __init__(
        self,
        config: GeneratorConfig,
        orders_df: pd.DataFrame,
        refund_rate: float = 0.08,
    ):
        super().__init__(config)
        self.orders = orders_df
        self.refund_rate = refund_rate

    def generate(self) -> pd.DataFrame:
        """Generate refunds dataset."""
        refunds = []

        # Only delivered and cancelled orders can have refunds
        eligible = self.orders[
            self.orders["status"].isin(["delivered", "cancelled"])
        ]

        # Sample orders that will have refunds
        n_refunds = int(len(eligible) * self.refund_rate)
        refund_orders = eligible.sample(n=n_refunds)

        for _, order in refund_orders.iterrows():
            refund = self._generate_refund(order)
            refunds.append(refund)

        df = pd.DataFrame(refunds)

        # Inject nulls in reason (some reasons unknown)
        df = self.inject_nulls(df, ["refund_reason"])

        return df

    def _generate_refund(self, order: pd.Series) -> dict:
        """Generate a single refund record."""
        # Select reason
        reason = np.random.choice(
            list(self.REFUND_REASONS.keys()),
            p=list(self.REFUND_REASONS.values())
        )

        # Calculate refund amount
        min_pct, max_pct = self.REFUND_PERCENTAGES[reason]
        refund_pct = np.random.uniform(min_pct, max_pct)
        refund_amount = round(order["order_value"] * refund_pct, 2)

        # Refund happens 1-72 hours after order
        order_ts = pd.to_datetime(order["order_ts"])
        refund_ts = order_ts + timedelta(hours=np.random.randint(1, 72))

        return {
            "refund_id": f"REF-{uuid.uuid4().hex[:10].upper()}",
            "order_id": order["order_id"],
            "refund_ts": refund_ts.isoformat(),
            "refund_reason": reason,
            "refund_amount": refund_amount,
        }
```

### 4.6 Orchestrator (Pattern)

```python
# src/generators/orchestrator.py
from pathlib import Path
from config.settings import DATA_CONFIG, PATHS
from .base import GeneratorConfig
from .restaurants import RestaurantsGenerator
from .riders import RidersGenerator
from .orders import OrdersGenerator
from .order_items import OrderItemsGenerator
from .delivery_events import DeliveryEventsGenerator
from .refunds import RefundsGenerator
from .support_tickets import SupportTicketsGenerator


def generate_all(seed: int = 42) -> None:
    """Generate all datasets in correct dependency order."""
    config = GeneratorConfig(seed=seed)
    output_dir = PATHS["raw"]
    output_dir.mkdir(parents=True, exist_ok=True)

    print("Generating restaurants...")
    restaurants = RestaurantsGenerator(config, DATA_CONFIG).generate()
    restaurants.to_csv(output_dir / "restaurants.csv", index=False)

    print("Generating riders...")
    riders = RidersGenerator(config, DATA_CONFIG).generate()
    riders.to_csv(output_dir / "riders.csv", index=False)

    print("Generating orders...")
    orders_gen = OrdersGenerator(
        config=config,
        restaurants_df=restaurants,
        date_range=(DATA_CONFIG["date_range"]["start"],
                   DATA_CONFIG["date_range"]["end"]),
        cities=DATA_CONFIG["volumes"]["cities"],
    )
    orders = orders_gen.generate()
    orders.to_csv(output_dir / "orders.csv", index=False)

    print("Generating order items...")
    order_items = OrderItemsGenerator(config, orders).generate()
    order_items.to_csv(output_dir / "order_items.csv", index=False)

    print("Generating delivery events...")
    # Need orders with _will_breach_sla column
    orders_with_breach = orders_gen.generate()  # Re-generate with internal column
    events_gen = DeliveryEventsGenerator(config, orders_with_breach, riders)
    events = events_gen.generate()
    import json
    with open(output_dir / "delivery_events.json", "w") as f:
        json.dump(events, f, indent=2)

    print("Generating refunds...")
    refunds = RefundsGenerator(config, orders).generate()
    refunds.to_csv(output_dir / "refunds.csv", index=False)

    print("Generating support tickets...")
    tickets = SupportTicketsGenerator(config, orders).generate()
    tickets.to_csv(output_dir / "support_tickets.csv", index=False)

    print(f"All data generated in {output_dir}")


if __name__ == "__main__":
    generate_all()
```

---

## 5. Spark Jobs

### 5.1 Schema Definitions (Full Code)

```python
# src/spark_jobs/common/schemas.py
from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType,
    DoubleType, IntegerType, DateType, BooleanType
)

ORDERS_SCHEMA = StructType([
    StructField("order_id", StringType(), nullable=False),
    StructField("customer_id", StringType(), nullable=True),
    StructField("restaurant_id", StringType(), nullable=False),
    StructField("city", StringType(), nullable=True),
    StructField("order_ts", TimestampType(), nullable=False),
    StructField("promised_delivery_ts", TimestampType(), nullable=True),
    StructField("status", StringType(), nullable=True),
    StructField("order_value", DoubleType(), nullable=True),
    StructField("payment_mode", StringType(), nullable=True),
])

ORDER_ITEMS_SCHEMA = StructType([
    StructField("order_id", StringType(), nullable=False),
    StructField("item_id", StringType(), nullable=False),
    StructField("quantity", IntegerType(), nullable=True),
    StructField("item_price", DoubleType(), nullable=True),
    StructField("cuisine_type", StringType(), nullable=True),
])

DELIVERY_EVENTS_SCHEMA = StructType([
    StructField("order_id", StringType(), nullable=False),
    StructField("rider_id", StringType(), nullable=True),
    StructField("event_type", StringType(), nullable=False),
    StructField("event_ts", TimestampType(), nullable=False),
    StructField("latitude", DoubleType(), nullable=True),
    StructField("longitude", DoubleType(), nullable=True),
])

RESTAURANTS_SCHEMA = StructType([
    StructField("restaurant_id", StringType(), nullable=False),
    StructField("city", StringType(), nullable=True),
    StructField("cuisine_type", StringType(), nullable=True),
    StructField("rating_band", StringType(), nullable=True),
    StructField("onboarding_date", DateType(), nullable=True),
])

RIDERS_SCHEMA = StructType([
    StructField("rider_id", StringType(), nullable=False),
    StructField("city", StringType(), nullable=True),
    StructField("shift_type", StringType(), nullable=True),
    StructField("joining_date", DateType(), nullable=True),
])

REFUNDS_SCHEMA = StructType([
    StructField("refund_id", StringType(), nullable=False),
    StructField("order_id", StringType(), nullable=False),
    StructField("refund_ts", TimestampType(), nullable=True),
    StructField("refund_reason", StringType(), nullable=True),
    StructField("refund_amount", DoubleType(), nullable=True),
])

SUPPORT_TICKETS_SCHEMA = StructType([
    StructField("ticket_id", StringType(), nullable=False),
    StructField("order_id", StringType(), nullable=False),
    StructField("ticket_type", StringType(), nullable=True),
    StructField("created_ts", TimestampType(), nullable=True),
    StructField("resolution_status", StringType(), nullable=True),
])
```

### 5.2 Bronze Ingestion (Full Code)

```python
# src/spark_jobs/bronze/ingest_raw.py
from datetime import datetime
from pathlib import Path
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    current_timestamp, input_file_name, lit
)
from pyspark.sql.types import StringType

from ..common.schemas import (
    ORDERS_SCHEMA, ORDER_ITEMS_SCHEMA, DELIVERY_EVENTS_SCHEMA,
    RESTAURANTS_SCHEMA, RIDERS_SCHEMA, REFUNDS_SCHEMA,
    SUPPORT_TICKETS_SCHEMA
)


class BronzeIngestion:
    """Ingest raw files to bronze layer with metadata."""

    def __init__(
        self,
        spark: SparkSession,
        raw_path: str,
        bronze_path: str
    ):
        self.spark = spark
        self.raw_path = Path(raw_path)
        self.bronze_path = Path(bronze_path)
        self.batch_id = datetime.now().strftime("%Y%m%d_%H%M%S")

    def ingest_csv(
        self,
        filename: str,
        schema,
        output_name: str = None
    ) -> DataFrame:
        """
        Ingest CSV file to bronze parquet.

        Args:
            filename: Source CSV filename
            schema: PySpark schema
            output_name: Output folder name (defaults to filename without .csv)
        """
        output_name = output_name or filename.replace(".csv", "")

        # Add corrupt record column to schema
        extended_schema = schema.add(
            StructField("_corrupt_record", StringType(), True)
        )

        # Read with permissive mode
        df = (self.spark.read
              .option("header", "true")
              .option("mode", "PERMISSIVE")
              .option("columnNameOfCorruptRecord", "_corrupt_record")
              .schema(extended_schema)
              .csv(str(self.raw_path / filename)))

        # Add metadata columns
        df_with_meta = self._add_metadata(df)

        # Write to bronze
        output_path = self.bronze_path / output_name
        (df_with_meta.write
         .mode("overwrite")
         .parquet(str(output_path)))

        print(f"Ingested {filename} -> {output_path} ({df.count()} rows)")
        return df_with_meta

    def ingest_json(
        self,
        filename: str,
        schema,
        output_name: str = None
    ) -> DataFrame:
        """
        Ingest JSON file to bronze parquet.

        Args:
            filename: Source JSON filename
            schema: PySpark schema
            output_name: Output folder name (defaults to filename without .json)
        """
        output_name = output_name or filename.replace(".json", "")

        df = (self.spark.read
              .option("mode", "PERMISSIVE")
              .option("multiLine", "true")
              .schema(schema)
              .json(str(self.raw_path / filename)))

        df_with_meta = self._add_metadata(df)

        output_path = self.bronze_path / output_name
        (df_with_meta.write
         .mode("overwrite")
         .parquet(str(output_path)))

        print(f"Ingested {filename} -> {output_path} ({df.count()} rows)")
        return df_with_meta

    def _add_metadata(self, df: DataFrame) -> DataFrame:
        """Add standard metadata columns."""
        return (df
            .withColumn("_source_file", input_file_name())
            .withColumn("_ingested_at", current_timestamp())
            .withColumn("_batch_id", lit(self.batch_id)))

    def run_all(self) -> None:
        """Ingest all raw files to bronze."""
        self.bronze_path.mkdir(parents=True, exist_ok=True)

        # CSV files
        csv_mappings = [
            ("orders.csv", ORDERS_SCHEMA),
            ("order_items.csv", ORDER_ITEMS_SCHEMA),
            ("restaurants.csv", RESTAURANTS_SCHEMA),
            ("riders.csv", RIDERS_SCHEMA),
            ("refunds.csv", REFUNDS_SCHEMA),
            ("support_tickets.csv", SUPPORT_TICKETS_SCHEMA),
        ]

        for filename, schema in csv_mappings:
            self.ingest_csv(filename, schema)

        # JSON files
        self.ingest_json("delivery_events.json", DELIVERY_EVENTS_SCHEMA)

        print(f"Bronze ingestion complete. Batch ID: {self.batch_id}")
```

### 5.3 Silver Transformation - Orders (Full Code)

```python
# src/spark_jobs/silver/clean_orders.py
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, when, coalesce, trim, upper, lit,
    row_number, hour, dayofweek, current_timestamp
)
from pyspark.sql.window import Window


class OrdersCleaner:
    """Clean and transform orders from bronze to silver."""

    def __init__(
        self,
        spark: SparkSession,
        bronze_path: str,
        silver_path: str
    ):
        self.spark = spark
        self.bronze_path = bronze_path
        self.silver_path = silver_path

    def read_bronze(self) -> DataFrame:
        """Read bronze orders."""
        return self.spark.read.parquet(f"{self.bronze_path}/orders")

    def clean(self, df: DataFrame) -> DataFrame:
        """Apply all cleaning transformations."""
        return (df
            .transform(self._remove_corrupt_records)
            .transform(self._deduplicate)
            .transform(self._standardize_fields)
            .transform(self._handle_nulls)
            .transform(self._add_derived_columns)
            .transform(self._add_data_quality_flags))

    def _remove_corrupt_records(self, df: DataFrame) -> DataFrame:
        """Remove records that failed schema parsing."""
        return (df
            .filter(col("_corrupt_record").isNull())
            .drop("_corrupt_record"))

    def _deduplicate(self, df: DataFrame) -> DataFrame:
        """Keep latest record per order_id based on ingestion time."""
        window = (Window
            .partitionBy("order_id")
            .orderBy(col("_ingested_at").desc()))

        return (df
            .withColumn("_row_num", row_number().over(window))
            .filter(col("_row_num") == 1)
            .drop("_row_num"))

    def _standardize_fields(self, df: DataFrame) -> DataFrame:
        """Normalize string fields."""
        return (df
            .withColumn("city", upper(trim(col("city"))))
            .withColumn("status", trim(col("status")))
            .withColumn("payment_mode", upper(trim(col("payment_mode")))))

    def _handle_nulls(self, df: DataFrame) -> DataFrame:
        """Apply null handling rules."""
        return (df
            .withColumn("payment_mode",
                coalesce(col("payment_mode"), lit("UNKNOWN")))
            .withColumn("order_value",
                when(col("order_value") < 0, lit(None))
                .otherwise(col("order_value"))))

    def _add_derived_columns(self, df: DataFrame) -> DataFrame:
        """Add computed columns for downstream use."""
        return (df
            .withColumn("order_date", col("order_ts").cast("date"))
            .withColumn("order_hour", hour(col("order_ts")))
            .withColumn("time_slot",
                when(col("order_hour").between(8, 11), lit("morning"))
                .when(col("order_hour").between(12, 15), lit("lunch"))
                .when(col("order_hour").between(16, 19), lit("evening"))
                .when(col("order_hour").between(20, 23), lit("dinner"))
                .otherwise(lit("late_night")))
            .withColumn("day_of_week", dayofweek(col("order_ts")))
            .withColumn("is_weekend", col("day_of_week").isin([1, 7])))

    def _add_data_quality_flags(self, df: DataFrame) -> DataFrame:
        """Add DQ flags for monitoring."""
        return (df
            .withColumn("_dq_has_null_customer",
                col("customer_id").isNull())
            .withColumn("_dq_invalid_order_value",
                (col("order_value").isNull()) | (col("order_value") <= 0))
            .withColumn("_dq_future_order",
                col("order_ts") > current_timestamp()))

    def write_silver(self, df: DataFrame) -> None:
        """Write cleaned data to silver layer."""
        (df.write
         .mode("overwrite")
         .partitionBy("order_date")
         .parquet(f"{self.silver_path}/orders"))

        print(f"Silver orders written ({df.count()} rows)")
```

### 5.4 Pipeline Orchestrator (Pattern)

```python
# src/spark_jobs/run_pipeline.py
from pyspark.sql import SparkSession
from pathlib import Path
import sys

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from config.settings import PATHS
from src.spark_jobs.bronze.ingest_raw import BronzeIngestion
from src.spark_jobs.silver.clean_orders import OrdersCleaner
# ... import other cleaners


def create_spark_session() -> SparkSession:
    """Create Spark session with local config."""
    return (SparkSession.builder
        .appName("FoodDeliveryPipeline")
        .master("local[*]")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.driver.memory", "4g")
        .getOrCreate())


def run_pipeline():
    """Run full bronze → silver pipeline."""
    spark = create_spark_session()

    try:
        # Bronze layer
        print("=" * 50)
        print("BRONZE LAYER")
        print("=" * 50)
        bronze = BronzeIngestion(
            spark=spark,
            raw_path=str(PATHS["raw"]),
            bronze_path=str(PATHS["bronze"])
        )
        bronze.run_all()

        # Silver layer
        print("=" * 50)
        print("SILVER LAYER")
        print("=" * 50)

        cleaners = [
            OrdersCleaner,
            # OrderItemsCleaner,
            # DeliveryEventsCleaner,
            # ... other cleaners
        ]

        for cleaner_class in cleaners:
            cleaner = cleaner_class(
                spark=spark,
                bronze_path=str(PATHS["bronze"]),
                silver_path=str(PATHS["silver"])
            )
            df = cleaner.read_bronze()
            cleaned = cleaner.clean(df)
            cleaner.write_silver(cleaned)

        print("=" * 50)
        print("PIPELINE COMPLETE")
        print("=" * 50)

    finally:
        spark.stop()


if __name__ == "__main__":
    run_pipeline()
```

---

## 6. dbt Models

### 6.1 Project Configuration

```yaml
# dbt_project/dbt_project.yml
name: 'food_delivery_analytics'
version: '1.0.0'
config-version: 2

profile: 'food_delivery'

model-paths: ["models"]
test-paths: ["tests"]
seed-paths: ["seeds"]
macro-paths: ["macros"]

target-path: "target"
clean-targets: ["target", "dbt_packages"]

models:
  food_delivery_analytics:
    staging:
      +materialized: view
      +schema: staging
    intermediate:
      +materialized: ephemeral
    marts:
      core:
        +materialized: table
        +schema: marts
      analytics:
        +materialized: table
        +schema: analytics
```

```yaml
# dbt_project/profiles.yml
food_delivery:
  target: dev
  outputs:
    dev:
      type: duckdb
      path: '/Users/patel/Code/Tarj/data/warehouse/analytics.duckdb'
      threads: 4
```

### 6.2 Staging Models (Pattern)

```sql
-- dbt_project/models/staging/stg_orders.sql
{{ config(materialized='view') }}

with source as (
    select * from {{ source('silver', 'orders') }}
),

staged as (
    select
        -- Primary key
        order_id,

        -- Foreign keys
        customer_id,
        restaurant_id,

        -- Dimensions
        city,
        status,
        payment_mode,
        time_slot,

        -- Timestamps
        order_ts,
        promised_delivery_ts,
        order_date,
        order_hour,
        day_of_week,
        is_weekend,

        -- Measures
        order_value,

        -- DQ flags
        _dq_has_null_customer,
        _dq_invalid_order_value,
        _ingested_at

    from source
    where _dq_invalid_order_value = false
)

select * from staged
```

### 6.3 Intermediate Models

```sql
-- dbt_project/models/intermediate/int_order_delivery_timeline.sql
{{ config(materialized='ephemeral') }}

with orders as (
    select * from {{ ref('stg_orders') }}
),

delivery_events as (
    select * from {{ ref('stg_delivery_events') }}
),

event_pivot as (
    select
        order_id,
        max(case when event_type = 'order_confirmed' then event_ts end) as confirmed_ts,
        max(case when event_type = 'restaurant_accepted' then event_ts end) as accepted_ts,
        max(case when event_type = 'food_ready' then event_ts end) as food_ready_ts,
        max(case when event_type = 'rider_picked_up' then event_ts end) as picked_up_ts,
        max(case when event_type = 'delivered' then event_ts end) as delivered_ts,
        max(rider_id) as rider_id
    from delivery_events
    group by order_id
),

final as (
    select
        o.order_id,
        o.customer_id,
        o.restaurant_id,
        o.city,
        o.order_ts,
        o.promised_delivery_ts,
        o.status,
        o.order_value,
        o.time_slot,
        o.order_date,
        o.is_weekend,

        ep.confirmed_ts,
        ep.accepted_ts,
        ep.food_ready_ts,
        ep.picked_up_ts,
        ep.delivered_ts,
        ep.rider_id,

        -- Calculated durations (minutes)
        {{ datediff_minutes('ep.accepted_ts', 'ep.food_ready_ts') }} as prep_time_minutes,
        {{ datediff_minutes('ep.picked_up_ts', 'ep.delivered_ts') }} as delivery_time_minutes,
        {{ datediff_minutes('o.order_ts', 'ep.delivered_ts') }} as total_time_minutes,

        -- SLA breach
        case
            when ep.delivered_ts is null then null
            when ep.delivered_ts > o.promised_delivery_ts then true
            else false
        end as is_sla_breached,

        case
            when ep.delivered_ts is null then null
            else {{ datediff_minutes('o.promised_delivery_ts', 'ep.delivered_ts') }}
        end as sla_breach_minutes

    from orders o
    left join event_pivot ep on o.order_id = ep.order_id
)

select * from final
```

### 6.4 Analytics Marts (Full SQL)

#### mart_sla_breach_analysis.sql

```sql
-- dbt_project/models/marts/analytics/mart_sla_breach_analysis.sql
-- Answers: Q1 - Which cities and time slots have highest SLA breach rate?

{{ config(
    materialized='table',
    description='SLA breach analysis by city and time slot'
) }}

with order_timeline as (
    select * from {{ ref('int_order_delivery_timeline') }}
    where status = 'delivered'
),

city_timeslot_metrics as (
    select
        city,
        time_slot,
        order_date,
        count(*) as total_orders,
        sum(case when is_sla_breached then 1 else 0 end) as breached_orders,
        avg(total_time_minutes) as avg_delivery_time_minutes,
        avg(case when is_sla_breached then sla_breach_minutes else 0 end) as avg_breach_minutes
    from order_timeline
    group by city, time_slot, order_date
),

aggregated as (
    select
        city,
        time_slot,
        sum(total_orders) as total_orders,
        sum(breached_orders) as breached_orders,
        round(sum(breached_orders) * 100.0 / nullif(sum(total_orders), 0), 2) as breach_rate_pct,
        round(avg(avg_delivery_time_minutes), 1) as avg_delivery_time_minutes,
        round(avg(avg_breach_minutes), 1) as avg_breach_minutes_when_breached,
        count(distinct order_date) as days_with_data
    from city_timeslot_metrics
    group by city, time_slot
),

ranked as (
    select
        *,
        row_number() over (order by breach_rate_pct desc) as overall_breach_rank,
        row_number() over (partition by city order by breach_rate_pct desc) as city_breach_rank
    from aggregated
)

select * from ranked
order by breach_rate_pct desc
```

#### mart_restaurant_prep_delays.sql

```sql
-- dbt_project/models/marts/analytics/mart_restaurant_prep_delays.sql
-- Answers: Q2 - Which restaurants are causing prep-time delays?

{{ config(
    materialized='table',
    description='Restaurant prep time analysis and risk categorization'
) }}

with order_timeline as (
    select * from {{ ref('int_order_delivery_timeline') }}
    where status = 'delivered'
      and prep_time_minutes is not null
      and prep_time_minutes > 0
),

restaurants as (
    select * from {{ ref('stg_restaurants') }}
),

restaurant_metrics as (
    select
        ot.restaurant_id,
        count(*) as total_orders,

        -- Prep time statistics
        round(avg(ot.prep_time_minutes), 1) as avg_prep_time_minutes,
        round(median(ot.prep_time_minutes), 1) as median_prep_time_minutes,
        round(percentile_cont(0.95) within group (order by ot.prep_time_minutes), 1)
            as p95_prep_time_minutes,
        min(ot.prep_time_minutes) as min_prep_time_minutes,
        max(ot.prep_time_minutes) as max_prep_time_minutes,

        -- Delay counts (>20 min is delayed)
        sum(case when ot.prep_time_minutes > 20 then 1 else 0 end) as delayed_orders,

        -- SLA breaches attributed to prep
        sum(case when ot.is_sla_breached and ot.prep_time_minutes > 20 then 1 else 0 end)
            as sla_breaches_from_prep

    from order_timeline ot
    group by ot.restaurant_id
    having count(*) >= 10  -- Minimum orders for statistical significance
),

final as (
    select
        rm.restaurant_id,
        r.city,
        r.cuisine_type,
        r.rating_band,
        r.onboarding_date,
        rm.total_orders,
        rm.avg_prep_time_minutes,
        rm.median_prep_time_minutes,
        rm.p95_prep_time_minutes,
        rm.min_prep_time_minutes,
        rm.max_prep_time_minutes,
        rm.delayed_orders,
        round(rm.delayed_orders * 100.0 / rm.total_orders, 2) as delay_rate_pct,
        rm.sla_breaches_from_prep,

        -- Risk categorization
        case
            when rm.avg_prep_time_minutes > 25
                 and (rm.delayed_orders * 100.0 / rm.total_orders) > 30
            then 'High Risk'
            when rm.avg_prep_time_minutes > 20
                 and (rm.delayed_orders * 100.0 / rm.total_orders) > 20
            then 'Medium Risk'
            else 'Low Risk'
        end as risk_category,

        current_date - r.onboarding_date as days_since_onboarding

    from restaurant_metrics rm
    left join restaurants r on rm.restaurant_id = r.restaurant_id
)

select * from final
order by delay_rate_pct desc
```

#### mart_refund_drivers.sql

```sql
-- dbt_project/models/marts/analytics/mart_refund_drivers.sql
-- Answers: Q3 - What percentage of refunds are driven by Delay, Missing Items, Cancellations?

{{ config(
    materialized='table',
    description='Refund driver analysis with percentages'
) }}

with order_refunds as (
    select * from {{ ref('int_order_refund_joined') }}
    where has_refund = true
),

-- Overall totals for percentage calculation
totals as (
    select
        count(*) as total_refunds,
        sum(refund_amount) as total_refund_amount
    from order_refunds
),

-- By driver category
by_category as (
    select
        refund_driver_category,
        count(*) as refund_count,
        sum(refund_amount) as refund_amount,
        round(avg(refund_amount), 2) as avg_refund_amount,
        min(refund_amount) as min_refund_amount,
        max(refund_amount) as max_refund_amount
    from order_refunds
    group by refund_driver_category
),

final as (
    select
        bc.refund_driver_category,
        bc.refund_count,
        round(bc.refund_count * 100.0 / t.total_refunds, 2) as refund_count_pct,
        round(bc.refund_amount, 2) as refund_amount,
        round(bc.refund_amount * 100.0 / t.total_refund_amount, 2) as refund_amount_pct,
        bc.avg_refund_amount,
        bc.min_refund_amount,
        bc.max_refund_amount,
        t.total_refunds,
        round(t.total_refund_amount, 2) as total_refund_amount
    from by_category bc
    cross join totals t
)

select * from final
order by refund_count_pct desc
```

#### mart_rider_performance.sql

```sql
-- dbt_project/models/marts/analytics/mart_rider_performance.sql
-- Answers: Q4 - Which riders consistently handle more orders without increasing late deliveries?

{{ config(
    materialized='table',
    description='Rider performance metrics and rankings'
) }}

with rider_daily as (
    select * from {{ ref('int_rider_order_metrics') }}
),

riders as (
    select * from {{ ref('stg_riders') }}
),

-- Aggregate to rider level
rider_summary as (
    select
        rider_id,
        sum(orders_delivered) as total_orders_delivered,
        sum(late_deliveries) as total_late_deliveries,
        round(avg(avg_delivery_time_minutes), 1) as avg_delivery_time_minutes,
        count(distinct order_date) as active_days,
        round(sum(orders_delivered) * 1.0 / count(distinct order_date), 1) as orders_per_day
    from rider_daily
    group by rider_id
    having sum(orders_delivered) >= 10  -- Minimum for ranking
),

-- Calculate performance metrics
with_metrics as (
    select
        rs.*,
        r.city,
        r.shift_type,
        r.joining_date,

        -- On-time rate
        round(1.0 - (rs.total_late_deliveries * 1.0 / rs.total_orders_delivered), 4)
            as on_time_rate,

        -- Days since joining
        current_date - r.joining_date as tenure_days

    from rider_summary rs
    left join riders r on rs.rider_id = r.rider_id
),

-- Calculate city averages for efficiency score
city_averages as (
    select
        city,
        avg(orders_per_day) as city_avg_orders_per_day
    from with_metrics
    group by city
),

-- Final with efficiency score and rankings
final as (
    select
        wm.*,
        ca.city_avg_orders_per_day,

        -- Efficiency score: volume (vs city avg) × quality (on-time rate)
        round((wm.orders_per_day / ca.city_avg_orders_per_day) * wm.on_time_rate, 3)
            as efficiency_score,

        -- Rankings
        row_number() over (partition by wm.city order by
            (wm.orders_per_day / ca.city_avg_orders_per_day) * wm.on_time_rate desc
        ) as city_rank,

        row_number() over (order by
            (wm.orders_per_day / ca.city_avg_orders_per_day) * wm.on_time_rate desc
        ) as overall_rank,

        -- Performance tier
        case
            when wm.on_time_rate >= 0.95 and wm.orders_per_day >= ca.city_avg_orders_per_day
            then 'Star Performer'
            when wm.on_time_rate >= 0.90
            then 'Meets Expectations'
            when wm.on_time_rate >= 0.80
            then 'Needs Improvement'
            else 'At Risk'
        end as performance_tier

    from with_metrics wm
    left join city_averages ca on wm.city = ca.city
)

select * from final
order by efficiency_score desc
```

#### mart_weekly_trends.sql

```sql
-- dbt_project/models/marts/analytics/mart_weekly_trends.sql
-- Answers: Q5 - How do completed orders, cancellations, refund amounts trend week over week?

{{ config(
    materialized='table',
    description='Weekly trends for orders, cancellations, and refunds'
) }}

with orders as (
    select * from {{ ref('stg_orders') }}
),

refunds as (
    select * from {{ ref('stg_refunds') }}
),

-- Weekly order metrics
weekly_orders as (
    select
        date_trunc('week', order_date) as week_start,
        count(*) as total_orders,
        sum(case when status = 'delivered' then 1 else 0 end) as completed_orders,
        sum(case when status = 'cancelled' then 1 else 0 end) as cancelled_orders,
        round(sum(case when status = 'delivered' then order_value else 0 end), 2) as gmv,
        round(avg(case when status = 'delivered' then order_value end), 2) as avg_order_value
    from orders
    group by date_trunc('week', order_date)
),

-- Weekly refund metrics
weekly_refunds as (
    select
        date_trunc('week', refund_ts::date) as week_start,
        count(*) as refund_count,
        round(sum(refund_amount), 2) as refund_amount
    from refunds
    group by date_trunc('week', refund_ts::date)
),

-- Combine metrics
combined as (
    select
        wo.week_start,
        wo.total_orders,
        wo.completed_orders,
        wo.cancelled_orders,
        wo.gmv,
        wo.avg_order_value,
        coalesce(wr.refund_count, 0) as refund_count,
        coalesce(wr.refund_amount, 0) as refund_amount,

        -- Derived metrics
        round(wo.cancelled_orders * 100.0 / wo.total_orders, 2) as cancellation_rate_pct,
        round(coalesce(wr.refund_amount, 0) * 100.0 / nullif(wo.gmv, 0), 2) as refund_rate_pct

    from weekly_orders wo
    left join weekly_refunds wr on wo.week_start = wr.week_start
),

-- Add week-over-week changes
with_wow as (
    select
        *,
        lag(completed_orders) over (order by week_start) as prev_completed_orders,
        lag(cancelled_orders) over (order by week_start) as prev_cancelled_orders,
        lag(refund_amount) over (order by week_start) as prev_refund_amount,
        lag(gmv) over (order by week_start) as prev_gmv
    from combined
),

final as (
    select
        week_start,
        week_start + interval '6 days' as week_end,
        total_orders,
        completed_orders,
        cancelled_orders,
        gmv,
        avg_order_value,
        refund_count,
        refund_amount,
        cancellation_rate_pct,
        refund_rate_pct,

        -- WoW changes
        round((completed_orders - prev_completed_orders) * 100.0
              / nullif(prev_completed_orders, 0), 2) as completed_orders_wow_pct,
        round((cancelled_orders - prev_cancelled_orders) * 100.0
              / nullif(prev_cancelled_orders, 0), 2) as cancelled_orders_wow_pct,
        round((refund_amount - prev_refund_amount) * 100.0
              / nullif(prev_refund_amount, 0), 2) as refund_amount_wow_pct,
        round((gmv - prev_gmv) * 100.0 / nullif(prev_gmv, 0), 2) as gmv_wow_pct,

        row_number() over (order by week_start) as week_number

    from with_wow
)

select * from final
order by week_start
```

---

## 7. dbt Macros

```sql
-- dbt_project/macros/datediff_minutes.sql
{% macro datediff_minutes(start_col, end_col) %}
    (epoch({{ end_col }}) - epoch({{ start_col }})) / 60
{% endmacro %}
```

```sql
-- dbt_project/macros/time_slot_bucket.sql
{% macro time_slot_bucket(hour_col) %}
    case
        when {{ hour_col }} between 8 and 11 then 'morning'
        when {{ hour_col }} between 12 and 15 then 'lunch'
        when {{ hour_col }} between 16 and 19 then 'evening'
        when {{ hour_col }} between 20 and 23 then 'dinner'
        else 'late_night'
    end
{% endmacro %}
```

---

## 8. Testing Strategy

### 8.1 dbt Schema Tests

```yaml
# dbt_project/models/staging/_staging.yml
version: 2

sources:
  - name: silver
    description: "Silver layer tables"
    schema: main
    tables:
      - name: orders
      - name: order_items
      - name: delivery_events
      - name: restaurants
      - name: riders
      - name: refunds
      - name: support_tickets

models:
  - name: stg_orders
    columns:
      - name: order_id
        tests:
          - unique
          - not_null
      - name: order_value
        tests:
          - not_null

  - name: stg_restaurants
    columns:
      - name: restaurant_id
        tests:
          - unique
          - not_null
```

### 8.2 Custom Tests

```sql
-- dbt_project/tests/assert_refund_pct_sums_to_100.sql
-- Verify refund percentages sum to approximately 100%

with totals as (
    select sum(refund_count_pct) as total_pct
    from {{ ref('mart_refund_drivers') }}
)

select *
from totals
where abs(total_pct - 100) > 0.1  -- Allow 0.1% tolerance
```

---

## 9. Orchestration

### Makefile

```makefile
# Makefile
.PHONY: all generate spark load dbt test clean

PROJECT_DIR := /Users/patel/Code/Tarj

all: generate spark load dbt

generate:
	@echo "Generating synthetic data..."
	python -m src.generators.orchestrator

spark:
	@echo "Running Spark pipeline..."
	python -m src.spark_jobs.run_pipeline

load:
	@echo "Loading silver to DuckDB..."
	python -m src.loaders.silver_to_duckdb

dbt:
	@echo "Running dbt models..."
	cd dbt_project && dbt run
	cd dbt_project && dbt test

test:
	@echo "Running Python tests..."
	pytest tests/ -v

clean:
	rm -rf data/raw/*
	rm -rf data/bronze/*
	rm -rf data/silver/*
	rm -f data/warehouse/analytics.duckdb
	cd dbt_project && dbt clean

setup:
	pip install -e .
	cd dbt_project && dbt deps
```

---

## 10. Configuration Reference

### pyproject.toml

```toml
[project]
name = "tarj"
version = "0.1.0"
description = "Food Delivery Analytics Pipeline"
requires-python = ">=3.10"
dependencies = [
    "pyspark>=3.5.0",
    "duckdb>=0.10.0",
    "pandas>=2.0.0",
    "numpy>=1.24.0",
    "faker>=20.0.0",
    "dbt-duckdb>=1.7.0",
    "pytest>=7.0.0",
]

[build-system]
requires = ["setuptools>=61.0"]
build-backend = "setuptools.build_meta"

[tool.pytest.ini_options]
testpaths = ["tests"]
python_files = ["test_*.py"]
```

### .gitignore

```
# Data files
data/raw/
data/bronze/
data/silver/
data/warehouse/

# Python
__pycache__/
*.pyc
.venv/
*.egg-info/

# dbt
dbt_project/target/
dbt_project/dbt_packages/
dbt_project/logs/

# IDE
.idea/
.vscode/
*.swp

# OS
.DS_Store
```
