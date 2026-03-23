# Food Delivery Operations Analytics - Solution Document

## 1. Executive Summary

### Problem Statement

A food delivery company operating across multiple cities lacks reliable visibility into:
- **Order Flow**: Volume, completion rates, cancellation patterns
- **Delivery Delays**: SLA breaches, bottleneck identification
- **Refund Leakage**: Root causes driving refunds
- **Rider Performance**: Efficiency and reliability metrics

The business team does not trust existing metrics due to inconsistent definitions, improper joins between systems, and lack of standardized data models.

### Solution Overview

This project delivers a **local batch analytics pipeline** that:

1. **Ingests** raw operational data from 7 source systems
2. **Validates** data quality with explicit defect handling
3. **Transforms** raw data into clean warehouse tables
4. **Publishes** decision-ready marts for daily business use

### Key Outcomes

| Outcome | Deliverable |
|---------|-------------|
| **Trusted Metrics** | 5 analytics marts with documented definitions |
| **Data Quality** | DQ flags at every layer, explicit handling of nulls/duplicates |
| **Reproducibility** | Make-based orchestration, deterministic data generation |
| **Self-Service Analytics** | DuckDB warehouse queryable via SQL |

---

## 2. Business Context

### Stakeholder Needs

| Stakeholder | Primary Questions | Data Needs |
|-------------|-------------------|------------|
| **Operations Lead** | Which cities/time slots have SLA issues? | SLA breach rates by city, time slot |
| **Restaurant Partnerships** | Which restaurants cause delays? | Prep time analysis, delay attribution |
| **Finance** | How much are we losing to refunds? | Refund amounts by reason, trends |
| **Rider Operations** | Who are top/bottom performers? | Rider efficiency scores, on-time rates |
| **Strategy** | How is the business trending? | Weekly order/revenue/cancellation trends |

### Current Pain Points

1. **Inconsistent Definitions**: "On-time delivery" calculated differently across reports
2. **Improper Joins**: Orders orphaned from delivery events, refunds missing context
3. **No Standardization**: Each team maintains separate spreadsheets
4. **Late-Arriving Data**: Events from delivery tracking arrive hours late, causing report discrepancies
5. **Duplicate Records**: Order management system occasionally sends duplicates

### Success Criteria

- [ ] All 5 business questions answerable from marts
- [ ] Metric definitions documented and agreed upon
- [ ] Data quality issues explicitly flagged (not silently dropped)
- [ ] Pipeline reproducible with `make all`
- [ ] 60+ days of historical data available

---

## 3. Data Domain Model

### Entity Relationships

```
┌─────────────────┐       ┌─────────────────┐       ┌─────────────────┐
│   RESTAURANTS   │       │     ORDERS      │       │     RIDERS      │
├─────────────────┤       ├─────────────────┤       ├─────────────────┤
│ restaurant_id   │◄──────┤ restaurant_id   │       │ rider_id        │
│ city            │       │ order_id (PK)   │       │ city            │
│ cuisine_type    │       │ customer_id     │       │ shift_type      │
│ rating_band     │       │ city            │       │ joining_date    │
│ onboarding_date │       │ order_ts        │       └────────┬────────┘
└─────────────────┘       │ promised_ts     │                │
                          │ status          │                │
                          │ order_value     │                │
                          │ payment_mode    │                │
                          └───────┬─────────┘                │
                                  │                          │
              ┌───────────────────┼──────────────────────────┤
              │                   │                          │
              ▼                   ▼                          ▼
┌─────────────────┐    ┌─────────────────┐       ┌─────────────────┐
│   ORDER_ITEMS   │    │ DELIVERY_EVENTS │       │    REFUNDS      │
├─────────────────┤    ├─────────────────┤       ├─────────────────┤
│ order_id (FK)   │    │ order_id (FK)   │       │ refund_id (PK)  │
│ item_id         │    │ rider_id (FK)   │       │ order_id (FK)   │
│ quantity        │    │ event_type      │       │ refund_ts       │
│ item_price      │    │ event_ts        │       │ refund_reason   │
│ cuisine_type    │    │ latitude        │       │ refund_amount   │
└─────────────────┘    │ longitude       │       └─────────────────┘
                       └─────────────────┘
                                                  ┌─────────────────┐
                                                  │ SUPPORT_TICKETS │
                                                  ├─────────────────┤
                                                  │ ticket_id (PK)  │
                                                  │ order_id (FK)   │
                                                  │ ticket_type     │
                                                  │ created_ts      │
                                                  │ resolution_status│
                                                  └─────────────────┘
```

### Key Business Concepts

| Concept | Definition |
|---------|------------|
| **Order** | A customer request to purchase and deliver food from a restaurant |
| **SLA** | Service Level Agreement - the promised delivery time given to customer |
| **Prep Time** | Duration from restaurant accepting order to food being ready |
| **Delivery Time** | Duration from rider pickup to customer delivery |
| **Time Slot** | Bucketed periods: morning (8-11), lunch (12-15), evening (16-19), dinner (20-23) |

### Data Lineage

```
[Raw Files]                [Bronze Layer]            [Silver Layer]           [Marts]
    │                           │                         │                      │
orders.csv ────────────► bronze/orders ──────────► silver/orders ─────┐
order_items.csv ───────► bronze/order_items ─────► silver/order_items │
delivery_events.json ──► bronze/delivery_events ─► silver/delivery ───┼──► mart_sla_breach
restaurants.csv ───────► bronze/restaurants ─────► silver/restaurants │    mart_restaurant_delays
riders.csv ────────────► bronze/riders ──────────► silver/riders ─────┼──► mart_rider_performance
refunds.csv ───────────► bronze/refunds ─────────► silver/refunds ────┼──► mart_refund_drivers
support_tickets.csv ───► bronze/support_tickets ─► silver/tickets ────┘    mart_weekly_trends
```

---

## 4. Metric Definitions

### 4.1 SLA Breach Rate

**Business Question**: Which cities and time slots have the highest delivery SLA breach rate?

**Definition**:
```
SLA Breach Rate (%) = (Orders Delivered After Promised Time / Total Delivered Orders) × 100
```

**Calculation Logic**:
```sql
is_sla_breached = CASE
    WHEN delivered_ts IS NULL THEN NULL  -- Exclude non-delivered
    WHEN delivered_ts > promised_delivery_ts THEN TRUE
    ELSE FALSE
END

breach_rate_pct = SUM(is_sla_breached) / COUNT(*) * 100
```

**Edge Cases**:
- Orders without `delivered_ts` (cancelled, in-progress) are excluded from denominator
- Orders with NULL `promised_delivery_ts` flagged as data quality issue
- Breach measured to the minute (no grace period)

**Assumptions**:
- Clock synchronization between order and delivery systems assumed
- No distinction between 1-minute breach and 30-minute breach in rate

---

### 4.2 Prep Time Delay

**Business Question**: Which restaurants are causing prep-time delays?

**Definition**:
```
Prep Time (minutes) = food_ready_ts - restaurant_accepted_ts
Prep Delay = Prep Time > 20 minutes
```

**Calculation Logic**:
```sql
prep_time_minutes = DATEDIFF('minute', restaurant_accepted_ts, food_ready_ts)

delay_rate_pct = SUM(CASE WHEN prep_time_minutes > 20 THEN 1 ELSE 0 END)
                 / COUNT(*) * 100
```

**Risk Categories**:
| Category | Criteria |
|----------|----------|
| High Risk | avg_prep_time > 25 min AND delay_rate > 30% |
| Medium Risk | avg_prep_time > 20 min AND delay_rate > 20% |
| Low Risk | All others |

**Edge Cases**:
- Missing `food_ready` event → excluded from prep time analysis
- `restaurant_accepted_ts` comes AFTER `food_ready_ts` → flagged as invalid sequence

**Assumptions**:
- 20-minute threshold is configurable business rule
- New restaurants (< 30 days) may have higher variance, noted but not excluded

---

### 4.3 Refund Driver Categories

**Business Question**: What percentage of refunds are driven by Delay, Missing Items, Cancellations?

**Definition**:
```
Refund drivers are categorized into three buckets based on refund_reason:
- Delay: Customer received order late
- Missing Items: Items were missing or wrong
- Cancellation: Order was cancelled before delivery
```

**Category Mapping**:
| Raw Reason | Category |
|------------|----------|
| `Delay` | Delay |
| `Missing_Items` | Missing Items |
| `Wrong_Order` | Missing Items |
| `Cancellation` | Cancellation |
| `Quality_Issue` | Other |
| NULL | Other |

**Calculation Logic**:
```sql
refund_driver_category = CASE
    WHEN refund_reason = 'Delay' THEN 'Delay'
    WHEN refund_reason IN ('Missing_Items', 'Wrong_Order') THEN 'Missing Items'
    WHEN refund_reason = 'Cancellation' THEN 'Cancellation'
    ELSE 'Other'
END

refund_pct = COUNT(category) / COUNT(*) * 100
```

**Validation**:
- Sum of all category percentages must equal 100%
- Custom dbt test enforces this constraint

---

### 4.4 Rider Performance Score

**Business Question**: Which riders consistently handle more orders without increasing late deliveries?

**Definition**:
```
On-Time Rate = 1 - (Late Deliveries / Total Deliveries)
Efficiency Score = Orders Per Shift Hour × On-Time Rate
```

**Calculation Logic**:
```sql
on_time_rate = 1.0 - (late_deliveries / NULLIF(total_deliveries, 0))

-- Efficiency combines volume and quality
efficiency_score = (orders_per_day / avg_orders_per_day) * on_time_rate
```

**Ranking**:
- Riders ranked by efficiency_score within their city
- Minimum 10 deliveries required for ranking inclusion

**Edge Cases**:
- Riders with 0 deliveries in period → excluded
- Riders spanning multiple cities → ranked in primary city only

---

### 4.5 Weekly Trends

**Business Question**: How do completed orders, cancellations, refund amounts trend week over week?

**Definition**:
```
Week = ISO week number (Monday start)
WoW Change = (This Week - Last Week) / Last Week × 100
```

**Metrics Tracked**:
| Metric | Calculation |
|--------|-------------|
| Completed Orders | COUNT WHERE status = 'delivered' |
| Cancellations | COUNT WHERE status = 'cancelled' |
| Total GMV | SUM(order_value) WHERE status = 'delivered' |
| Refund Amount | SUM(refund_amount) |
| Refund Rate | Refund Amount / Total GMV × 100 |

**Week Boundaries**:
- ISO week: Monday 00:00:00 to Sunday 23:59:59
- Partial weeks (start/end of data range) included but flagged

---

## 5. Design Decisions & Trade-offs

### 5.1 Why DuckDB over Postgres/SQLite?

| Factor | DuckDB | Postgres | SQLite |
|--------|--------|----------|--------|
| **Setup** | Zero config, embedded | Requires server | Simple but limited |
| **Analytics** | Columnar, optimized for OLAP | OLTP-focused | General purpose |
| **dbt Support** | Native dbt-duckdb | dbt-postgres | dbt-sqlite (limited) |
| **Parquet Support** | Native, excellent | Via extensions | None |

**Decision**: DuckDB provides analytical performance with zero infrastructure, ideal for local development.

### 5.2 Why Medallion Architecture (Bronze/Silver)?

```
Bronze: Raw data preservation, audit trail, reprocessing capability
Silver: Clean data, consistent types, deduplication, derived columns
Marts:  Business logic, aggregations, ready for consumption
```

**Trade-off**: Additional storage and processing vs. debugging capability and data quality visibility.

**Decision**: Explicit layers make data quality issues visible rather than silently fixing them.

### 5.3 Why dbt for Modeling Layer?

- **Documentation**: Built-in docs generation
- **Testing**: Native data quality tests
- **Lineage**: Automatic DAG visualization
- **Modularity**: SQL-based, version controlled
- **Ecosystem**: dbt_utils, data contracts

**Alternative Considered**: Pure Spark SQL → Rejected due to limited testing/documentation features.

### 5.4 Handling Late-Arriving Events

**Problem**: 3% of delivery events arrive 1-24 hours after actual occurrence.

**Approach**:
1. **Bronze**: All events ingested with `_ingested_at` timestamp
2. **Silver**: Events ordered by `event_ts` (actual time), not ingestion time
3. **Reprocessing**: Daily batch reprocesses last 48 hours to catch late arrivals

**Trade-off**: Storage cost of retaining multiple versions vs. accuracy.

### 5.5 Handling Orphan Records

**Problem**: 1% of delivery events reference non-existent order_ids.

**Approach**:
1. **Flag, don't drop**: `_dq_orphan_order = TRUE`
2. **Excluded from marts**: Filtered in staging layer
3. **Monitored**: DQ dashboard tracks orphan rate over time

**Rationale**: Dropping silently hides upstream issues; flagging enables investigation.

---

## 6. Data Quality Approach

### 6.1 Injected Defects (Why?)

| Defect Type | Rate | Rationale |
|-------------|------|-----------|
| Nulls | 2% | Test null handling in transforms |
| Duplicates | 0.5% | Validate deduplication logic |
| Late Events | 3% | Test event ordering logic |
| Orphan Keys | 1% | Test referential integrity checks |
| SLA Breaches | 12% | Realistic operational scenario |

### 6.2 DQ Checks by Layer

| Layer | Checks |
|-------|--------|
| **Bronze** | Schema validation, corrupt record capture |
| **Silver** | Deduplication, null handling, orphan detection, valid ranges |
| **dbt Staging** | Unique PKs, not-null on critical fields |
| **dbt Marts** | Business rule validation (e.g., percentages sum to 100) |

### 6.3 Monitoring Strategy

**DQ Flags**: Every silver table includes `_dq_*` columns:
- `_dq_has_null_customer`
- `_dq_invalid_order_value`
- `_dq_orphan_order`
- `_dq_invalid_sequence`

**Periodic Review**:
```sql
SELECT
    COUNT(*) as total_records,
    SUM(_dq_has_null_customer::int) as null_customer_count,
    SUM(_dq_invalid_order_value::int) as invalid_value_count
FROM silver.orders
```

---

## 7. Assumptions & Limitations

### Assumptions

1. **Batch Processing**: Data refreshed daily, no real-time requirements
2. **Single Timezone**: All timestamps assumed to be in IST
3. **Static Restaurants/Riders**: Master data doesn't change retroactively
4. **Synthetic Data**: Generated data approximates but doesn't replicate real patterns

### Limitations

1. **No Streaming**: Late-arriving events require batch reprocessing
2. **Local Only**: Not designed for distributed execution
3. **No Incremental**: Full refresh on each run (could optimize later)
4. **Limited History**: 60 days may not capture seasonal patterns

### Future Enhancements

- [ ] Incremental processing for large datasets
- [ ] Real-time streaming with Kafka/Spark Structured Streaming
- [ ] ML-based anomaly detection for DQ
- [ ] Dashboard layer (Metabase/Superset integration)
