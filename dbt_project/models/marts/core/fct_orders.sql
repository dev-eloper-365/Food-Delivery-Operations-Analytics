{{ config(materialized='table') }}

select
    order_id,
    customer_id,
    restaurant_id,
    rider_id,
    city,
    status,
    order_value,
    time_slot,
    order_ts,
    promised_delivery_ts,
    order_date,
    is_weekend,
    confirmed_ts,
    accepted_ts,
    food_ready_ts,
    picked_up_ts,
    delivered_ts,
    prep_time_minutes,
    delivery_time_minutes,
    total_time_minutes,
    is_sla_breached,
    sla_breach_minutes
from {{ ref('int_order_delivery_timeline') }}
