{{ config(materialized='view') }}

with source as (
    select * from {{ source('silver', 'orders') }}
),

staged as (
    select
        order_id,
        customer_id,
        restaurant_id,
        city,
        status,
        payment_mode,
        time_slot,
        order_ts,
        promised_delivery_ts,
        order_date,
        order_hour,
        day_of_week,
        is_weekend,
        order_value,
        _dq_has_null_customer,
        _dq_invalid_order_value,
        _ingested_at
    from source
    where _dq_invalid_order_value = false
)

select * from staged
