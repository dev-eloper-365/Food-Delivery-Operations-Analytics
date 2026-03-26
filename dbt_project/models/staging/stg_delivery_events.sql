{{ config(materialized='view') }}

with source as (
    select * from {{ source('silver', 'delivery_events') }}
),

staged as (
    select
        order_id,
        rider_id,
        event_type,
        event_ts,
        latitude,
        longitude,
        _dq_orphan_order,
        _ingested_at
    from source
    where _dq_orphan_order = false
)

select * from staged
