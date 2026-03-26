{{ config(materialized='view') }}

with source as (
    select * from {{ source('silver', 'refunds') }}
)

select
    refund_id,
    order_id,
    refund_ts,
    refund_reason,
    refund_amount,
    _ingested_at
from source
