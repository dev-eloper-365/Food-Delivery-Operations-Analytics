{{ config(materialized='view') }}

with source as (
    select * from {{ source('silver', 'order_items') }}
)

select
    order_id,
    item_id,
    quantity,
    item_price,
    cuisine_type,
    _ingested_at
from source
