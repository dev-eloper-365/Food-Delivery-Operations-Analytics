{{ config(materialized='view') }}

with source as (
    select * from {{ source('silver', 'support_tickets') }}
)

select
    ticket_id,
    order_id,
    ticket_type,
    created_ts,
    resolution_status,
    _ingested_at
from source
