{{ config(materialized='view') }}

with source as (
    select * from {{ source('silver', 'riders') }}
)

select
    rider_id,
    city,
    shift_type,
    joining_date,
    _ingested_at
from source
