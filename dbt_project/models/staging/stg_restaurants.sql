{{ config(materialized='view') }}

with source as (
    select * from {{ source('silver', 'restaurants') }}
)

select
    restaurant_id,
    city,
    cuisine_type,
    rating_band,
    onboarding_date,
    _ingested_at
from source
