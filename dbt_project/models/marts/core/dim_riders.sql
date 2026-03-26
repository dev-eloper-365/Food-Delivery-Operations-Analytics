{{ config(materialized='table') }}

select
    rider_id,
    city,
    shift_type,
    joining_date
from {{ ref('stg_riders') }}
