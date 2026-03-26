{{ config(materialized='table') }}

with restaurants as (
    select * from {{ ref('stg_restaurants') }}
),

prep_stats as (
    select
        restaurant_id,
        sum(total_orders) as total_orders,
        round(avg(avg_prep_time_minutes), 1) as avg_prep_time_minutes,
        sum(delayed_orders) as total_delayed_orders
    from {{ ref('int_restaurant_prep_times') }}
    group by restaurant_id
)

select
    r.restaurant_id,
    r.city,
    r.cuisine_type,
    r.rating_band,
    r.onboarding_date,
    coalesce(ps.total_orders, 0) as total_orders,
    ps.avg_prep_time_minutes,
    coalesce(ps.total_delayed_orders, 0) as total_delayed_orders
from restaurants r
left join prep_stats ps on r.restaurant_id = ps.restaurant_id
