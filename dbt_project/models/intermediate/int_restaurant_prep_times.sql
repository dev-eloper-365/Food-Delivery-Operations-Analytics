{{ config(materialized='ephemeral') }}

with timeline as (
    select * from {{ ref('int_order_delivery_timeline') }}
    where status = 'delivered'
      and prep_time_minutes is not null
      and prep_time_minutes > 0
),

restaurant_metrics as (
    select
        restaurant_id,
        order_date,
        count(*) as total_orders,
        round(avg(prep_time_minutes), 1) as avg_prep_time_minutes,
        sum(case when prep_time_minutes > 20 then 1 else 0 end) as delayed_orders
    from timeline
    group by restaurant_id, order_date
)

select * from restaurant_metrics
