{{ config(materialized='ephemeral') }}

with timeline as (
    select * from {{ ref('int_order_delivery_timeline') }}
    where status = 'delivered'
      and rider_id is not null
),

daily_metrics as (
    select
        rider_id,
        order_date,
        count(*) as orders_delivered,
        sum(case when is_sla_breached then 1 else 0 end) as late_deliveries,
        round(avg(delivery_time_minutes), 1) as avg_delivery_time_minutes
    from timeline
    group by rider_id, order_date
)

select * from daily_metrics
