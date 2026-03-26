{{ config(
    materialized='table',
    description='SLA breach analysis by city and time slot'
) }}

with order_timeline as (
    select * from {{ ref('int_order_delivery_timeline') }}
    where status = 'delivered'
),

city_timeslot_metrics as (
    select
        city,
        time_slot,
        order_date,
        count(*) as total_orders,
        sum(case when is_sla_breached then 1 else 0 end) as breached_orders,
        avg(total_time_minutes) as avg_delivery_time_minutes,
        avg(case when is_sla_breached then sla_breach_minutes else 0 end) as avg_breach_minutes
    from order_timeline
    group by city, time_slot, order_date
),

aggregated as (
    select
        city,
        time_slot,
        sum(total_orders) as total_orders,
        sum(breached_orders) as breached_orders,
        round(sum(breached_orders) * 100.0 / nullif(sum(total_orders), 0), 2) as breach_rate_pct,
        round(avg(avg_delivery_time_minutes), 1) as avg_delivery_time_minutes,
        round(avg(avg_breach_minutes), 1) as avg_breach_minutes_when_breached,
        count(distinct order_date) as days_with_data
    from city_timeslot_metrics
    group by city, time_slot
),

ranked as (
    select
        *,
        row_number() over (order by breach_rate_pct desc) as overall_breach_rank,
        row_number() over (partition by city order by breach_rate_pct desc) as city_breach_rank
    from aggregated
)

select * from ranked
order by breach_rate_pct desc
