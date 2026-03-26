{{ config(
    materialized='table',
    description='Rider performance metrics and rankings'
) }}

with rider_daily as (
    select * from {{ ref('int_rider_order_metrics') }}
),

riders as (
    select * from {{ ref('stg_riders') }}
),

rider_summary as (
    select
        rider_id,
        sum(orders_delivered) as total_orders_delivered,
        sum(late_deliveries) as total_late_deliveries,
        round(avg(avg_delivery_time_minutes), 1) as avg_delivery_time_minutes,
        count(distinct order_date) as active_days,
        round(sum(orders_delivered) * 1.0 / count(distinct order_date), 1) as orders_per_day
    from rider_daily
    group by rider_id
    having sum(orders_delivered) >= 10
),

with_metrics as (
    select
        rs.*,
        r.city,
        r.shift_type,
        r.joining_date,
        round(1.0 - (rs.total_late_deliveries * 1.0 / rs.total_orders_delivered), 4) as on_time_rate,
        current_date - r.joining_date as tenure_days
    from rider_summary rs
    left join riders r on rs.rider_id = r.rider_id
),

city_averages as (
    select
        city,
        avg(orders_per_day) as city_avg_orders_per_day
    from with_metrics
    group by city
),

final as (
    select
        wm.*,
        ca.city_avg_orders_per_day,
        round((wm.orders_per_day / ca.city_avg_orders_per_day) * wm.on_time_rate, 3) as efficiency_score,
        row_number() over (partition by wm.city order by
            (wm.orders_per_day / ca.city_avg_orders_per_day) * wm.on_time_rate desc
        ) as city_rank,
        row_number() over (order by
            (wm.orders_per_day / ca.city_avg_orders_per_day) * wm.on_time_rate desc
        ) as overall_rank,
        case
            when wm.on_time_rate >= 0.95 and wm.orders_per_day >= ca.city_avg_orders_per_day
            then 'Star Performer'
            when wm.on_time_rate >= 0.90
            then 'Meets Expectations'
            when wm.on_time_rate >= 0.80
            then 'Needs Improvement'
            else 'At Risk'
        end as performance_tier
    from with_metrics wm
    left join city_averages ca on wm.city = ca.city
)

select * from final
order by efficiency_score desc
