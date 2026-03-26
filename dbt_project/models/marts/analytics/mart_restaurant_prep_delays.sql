{{ config(
    materialized='table',
    description='Restaurant prep time analysis and risk categorization'
) }}

with order_timeline as (
    select * from {{ ref('int_order_delivery_timeline') }}
    where status = 'delivered'
      and prep_time_minutes is not null
      and prep_time_minutes > 0
),

restaurants as (
    select * from {{ ref('stg_restaurants') }}
),

restaurant_metrics as (
    select
        ot.restaurant_id,
        count(*) as total_orders,
        round(avg(ot.prep_time_minutes), 1) as avg_prep_time_minutes,
        round(median(ot.prep_time_minutes), 1) as median_prep_time_minutes,
        round(percentile_cont(0.95) within group (order by ot.prep_time_minutes), 1) as p95_prep_time_minutes,
        min(ot.prep_time_minutes) as min_prep_time_minutes,
        max(ot.prep_time_minutes) as max_prep_time_minutes,
        sum(case when ot.prep_time_minutes > 20 then 1 else 0 end) as delayed_orders,
        sum(case when ot.is_sla_breached and ot.prep_time_minutes > 20 then 1 else 0 end) as sla_breaches_from_prep
    from order_timeline ot
    group by ot.restaurant_id
    having count(*) >= 10
),

final as (
    select
        rm.restaurant_id,
        r.city,
        r.cuisine_type,
        r.rating_band,
        r.onboarding_date,
        rm.total_orders,
        rm.avg_prep_time_minutes,
        rm.median_prep_time_minutes,
        rm.p95_prep_time_minutes,
        rm.min_prep_time_minutes,
        rm.max_prep_time_minutes,
        rm.delayed_orders,
        round(rm.delayed_orders * 100.0 / rm.total_orders, 2) as delay_rate_pct,
        rm.sla_breaches_from_prep,
        case
            when rm.avg_prep_time_minutes > 25
                 and (rm.delayed_orders * 100.0 / rm.total_orders) > 30
            then 'High Risk'
            when rm.avg_prep_time_minutes > 20
                 and (rm.delayed_orders * 100.0 / rm.total_orders) > 20
            then 'Medium Risk'
            else 'Low Risk'
        end as risk_category,
        current_date - r.onboarding_date as days_since_onboarding
    from restaurant_metrics rm
    left join restaurants r on rm.restaurant_id = r.restaurant_id
)

select * from final
order by delay_rate_pct desc
