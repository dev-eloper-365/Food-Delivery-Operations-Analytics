{{ config(
    materialized='table',
    description='Weekly trends for orders, cancellations, and refunds'
) }}

with orders as (
    select * from {{ ref('stg_orders') }}
),

refunds as (
    select * from {{ ref('stg_refunds') }}
),

weekly_orders as (
    select
        date_trunc('week', order_date) as week_start,
        count(*) as total_orders,
        sum(case when status = 'delivered' then 1 else 0 end) as completed_orders,
        sum(case when status = 'cancelled' then 1 else 0 end) as cancelled_orders,
        round(sum(case when status = 'delivered' then order_value else 0 end), 2) as gmv,
        round(avg(case when status = 'delivered' then order_value end), 2) as avg_order_value
    from orders
    group by date_trunc('week', order_date)
),

weekly_refunds as (
    select
        date_trunc('week', refund_ts::date) as week_start,
        count(*) as refund_count,
        round(sum(refund_amount), 2) as refund_amount
    from refunds
    group by date_trunc('week', refund_ts::date)
),

combined as (
    select
        wo.week_start,
        wo.total_orders,
        wo.completed_orders,
        wo.cancelled_orders,
        wo.gmv,
        wo.avg_order_value,
        coalesce(wr.refund_count, 0) as refund_count,
        coalesce(wr.refund_amount, 0) as refund_amount,
        round(wo.cancelled_orders * 100.0 / wo.total_orders, 2) as cancellation_rate_pct,
        round(coalesce(wr.refund_amount, 0) * 100.0 / nullif(wo.gmv, 0), 2) as refund_rate_pct
    from weekly_orders wo
    left join weekly_refunds wr on wo.week_start = wr.week_start
),

with_wow as (
    select
        *,
        lag(completed_orders) over (order by week_start) as prev_completed_orders,
        lag(cancelled_orders) over (order by week_start) as prev_cancelled_orders,
        lag(refund_amount) over (order by week_start) as prev_refund_amount,
        lag(gmv) over (order by week_start) as prev_gmv
    from combined
),

final as (
    select
        week_start,
        week_start + interval '6 days' as week_end,
        total_orders,
        completed_orders,
        cancelled_orders,
        gmv,
        avg_order_value,
        refund_count,
        refund_amount,
        cancellation_rate_pct,
        refund_rate_pct,
        round((completed_orders - prev_completed_orders) * 100.0
              / nullif(prev_completed_orders, 0), 2) as completed_orders_wow_pct,
        round((cancelled_orders - prev_cancelled_orders) * 100.0
              / nullif(prev_cancelled_orders, 0), 2) as cancelled_orders_wow_pct,
        round((refund_amount - prev_refund_amount) * 100.0
              / nullif(prev_refund_amount, 0), 2) as refund_amount_wow_pct,
        round((gmv - prev_gmv) * 100.0 / nullif(prev_gmv, 0), 2) as gmv_wow_pct,
        row_number() over (order by week_start) as week_number
    from with_wow
)

select * from final
order by week_start
