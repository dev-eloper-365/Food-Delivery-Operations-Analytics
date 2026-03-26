{{ config(
    materialized='table',
    description='Refund driver analysis with percentages'
) }}

with order_refunds as (
    select * from {{ ref('int_order_refund_joined') }}
    where has_refund = true
),

totals as (
    select
        count(*) as total_refunds,
        sum(refund_amount) as total_refund_amount
    from order_refunds
),

by_category as (
    select
        refund_driver_category,
        count(*) as refund_count,
        sum(refund_amount) as refund_amount,
        round(avg(refund_amount), 2) as avg_refund_amount,
        min(refund_amount) as min_refund_amount,
        max(refund_amount) as max_refund_amount
    from order_refunds
    group by refund_driver_category
),

final as (
    select
        bc.refund_driver_category,
        bc.refund_count,
        round(bc.refund_count * 100.0 / t.total_refunds, 2) as refund_count_pct,
        round(bc.refund_amount, 2) as refund_amount,
        round(bc.refund_amount * 100.0 / t.total_refund_amount, 2) as refund_amount_pct,
        bc.avg_refund_amount,
        bc.min_refund_amount,
        bc.max_refund_amount,
        t.total_refunds,
        round(t.total_refund_amount, 2) as total_refund_amount
    from by_category bc
    cross join totals t
)

select * from final
order by refund_count_pct desc
