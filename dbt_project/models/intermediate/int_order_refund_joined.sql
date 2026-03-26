{{ config(materialized='ephemeral') }}

with orders as (
    select * from {{ ref('stg_orders') }}
),

refunds as (
    select * from {{ ref('stg_refunds') }}
),

final as (
    select
        o.order_id,
        o.customer_id,
        o.restaurant_id,
        o.city,
        o.status,
        o.order_value,
        o.order_date,

        r.refund_id,
        r.refund_ts,
        r.refund_reason,
        r.refund_amount,

        case when r.refund_id is not null then true else false end as has_refund,

        case
            when r.refund_reason = 'Delay' then 'Delay'
            when r.refund_reason in ('Missing_Items', 'Wrong_Order') then 'Missing Items'
            when r.refund_reason = 'Cancellation' then 'Cancellation'
            else 'Other'
        end as refund_driver_category

    from orders o
    left join refunds r on o.order_id = r.order_id
)

select * from final
