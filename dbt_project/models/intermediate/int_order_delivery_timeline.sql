{{ config(materialized='ephemeral') }}

with orders as (
    select * from {{ ref('stg_orders') }}
),

delivery_events as (
    select * from {{ ref('stg_delivery_events') }}
),

event_pivot as (
    select
        order_id,
        max(case when event_type = 'order_confirmed' then event_ts end) as confirmed_ts,
        max(case when event_type = 'restaurant_accepted' then event_ts end) as accepted_ts,
        max(case when event_type = 'food_ready' then event_ts end) as food_ready_ts,
        max(case when event_type = 'rider_picked_up' then event_ts end) as picked_up_ts,
        max(case when event_type = 'delivered' then event_ts end) as delivered_ts,
        max(rider_id) as rider_id
    from delivery_events
    group by order_id
),

final as (
    select
        o.order_id,
        o.customer_id,
        o.restaurant_id,
        o.city,
        o.order_ts,
        o.promised_delivery_ts,
        o.status,
        o.order_value,
        o.time_slot,
        o.order_date,
        o.is_weekend,

        ep.confirmed_ts,
        ep.accepted_ts,
        ep.food_ready_ts,
        ep.picked_up_ts,
        ep.delivered_ts,
        ep.rider_id,

        {{ datediff_minutes('ep.accepted_ts', 'ep.food_ready_ts') }} as prep_time_minutes,
        {{ datediff_minutes('ep.picked_up_ts', 'ep.delivered_ts') }} as delivery_time_minutes,
        {{ datediff_minutes('o.order_ts', 'ep.delivered_ts') }} as total_time_minutes,

        case
            when ep.delivered_ts is null then null
            when ep.delivered_ts > o.promised_delivery_ts then true
            else false
        end as is_sla_breached,

        case
            when ep.delivered_ts is null then null
            else {{ datediff_minutes('o.promised_delivery_ts', 'ep.delivered_ts') }}
        end as sla_breach_minutes

    from orders o
    left join event_pivot ep on o.order_id = ep.order_id
)

select * from final
