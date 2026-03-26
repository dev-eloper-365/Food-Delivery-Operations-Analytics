{{ config(materialized='table') }}

with date_spine as (
    select
        unnest(generate_series(
            date '2026-01-15',
            date '2026-03-22',
            interval '1 day'
        ))::date as date_day
),

final as (
    select
        date_day,
        extract(year from date_day) as year,
        extract(month from date_day) as month,
        extract(day from date_day) as day_of_month,
        extract(dow from date_day) as day_of_week,
        extract(week from date_day) as iso_week,
        case when extract(dow from date_day) in (0, 6) then true else false end as is_weekend,
        case
            when extract(dow from date_day) = 0 then 'Sunday'
            when extract(dow from date_day) = 1 then 'Monday'
            when extract(dow from date_day) = 2 then 'Tuesday'
            when extract(dow from date_day) = 3 then 'Wednesday'
            when extract(dow from date_day) = 4 then 'Thursday'
            when extract(dow from date_day) = 5 then 'Friday'
            when extract(dow from date_day) = 6 then 'Saturday'
        end as day_name,
        date_trunc('week', date_day)::date as week_start,
        date_trunc('month', date_day)::date as month_start
    from date_spine
)

select * from final
