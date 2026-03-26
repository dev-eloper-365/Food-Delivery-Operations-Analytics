-- Verify refund percentages sum to approximately 100%

with totals as (
    select sum(refund_count_pct) as total_pct
    from {{ ref('mart_refund_drivers') }}
)

select *
from totals
where abs(total_pct - 100) > 0.1
