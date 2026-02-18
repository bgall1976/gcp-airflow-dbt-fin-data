/*
    Singular test: Validate that total recognized revenue in the fact table
    is within 10% of total booked (won) revenue in Salesforce for completed
    quarters. Large discrepancies indicate data gaps.
*/

with recognized as (

    select
        revenue_year,
        revenue_quarter,
        sum(revenue_amount)                                 as total_recognized
    from {{ ref('fct_revenue') }}
    where revenue_stage = 'recognized'
        and revenue_year >= 2023
    group by 1, 2

),

booked as (

    select
        revenue_year,
        revenue_quarter,
        sum(revenue_amount)                                 as total_booked
    from {{ ref('fct_revenue') }}
    where revenue_stage = 'booked'
        and revenue_year >= 2023
    group by 1, 2

),

comparison as (

    select
        coalesce(r.revenue_year, b.revenue_year)            as year,
        coalesce(r.revenue_quarter, b.revenue_quarter)      as quarter,
        coalesce(r.total_recognized, 0)                     as recognized,
        coalesce(b.total_booked, 0)                         as booked,
        abs(coalesce(r.total_recognized, 0) - coalesce(b.total_booked, 0))
                                                            as variance
    from recognized r
    full outer join booked b
        on r.revenue_year = b.revenue_year
        and r.revenue_quarter = b.revenue_quarter

)

-- Fail if any completed quarter has > 10% variance
select *
from comparison
where booked > 0
    and variance / booked > 0.10
    -- Only check completed quarters
    and concat(cast(year as string), cast(quarter as string))
        < concat(
            cast(extract(year from current_date()) as string),
            cast(extract(quarter from current_date()) as string)
        )
