{{
    config(materialized='table')
}}

/*
    dim_date
    --------
    Standard date dimension covering 2020 through 2030.
*/

with date_spine as (

    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2020-01-01' as date)",
        end_date="cast('2030-12-31' as date)"
    ) }}

),

final as (

    select
        cast(date_day as date)                              as date_key,
        extract(year from date_day)                         as year,
        extract(quarter from date_day)                      as quarter,
        extract(month from date_day)                        as month,
        extract(week from date_day)                         as week_of_year,
        extract(dayofweek from date_day)                    as day_of_week,
        extract(dayofyear from date_day)                    as day_of_year,
        format_date('%B', date_day)                         as month_name,
        format_date('%b', date_day)                         as month_name_short,
        format_date('%A', date_day)                         as day_name,
        format_date('%a', date_day)                         as day_name_short,
        format_date('%Y-%m', date_day)                      as year_month,
        concat(
            cast(extract(year from date_day) as string),
            '-Q',
            cast(extract(quarter from date_day) as string)
        )                                                   as year_quarter,

        -- Fiscal calendar (assuming fiscal year = calendar year)
        extract(year from date_day)                         as fiscal_year,
        extract(quarter from date_day)                      as fiscal_quarter,

        -- Flags
        case
            when extract(dayofweek from date_day) in (1, 7) then true
            else false
        end                                                 as is_weekend,
        date_day = current_date()                           as is_today,

        -- Relative references
        date_diff(current_date(), date_day, day)            as days_ago,
        date_diff(current_date(), date_day, month)          as months_ago

    from date_spine

)

select * from final
