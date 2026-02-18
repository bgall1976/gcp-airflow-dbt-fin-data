{{
    config(materialized='table')
}}

/*
    dq_transaction_anomalies
    ------------------------
    Detects anomalies in daily transaction volumes and amounts using a
    rolling 30-day average and standard deviation. Flags days that deviate
    more than 2 standard deviations from the mean.
*/

with daily_stats as (

    select
        transaction_date,
        source_system,
        count(*)                                            as daily_count,
        sum(amount)                                         as daily_total,
        avg(amount)                                         as daily_avg_amount,
        max(amount)                                         as daily_max_amount
    from {{ ref('int_unified_transactions') }}
    where transaction_date >= date_sub(current_date(), interval 120 day)
    group by 1, 2

),

rolling_stats as (

    select
        transaction_date,
        source_system,
        daily_count,
        daily_total,
        daily_avg_amount,
        daily_max_amount,

        avg(daily_count) over (
            partition by source_system
            order by transaction_date
            rows between 30 preceding and 1 preceding
        )                                                   as rolling_avg_count,

        stddev(daily_count) over (
            partition by source_system
            order by transaction_date
            rows between 30 preceding and 1 preceding
        )                                                   as rolling_stddev_count,

        avg(daily_total) over (
            partition by source_system
            order by transaction_date
            rows between 30 preceding and 1 preceding
        )                                                   as rolling_avg_total,

        stddev(daily_total) over (
            partition by source_system
            order by transaction_date
            rows between 30 preceding and 1 preceding
        )                                                   as rolling_stddev_total

    from daily_stats

),

anomalies as (

    select
        transaction_date,
        source_system,
        daily_count,
        daily_total,
        rolling_avg_count,
        rolling_stddev_count,
        rolling_avg_total,
        rolling_stddev_total,

        -- Z-scores
        case
            when rolling_stddev_count > 0
            then round((daily_count - rolling_avg_count) / rolling_stddev_count, 2)
            else 0
        end                                                 as count_z_score,

        case
            when rolling_stddev_total > 0
            then round((daily_total - rolling_avg_total) / rolling_stddev_total, 2)
            else 0
        end                                                 as amount_z_score,

        -- Anomaly flags
        case
            when rolling_stddev_count > 0
                and abs(daily_count - rolling_avg_count) > 2 * rolling_stddev_count
            then true
            else false
        end                                                 as is_count_anomaly,

        case
            when rolling_stddev_total > 0
                and abs(daily_total - rolling_avg_total) > 2 * rolling_stddev_total
            then true
            else false
        end                                                 as is_amount_anomaly,

        current_timestamp()                                 as _checked_at

    from rolling_stats
    where transaction_date >= date_sub(current_date(), interval 90 day)

)

select * from anomalies
where is_count_anomaly or is_amount_anomaly
order by transaction_date desc
