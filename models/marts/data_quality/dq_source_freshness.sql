{{
    config(materialized='table')
}}

/*
    dq_source_freshness
    -------------------
    Monitors the freshness of data from each source system by tracking the
    most recent record timestamps. Flags sources that have gone stale.
*/

with freshness_checks as (

    select
        source_system,
        max(_unified_at)                                    as last_record_at,
        max(transaction_date)                               as most_recent_transaction_date,
        count(*)                                            as total_records,
        count(case
            when transaction_date >= date_sub(current_date(), interval 7 day)
            then 1 end)                                     as records_last_7_days,
        count(case
            when transaction_date >= date_sub(current_date(), interval 1 day)
            then 1 end)                                     as records_last_24_hours
    from {{ ref('int_unified_transactions') }}
    group by 1

),

with_sla as (

    select
        source_system,
        last_record_at,
        most_recent_transaction_date,
        total_records,
        records_last_7_days,
        records_last_24_hours,

        timestamp_diff(current_timestamp(), last_record_at, hour)
                                                            as hours_since_last_load,

        -- SLA definitions per source
        case source_system
            when 'stripe' then 6
            when 'plaid' then 24
            when 'quickbooks' then 12
            when 'netsuite' then 12
            when 'salesforce' then 12
        end                                                 as sla_hours,

        case
            when timestamp_diff(current_timestamp(), last_record_at, hour) <=
                case source_system
                    when 'stripe' then 6
                    when 'plaid' then 24
                    else 12
                end
            then 'healthy'
            when timestamp_diff(current_timestamp(), last_record_at, hour) <=
                case source_system
                    when 'stripe' then 12
                    when 'plaid' then 48
                    else 24
                end
            then 'warning'
            else 'critical'
        end                                                 as freshness_status,

        current_timestamp()                                 as _checked_at

    from freshness_checks

)

select * from with_sla
order by
    case freshness_status
        when 'critical' then 1
        when 'warning' then 2
        else 3
    end
