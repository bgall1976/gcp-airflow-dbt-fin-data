{{
    config(
        materialized='table',
        partition_by={
            "field": "revenue_date",
            "data_type": "date",
            "granularity": "month"
        }
    )
}}

/*
    fct_revenue
    -----------
    Revenue fact table combining recognized revenue from accounting systems
    with pipeline/bookings data from Salesforce. Supports revenue analysis
    by customer, product, period, and source system.
*/

with invoiced_revenue as (

    select
        transaction_key,
        customer_key,
        transaction_date                                    as revenue_date,
        amount                                              as revenue_amount,
        currency_code,
        'recognized'                                        as revenue_stage,
        source_system,
        reference_number
    from {{ ref('fct_transactions') }}
    where transaction_category = 'accounts_receivable'
        and amount > 0

),

booked_revenue as (

    select
        {{ dbt_utils.generate_surrogate_key(['opportunity_id']) }}
                                                            as transaction_key,
        gc.customer_golden_id                               as customer_key,
        opp.close_date                                      as revenue_date,
        opp.amount                                          as revenue_amount,
        opp.currency_code,
        case
            when opp.is_won then 'booked'
            when opp.is_closed then 'lost'
            else 'pipeline'
        end                                                 as revenue_stage,
        'salesforce'                                        as source_system,
        opp.opportunity_name                                as reference_number
    from {{ ref('stg_salesforce__opportunities') }} opp
    left join {{ ref('int_golden_customers') }} gc
        on opp.sf_account_id = gc.salesforce_account_id

),

final as (

    select
        transaction_key,
        customer_key,
        revenue_date,
        revenue_amount,
        currency_code,
        revenue_stage,
        source_system,
        reference_number,

        -- Time dimensions for easy aggregation
        extract(year from revenue_date)                     as revenue_year,
        extract(quarter from revenue_date)                  as revenue_quarter,
        extract(month from revenue_date)                    as revenue_month,

        -- Running totals (will be computed at query time typically, but useful)
        current_timestamp()                                 as _loaded_at

    from invoiced_revenue

    union all

    select
        transaction_key,
        customer_key,
        revenue_date,
        revenue_amount,
        currency_code,
        revenue_stage,
        source_system,
        reference_number,
        extract(year from revenue_date),
        extract(quarter from revenue_date),
        extract(month from revenue_date),
        current_timestamp()
    from booked_revenue

)

select * from final
