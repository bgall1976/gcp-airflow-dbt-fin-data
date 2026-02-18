{{
    config(materialized='table')
}}

/*
    dim_customers
    -------------
    Conformed customer dimension built from the golden customer record.
    Enriched with lifetime transaction metrics.
*/

with golden as (

    select * from {{ ref('int_golden_customers') }}

),

transaction_metrics as (

    select
        customer_key,
        count(distinct transaction_key)                     as lifetime_transaction_count,
        sum(amount)                                         as lifetime_transaction_value,
        sum(revenue_amount)                                 as lifetime_revenue,
        min(transaction_date)                               as first_transaction_date,
        max(transaction_date)                               as last_transaction_date,
        count(distinct source_system)                       as source_system_count
    from {{ ref('fct_transactions') }}
    where customer_key is not null
    group by 1

),

final as (

    select
        g.customer_golden_id                                as customer_key,

        -- Identifiers
        g.stripe_customer_id,
        g.quickbooks_customer_id,
        g.netsuite_customer_id,
        g.salesforce_account_id,

        -- Attributes
        g.company_name,
        g.first_name,
        g.last_name,
        g.email,
        g.phone,
        g.website,

        -- Address
        g.billing_address,
        g.billing_city,
        g.billing_state,
        g.billing_postal_code,
        g.billing_country,

        -- Business
        g.industry,
        g.customer_segment,
        g.annual_revenue,
        g.employee_count,

        -- Flags
        g.is_active_accounting,
        g.is_delinquent,
        g.is_multi_source_match,
        g.primary_source,

        -- Transaction metrics
        coalesce(tm.lifetime_transaction_count, 0)          as lifetime_transaction_count,
        coalesce(tm.lifetime_transaction_value, 0)          as lifetime_transaction_value,
        coalesce(tm.lifetime_revenue, 0)                    as lifetime_revenue,
        tm.first_transaction_date,
        tm.last_transaction_date,
        tm.source_system_count,

        -- Engagement tier
        case
            when coalesce(tm.lifetime_revenue, 0) >= 100000 then 'Enterprise'
            when coalesce(tm.lifetime_revenue, 0) >= 25000  then 'Mid-Market'
            when coalesce(tm.lifetime_revenue, 0) >= 5000   then 'SMB'
            when coalesce(tm.lifetime_revenue, 0) > 0       then 'Starter'
            else 'Prospect'
        end                                                 as customer_tier,

        -- Metadata
        g.first_seen_at,
        g.updated_at,
        current_timestamp()                                 as _dim_updated_at

    from golden g
    left join transaction_metrics tm
        on g.customer_golden_id = tm.customer_key

)

select * from final
