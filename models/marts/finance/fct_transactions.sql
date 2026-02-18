{{
    config(
        materialized='incremental',
        unique_key='transaction_key',
        partition_by={
            "field": "transaction_date",
            "data_type": "date",
            "granularity": "month"
        },
        cluster_by=['transaction_category', 'source_system']
    )
}}

/*
    fct_transactions
    ----------------
    Core financial transactions fact table. Joins unified transactions to golden
    customer and account dimensions. Every row represents one financial event
    with foreign keys to conformed dimensions.
*/

with transactions as (

    select * from {{ ref('int_unified_transactions') }}
    {% if is_incremental() %}
    where _unified_at > (select max(_unified_at) from {{ this }})
    {% endif %}

),

customers as (

    select * from {{ ref('int_golden_customers') }}

),

-- Map source entity IDs to golden customer IDs
customer_lookup as (

    select customer_golden_id, stripe_customer_id as source_id, 'stripe' as sys
    from customers where stripe_customer_id is not null
    union all
    select customer_golden_id, quickbooks_customer_id, 'quickbooks'
    from customers where quickbooks_customer_id is not null
    union all
    select customer_golden_id, netsuite_customer_id, 'netsuite'
    from customers where netsuite_customer_id is not null

),

final as (

    select
        t.transaction_unified_id                            as transaction_key,
        t.source_transaction_id,
        cl.customer_golden_id                               as customer_key,
        t.transaction_date                                  as date_key,

        -- Transaction attributes
        t.transaction_type,
        t.transaction_category,
        t.entity_type,
        t.status,
        t.reference_number,
        t.memo,
        t.source_system,

        -- Measures
        t.amount,
        t.currency_code,
        case
            when t.transaction_category = 'cash_receipt' then t.amount
            else 0
        end                                                 as cash_inflow,
        case
            when t.transaction_category = 'cash_disbursement' then abs(t.amount)
            else 0
        end                                                 as cash_outflow,
        case
            when t.transaction_category = 'accounts_receivable' then t.amount
            else 0
        end                                                 as revenue_amount,

        -- Metadata
        t.created_at                                        as transaction_created_at,
        t._unified_at

    from transactions t
    left join customer_lookup cl
        on t.source_entity_id = cl.source_id
        and t.source_system = cl.sys

)

select * from final
