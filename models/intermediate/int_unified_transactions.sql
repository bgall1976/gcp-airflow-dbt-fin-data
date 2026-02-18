{{
    config(
        materialized='incremental',
        unique_key='transaction_unified_id',
        partition_by={
            "field": "transaction_date",
            "data_type": "date",
            "granularity": "month"
        },
        cluster_by=['source_system', 'transaction_type']
    )
}}

/*
    Unified Transactions
    --------------------
    Brings together all financial transactions from every source into a single
    spine with a common schema. Each row is one transaction from one source
    system. Downstream fact tables join to golden customer/vendor records.
*/

with qb_invoices as (

    select
        {{ dbt_utils.generate_surrogate_key(['invoice_id', "'qb_invoice'"]) }}
                                                            as transaction_unified_id,
        invoice_id                                          as source_transaction_id,
        'invoice'                                           as transaction_type,
        'accounts_receivable'                               as transaction_category,
        customer_id                                         as source_entity_id,
        'customer'                                          as entity_type,
        total_amount                                        as amount,
        currency_code,
        invoice_date                                        as transaction_date,
        payment_status                                      as status,
        invoice_number                                      as reference_number,
        null                                                as memo,
        created_at,
        source_system
    from {{ ref('stg_quickbooks__invoices') }}
    {% if is_incremental() %}
    where _loaded_at > (
        select max(_loaded_at) from {{ this }}
        where source_system = 'quickbooks'
    ) - interval {{ var('incremental_lookback') }} day
    {% endif %}

),

qb_payments as (

    select
        {{ dbt_utils.generate_surrogate_key(['payment_id', "'qb_payment'"]) }}
                                                            as transaction_unified_id,
        payment_id                                          as source_transaction_id,
        'payment_received'                                  as transaction_type,
        'cash_receipt'                                      as transaction_category,
        customer_id                                         as source_entity_id,
        'customer'                                          as entity_type,
        payment_amount                                      as amount,
        currency_code,
        payment_date                                        as transaction_date,
        'completed'                                         as status,
        null                                                as reference_number,
        null                                                as memo,
        created_at,
        source_system
    from {{ ref('stg_quickbooks__payments') }}
    {% if is_incremental() %}
    where _loaded_at > (
        select max(_loaded_at) from {{ this }}
        where source_system = 'quickbooks'
    ) - interval {{ var('incremental_lookback') }} day
    {% endif %}

),

stripe_charges as (

    select
        {{ dbt_utils.generate_surrogate_key(['charge_id', "'stripe_charge'"]) }}
                                                            as transaction_unified_id,
        charge_id                                           as source_transaction_id,
        'payment_received'                                  as transaction_type,
        'cash_receipt'                                      as transaction_category,
        stripe_customer_id                                  as source_entity_id,
        'customer'                                          as entity_type,
        charge_amount                                       as amount,
        currency_code,
        date(created_at)                                    as transaction_date,
        charge_status                                       as status,
        external_order_id                                   as reference_number,
        description                                         as memo,
        created_at,
        source_system
    from {{ ref('stg_stripe__charges') }}
    where is_paid = true
    {% if is_incremental() %}
        and _loaded_at > (
            select max(_loaded_at) from {{ this }}
            where source_system = 'stripe'
        ) - interval {{ var('incremental_lookback') }} day
    {% endif %}

),

ns_transactions as (

    select
        {{ dbt_utils.generate_surrogate_key(['transaction_id', "'ns_txn'"]) }}
                                                            as transaction_unified_id,
        transaction_id                                      as source_transaction_id,
        transaction_type,
        case
            when transaction_type in ('CustInvc', 'CustCred') then 'accounts_receivable'
            when transaction_type in ('VendBill', 'VendCred') then 'accounts_payable'
            when transaction_type = 'Journal' then 'journal_entry'
            when transaction_type in ('CustPymt', 'VendPymt') then 'cash_receipt'
            else 'other'
        end                                                 as transaction_category,
        entity_id                                           as source_entity_id,
        case
            when transaction_type like 'Cust%' then 'customer'
            when transaction_type like 'Vend%' then 'vendor'
            else 'other'
        end                                                 as entity_type,
        total_amount_base                                   as amount,
        'USD'                                               as currency_code,
        transaction_date,
        transaction_status                                  as status,
        transaction_number                                  as reference_number,
        memo,
        created_at,
        source_system
    from {{ ref('stg_netsuite__transactions') }}
    where is_posting = true
    {% if is_incremental() %}
        and _loaded_at > (
            select max(_loaded_at) from {{ this }}
            where source_system = 'netsuite'
        ) - interval {{ var('incremental_lookback') }} day
    {% endif %}

),

plaid_transactions as (

    select
        {{ dbt_utils.generate_surrogate_key(['plaid_transaction_id', "'plaid_txn'"]) }}
                                                            as transaction_unified_id,
        plaid_transaction_id                                as source_transaction_id,
        'bank_transaction'                                  as transaction_type,
        case
            when flow_direction = 'inflow' then 'cash_receipt'
            when flow_direction = 'outflow' then 'cash_disbursement'
            else 'other'
        end                                                 as transaction_category,
        plaid_account_id                                    as source_entity_id,
        'bank_account'                                      as entity_type,
        amount,
        currency_code,
        transaction_date,
        'posted'                                            as status,
        null                                                as reference_number,
        merchant_name                                       as memo,
        _loaded_at                                          as created_at,
        source_system
    from {{ ref('stg_plaid__transactions') }}
    {% if is_incremental() %}
    where _loaded_at > (
        select max(_loaded_at) from {{ this }}
        where source_system = 'plaid'
    ) - interval {{ var('incremental_lookback') }} day
    {% endif %}

),

unioned as (

    select * from qb_invoices
    union all
    select * from qb_payments
    union all
    select * from stripe_charges
    union all
    select * from ns_transactions
    union all
    select * from plaid_transactions

),

final as (

    select
        transaction_unified_id,
        source_transaction_id,
        transaction_type,
        transaction_category,
        source_entity_id,
        entity_type,
        amount,
        currency_code,
        transaction_date,
        status,
        reference_number,
        memo,
        created_at,
        source_system,
        current_timestamp()                                 as _unified_at
    from unioned

)

select * from final
