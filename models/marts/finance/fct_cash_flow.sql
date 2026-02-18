{{
    config(
        materialized='table',
        partition_by={
            "field": "cash_flow_date",
            "data_type": "date",
            "granularity": "month"
        }
    )
}}

/*
    fct_cash_flow
    -------------
    Daily cash flow combining bank transaction data (Plaid) with accounting
    cash entries (QuickBooks payments, Stripe payouts). Provides a single
    view of cash position and movement.
*/

with bank_transactions as (

    -- Actual bank transactions from Plaid
    select
        plaid_transaction_id                                as cash_flow_id,
        plaid_account_id                                    as bank_account_id,
        transaction_date                                    as cash_flow_date,
        amount,
        currency_code,
        case
            when amount > 0 then amount else 0
        end                                                 as inflow_amount,
        case
            when amount < 0 then abs(amount) else 0
        end                                                 as outflow_amount,
        pfc_primary                                         as category,
        pfc_detailed                                        as subcategory,
        clean_merchant_name                                 as counterparty,
        payment_channel,
        'plaid'                                             as source_system,
        'bank_transaction'                                  as cash_flow_type
    from {{ ref('stg_plaid__transactions') }}

),

stripe_payouts as (

    -- Stripe balance transactions that hit the bank
    select
        balance_transaction_id                              as cash_flow_id,
        null                                                as bank_account_id,
        date(created_at)                                    as cash_flow_date,
        net_amount                                          as amount,
        currency_code,
        case
            when net_amount > 0 then net_amount else 0
        end                                                 as inflow_amount,
        case
            when net_amount < 0 then abs(net_amount) else 0
        end                                                 as outflow_amount,
        transaction_type                                    as category,
        null                                                as subcategory,
        'Stripe'                                            as counterparty,
        'electronic'                                        as payment_channel,
        'stripe'                                            as source_system,
        'payment_processor'                                 as cash_flow_type
    from {{ ref('stg_stripe__balance_transactions') }}
    where transaction_type in ('payout', 'charge', 'refund')

),

daily_balances as (

    select
        plaid_account_id                                    as bank_account_id,
        balance_date,
        current_balance,
        available_balance
    from {{ ref('stg_plaid__balances') }}

),

combined as (

    select * from bank_transactions
    union all
    select * from stripe_payouts

),

final as (

    select
        c.cash_flow_id,
        c.bank_account_id,
        c.cash_flow_date,
        c.amount,
        c.currency_code,
        c.inflow_amount,
        c.outflow_amount,
        c.inflow_amount - c.outflow_amount                  as net_cash_flow,
        c.category,
        c.subcategory,
        c.counterparty,
        c.payment_channel,
        c.source_system,
        c.cash_flow_type,

        -- Join daily balance when available
        b.current_balance                                   as eod_balance,
        b.available_balance                                 as eod_available_balance,

        -- Time dimensions
        extract(year from c.cash_flow_date)                 as year,
        extract(quarter from c.cash_flow_date)              as quarter,
        extract(month from c.cash_flow_date)                as month,
        extract(dayofweek from c.cash_flow_date)            as day_of_week,

        current_timestamp()                                 as _loaded_at

    from combined c
    left join daily_balances b
        on c.bank_account_id = b.bank_account_id
        and c.cash_flow_date = b.balance_date

)

select * from final
