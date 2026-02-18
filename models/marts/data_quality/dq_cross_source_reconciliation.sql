{{
    config(materialized='table')
}}

/*
    dq_cross_source_reconciliation
    ------------------------------
    Compares transaction totals across source systems by date to identify
    discrepancies. Large variances indicate extraction issues, duplicate
    records, or missed transactions.
*/

with daily_totals_by_source as (

    select
        transaction_date,
        source_system,
        transaction_category,
        count(*)                                            as transaction_count,
        sum(amount)                                         as total_amount,
        sum(case when amount > 0 then amount else 0 end)    as total_debits,
        sum(case when amount < 0 then abs(amount) else 0 end) as total_credits
    from {{ ref('int_unified_transactions') }}
    where transaction_date >= date_sub(current_date(), interval 90 day)
    group by 1, 2, 3

),

-- Compare accounting system totals vs bank totals
accounting_vs_bank as (

    select
        d_acct.transaction_date,
        d_acct.transaction_category,

        -- Accounting side (QB + NS)
        sum(case when d_acct.source_system in ('quickbooks', 'netsuite')
            then d_acct.total_amount else 0 end)            as accounting_total,

        -- Bank side (Plaid)
        sum(case when d_acct.source_system = 'plaid'
            then d_acct.total_amount else 0 end)            as bank_total,

        -- Stripe
        sum(case when d_acct.source_system = 'stripe'
            then d_acct.total_amount else 0 end)            as stripe_total

    from daily_totals_by_source d_acct
    where d_acct.transaction_category in ('cash_receipt', 'cash_disbursement')
    group by 1, 2

),

final as (

    select
        transaction_date,
        transaction_category,
        accounting_total,
        bank_total,
        stripe_total,
        accounting_total - bank_total                       as accounting_bank_variance,
        abs(accounting_total - bank_total)                  as absolute_variance,

        case
            when bank_total = 0 and accounting_total = 0 then 0
            when bank_total = 0 then 100
            else round(
                abs(accounting_total - bank_total) / abs(bank_total) * 100, 2
            )
        end                                                 as variance_pct,

        case
            when abs(accounting_total - bank_total) < 1 then 'match'
            when abs(accounting_total - bank_total) < 100 then 'minor_variance'
            when abs(accounting_total - bank_total) < 1000 then 'notable_variance'
            else 'critical_variance'
        end                                                 as reconciliation_status,

        current_timestamp()                                 as _checked_at

    from accounting_vs_bank

)

select * from final
where reconciliation_status != 'match'
order by transaction_date desc, absolute_variance desc
