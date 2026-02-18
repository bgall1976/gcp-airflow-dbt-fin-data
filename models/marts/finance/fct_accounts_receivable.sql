{{
    config(materialized='table')
}}

/*
    fct_accounts_receivable
    -----------------------
    AR aging and collection metrics built from QuickBooks invoices and payments.
    Each row represents an open or recently closed receivable with aging bucket
    classification.
*/

with invoices as (

    select * from {{ ref('stg_quickbooks__invoices') }}

),

payments as (

    select
        customer_id,
        payment_date,
        payment_amount,
        source_system
    from {{ ref('stg_quickbooks__payments') }}

),

golden_customers as (

    select * from {{ ref('int_golden_customers') }}

),

ar_detail as (

    select
        i.invoice_id,
        gc.customer_golden_id                               as customer_key,
        i.invoice_number,
        i.invoice_date,
        i.due_date,
        i.total_amount,
        i.balance_due,
        i.total_amount - i.balance_due                      as amount_paid,
        i.payment_status,
        i.currency_code,

        -- Aging calculation
        date_diff(current_date(), i.due_date, day)          as days_past_due,
        case
            when i.balance_due = 0 then 'Paid'
            when i.due_date >= current_date() then 'Current'
            when date_diff(current_date(), i.due_date, day) between 1 and 30
                then '1-30 Days'
            when date_diff(current_date(), i.due_date, day) between 31 and 60
                then '31-60 Days'
            when date_diff(current_date(), i.due_date, day) between 61 and 90
                then '61-90 Days'
            else '90+ Days'
        end                                                 as aging_bucket,

        case
            when i.balance_due = 0 then 0
            when i.due_date >= current_date() then 1
            when date_diff(current_date(), i.due_date, day) between 1 and 30 then 2
            when date_diff(current_date(), i.due_date, day) between 31 and 60 then 3
            when date_diff(current_date(), i.due_date, day) between 61 and 90 then 4
            else 5
        end                                                 as aging_bucket_sort,

        i.source_system,
        current_timestamp()                                 as _loaded_at

    from invoices i
    left join golden_customers gc
        on i.customer_id = gc.quickbooks_customer_id

)

select * from ar_detail
