with source as (

    select * from {{ source('quickbooks', 'invoices') }}

),

renamed as (

    select
        cast(id as string)                                  as invoice_id,
        cast(customer_ref_value as string)                  as customer_id,
        cast(doc_number as string)                          as invoice_number,
        cast(total_amt as numeric)                          as total_amount,
        cast(balance as numeric)                            as balance_due,
        upper(trim(currency_ref_value))                     as currency_code,
        cast(txn_date as date)                              as invoice_date,
        cast(due_date as date)                              as due_date,
        cast(email_status as string)                        as email_status,
        case
            when balance = 0 then 'paid'
            when due_date < current_date() then 'overdue'
            else 'open'
        end                                                 as payment_status,
        timestamp(meta_create_time)                         as created_at,
        timestamp(meta_last_updated_time)                   as updated_at,
        _loaded_at,
        'quickbooks'                                        as source_system

    from source

)

select * from renamed
