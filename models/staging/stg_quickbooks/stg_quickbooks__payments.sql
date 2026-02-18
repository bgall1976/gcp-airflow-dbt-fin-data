with source as (

    select * from {{ source('quickbooks', 'payments') }}

),

renamed as (

    select
        cast(id as string)                                  as payment_id,
        cast(customer_ref_value as string)                  as customer_id,
        cast(total_amt as numeric)                          as payment_amount,
        upper(trim(currency_ref_value))                     as currency_code,
        cast(txn_date as date)                              as payment_date,
        cast(deposit_to_account_ref_value as string)        as deposit_account_id,
        coalesce(
            cast(payment_method_ref_value as string),
            'unknown'
        )                                                   as payment_method,
        timestamp(meta_create_time)                         as created_at,
        timestamp(meta_last_updated_time)                   as updated_at,
        _loaded_at,
        'quickbooks'                                        as source_system

    from source

)

select * from renamed
