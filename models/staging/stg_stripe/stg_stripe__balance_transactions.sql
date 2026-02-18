with source as (

    select * from {{ source('stripe', 'balance_transactions') }}

),

renamed as (

    select
        cast(id as string)                                  as balance_transaction_id,
        cast(amount as numeric) / 100                       as amount,
        cast(fee as numeric) / 100                          as fee_amount,
        cast(net as numeric) / 100                          as net_amount,
        upper(currency)                                     as currency_code,
        cast(type as string)                                as transaction_type,
        cast(status as string)                              as transaction_status,
        cast(source as string)                              as source_id,
        cast(description as string)                         as description,
        timestamp_seconds(cast(created as int64))           as created_at,
        timestamp_seconds(cast(available_on as int64))      as available_on,
        _loaded_at,
        'stripe'                                            as source_system

    from source

)

select * from renamed
