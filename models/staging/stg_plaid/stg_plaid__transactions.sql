with source as (

    select * from {{ source('plaid', 'transactions') }}

),

renamed as (

    select
        cast(transaction_id as string)                      as plaid_transaction_id,
        cast(account_id as string)                          as plaid_account_id,
        cast(amount as numeric) * -1                        as amount,
        -- Plaid reports outflows as positive; we invert to match accounting convention
        upper(iso_currency_code)                            as currency_code,
        cast(date as date)                                  as transaction_date,
        cast(authorized_date as date)                       as authorized_date,
        cast(name as string)                                as merchant_name,
        cast(merchant_name as string)                       as clean_merchant_name,
        cast(payment_channel as string)                     as payment_channel,
        cast(pending as boolean)                            as is_pending,
        cast(category_id as string)                         as plaid_category_id,
        array_to_string(
            json_extract_string_array(category, '$'),
            ' > '
        )                                                   as category_hierarchy,
        json_extract_scalar(personal_finance_category, '$.primary')
                                                            as pfc_primary,
        json_extract_scalar(personal_finance_category, '$.detailed')
                                                            as pfc_detailed,
        case
            when cast(amount as numeric) > 0 then 'outflow'
            when cast(amount as numeric) < 0 then 'inflow'
            else 'zero'
        end                                                 as flow_direction,
        _loaded_at,
        'plaid'                                             as source_system

    from source
    where cast(pending as boolean) = false

)

select * from renamed
