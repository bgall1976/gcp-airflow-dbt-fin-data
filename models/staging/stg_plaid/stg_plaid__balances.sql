with source as (

    select * from {{ source('plaid', 'balances') }}

),

renamed as (

    select
        cast(account_id as string)                          as plaid_account_id,
        cast(current as numeric)                            as current_balance,
        cast(available as numeric)                          as available_balance,
        cast(limit as numeric)                              as credit_limit,
        upper(iso_currency_code)                            as currency_code,
        cast(snapshot_date as date)                         as balance_date,
        _loaded_at,
        'plaid'                                             as source_system

    from source

)

select * from renamed
