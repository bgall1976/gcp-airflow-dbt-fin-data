with source as (

    select * from {{ source('quickbooks', 'accounts') }}

),

renamed as (

    select
        cast(id as string)                                  as account_id,
        cast(name as string)                                as account_name,
        cast(account_type as string)                        as account_type,
        cast(account_sub_type as string)                    as account_sub_type,
        cast(acct_num as string)                            as account_number,
        cast(classification as string)                      as classification,
        cast(current_balance as numeric)                    as current_balance,
        upper(trim(currency_ref_value))                     as currency_code,
        cast(active as boolean)                             as is_active,
        timestamp(meta_create_time)                         as created_at,
        timestamp(meta_last_updated_time)                   as updated_at,
        _loaded_at,
        'quickbooks'                                        as source_system

    from source

)

select * from renamed
