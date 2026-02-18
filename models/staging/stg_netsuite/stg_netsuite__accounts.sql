with source as (

    select * from {{ source('netsuite', 'accounts') }}

),

renamed as (

    select
        cast(id as string)                                  as account_id,
        cast(acct_name as string)                           as account_name,
        cast(acct_number as string)                         as account_number,
        cast(acct_type as string)                           as account_type,
        cast(general_rate_type as string)                   as rate_type,
        cast(parent as string)                              as parent_account_id,
        cast(is_inactive as boolean)                        as is_inactive,
        not cast(is_inactive as boolean)                    as is_active,
        case cast(acct_type as string)
            when 'Bank' then 'Asset'
            when 'AcctRec' then 'Asset'
            when 'OthCurrAsset' then 'Asset'
            when 'FixedAsset' then 'Asset'
            when 'OthAsset' then 'Asset'
            when 'AcctPay' then 'Liability'
            when 'CreditCard' then 'Liability'
            when 'OthCurrLiab' then 'Liability'
            when 'LongTermLiab' then 'Liability'
            when 'Equity' then 'Equity'
            when 'Income' then 'Revenue'
            when 'OthIncome' then 'Revenue'
            when 'Expense' then 'Expense'
            when 'OthExpense' then 'Expense'
            when 'COGS' then 'Expense'
            else 'Other'
        end                                                 as classification,
        _loaded_at,
        'netsuite'                                          as source_system

    from source

)

select * from renamed
