with source as (

    select * from {{ source('salesforce', 'opportunities') }}

),

renamed as (

    select
        cast(id as string)                                  as opportunity_id,
        cast(account_id as string)                          as sf_account_id,
        cast(name as string)                                as opportunity_name,
        cast(stage_name as string)                          as stage,
        cast(amount as numeric)                             as amount,
        upper(coalesce(cast(currency_iso_code as string), 'USD'))
                                                            as currency_code,
        cast(probability as numeric) / 100                  as probability,
        cast(close_date as date)                            as close_date,
        cast(type as string)                                as opportunity_type,
        cast(is_won as boolean)                             as is_won,
        cast(is_closed as boolean)                          as is_closed,
        cast(fiscal_year as int64)                          as fiscal_year,
        cast(fiscal_quarter as int64)                       as fiscal_quarter,
        cast(owner_id as string)                            as owner_id,
        timestamp(created_date)                             as created_at,
        timestamp(last_modified_date)                       as updated_at,
        _loaded_at,
        'salesforce'                                        as source_system

    from source
    where is_deleted = false

)

select * from renamed
