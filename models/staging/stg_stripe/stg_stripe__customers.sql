with source as (

    select * from {{ source('stripe', 'customers') }}

),

renamed as (

    select
        cast(id as string)                                  as stripe_customer_id,
        cast(name as string)                                as customer_name,
        cast(email as string)                               as email,
        cast(phone as string)                               as phone,
        json_extract_scalar(address, '$.line1')             as address_line1,
        json_extract_scalar(address, '$.city')              as city,
        json_extract_scalar(address, '$.state')             as state,
        json_extract_scalar(address, '$.postal_code')       as postal_code,
        json_extract_scalar(address, '$.country')           as country,
        upper(coalesce(currency, 'USD'))                    as currency_code,
        cast(delinquent as boolean)                         as is_delinquent,
        json_extract_scalar(metadata, '$.qb_customer_id')   as quickbooks_customer_id,
        json_extract_scalar(metadata, '$.ns_customer_id')   as netsuite_customer_id,
        json_extract_scalar(metadata, '$.sf_account_id')    as salesforce_account_id,
        timestamp_seconds(cast(created as int64))           as created_at,
        _loaded_at,
        'stripe'                                            as source_system

    from source

)

select * from renamed
