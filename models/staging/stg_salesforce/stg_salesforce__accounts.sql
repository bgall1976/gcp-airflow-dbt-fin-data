with source as (

    select * from {{ source('salesforce', 'accounts') }}

),

renamed as (

    select
        cast(id as string)                                  as sf_account_id,
        cast(name as string)                                as company_name,
        cast(type as string)                                as account_type,
        cast(industry as string)                            as industry,
        cast(annual_revenue as numeric)                     as annual_revenue,
        cast(number_of_employees as int64)                  as employee_count,
        cast(billing_street as string)                      as billing_street,
        cast(billing_city as string)                        as billing_city,
        cast(billing_state as string)                       as billing_state,
        cast(billing_postal_code as string)                 as billing_postal_code,
        cast(billing_country as string)                     as billing_country,
        cast(phone as string)                               as phone,
        cast(website as string)                             as website,
        cast(owner_id as string)                            as owner_id,
        timestamp(created_date)                             as created_at,
        timestamp(last_modified_date)                       as updated_at,
        _loaded_at,
        'salesforce'                                        as source_system

    from source
    where is_deleted = false

)

select * from renamed
