with source as (

    select * from {{ source('quickbooks', 'customers') }}

),

renamed as (

    select
        cast(id as string)                                  as customer_id,
        cast(display_name as string)                        as display_name,
        cast(company_name as string)                        as company_name,
        cast(given_name as string)                          as first_name,
        cast(family_name as string)                         as last_name,
        cast(primary_email_addr_address as string)          as email,
        cast(primary_phone_free_form_number as string)      as phone,
        cast(bill_addr_line1 as string)                     as billing_address_line1,
        cast(bill_addr_city as string)                      as billing_city,
        cast(bill_addr_country_sub_division_code as string) as billing_state,
        cast(bill_addr_postal_code as string)               as billing_postal_code,
        cast(bill_addr_country as string)                   as billing_country,
        cast(active as boolean)                             as is_active,
        cast(balance as numeric)                            as outstanding_balance,
        upper(trim(currency_ref_value))                     as currency_code,
        timestamp(meta_create_time)                         as created_at,
        timestamp(meta_last_updated_time)                   as updated_at,
        _loaded_at,
        'quickbooks'                                        as source_system

    from source

)

select * from renamed
