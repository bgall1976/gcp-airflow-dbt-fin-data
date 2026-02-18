{{
    config(
        materialized='incremental',
        unique_key='sat_customer_details_hk'
    )
}}

/*
    sat_customer_details
    --------------------
    Data Vault 2.0 Satellite storing descriptive attributes for customers
    sourced from QuickBooks. Full history is maintained -- a new row is
    inserted whenever any attribute changes.
*/

with source_data as (

    select
        {{ dbt_utils.generate_surrogate_key(['customer_id', "'quickbooks'"]) }}
                                                            as hub_customer_hk,
        display_name,
        company_name,
        first_name,
        last_name,
        email,
        phone,
        billing_address_line1,
        billing_city,
        billing_state,
        billing_postal_code,
        billing_country,
        is_active,
        outstanding_balance,
        currency_code,
        'quickbooks'                                        as record_source,
        _loaded_at                                          as load_date,
        {{ dbt_utils.generate_surrogate_key([
            'display_name', 'company_name', 'first_name', 'last_name',
            'email', 'phone', 'billing_address_line1', 'billing_city',
            'billing_state', 'billing_postal_code', 'is_active',
            'outstanding_balance'
        ]) }}                                               as hash_diff
    from {{ ref('stg_quickbooks__customers') }}

),

final as (

    select
        {{ dbt_utils.generate_surrogate_key(['hub_customer_hk', 'load_date']) }}
                                                            as sat_customer_details_hk,
        hub_customer_hk,
        hash_diff,
        display_name,
        company_name,
        first_name,
        last_name,
        email,
        phone,
        billing_address_line1,
        billing_city,
        billing_state,
        billing_postal_code,
        billing_country,
        is_active,
        outstanding_balance,
        currency_code,
        record_source,
        load_date
    from source_data

)

select * from final

{% if is_incremental() %}
where hub_customer_hk not in (
    select hub_customer_hk from {{ this }}
    where hash_diff = final.hash_diff
)
{% endif %}
