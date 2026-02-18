{{
    config(
        materialized='incremental',
        unique_key='sat_customer_stripe_hk'
    )
}}

/*
    sat_customer_stripe
    -------------------
    Satellite storing Stripe-specific customer attributes.
    Includes payment metadata not available in other systems.
*/

with source_data as (

    select
        {{ dbt_utils.generate_surrogate_key(['stripe_customer_id', "'stripe'"]) }}
                                                            as hub_customer_hk,
        customer_name,
        email,
        phone,
        address_line1,
        city,
        state,
        postal_code,
        country,
        currency_code,
        is_delinquent,
        quickbooks_customer_id                              as xref_quickbooks_id,
        netsuite_customer_id                                as xref_netsuite_id,
        salesforce_account_id                               as xref_salesforce_id,
        'stripe'                                            as record_source,
        _loaded_at                                          as load_date,
        {{ dbt_utils.generate_surrogate_key([
            'customer_name', 'email', 'phone', 'city', 'state',
            'is_delinquent', 'quickbooks_customer_id',
            'netsuite_customer_id', 'salesforce_account_id'
        ]) }}                                               as hash_diff
    from {{ ref('stg_stripe__customers') }}

)

select
    {{ dbt_utils.generate_surrogate_key(['hub_customer_hk', 'load_date']) }}
                                                            as sat_customer_stripe_hk,
    hub_customer_hk,
    hash_diff,
    customer_name,
    email,
    phone,
    address_line1,
    city,
    state,
    postal_code,
    country,
    currency_code,
    is_delinquent,
    xref_quickbooks_id,
    xref_netsuite_id,
    xref_salesforce_id,
    record_source,
    load_date
from source_data

{% if is_incremental() %}
where hub_customer_hk not in (
    select hub_customer_hk from {{ this }}
    where hash_diff = source_data.hash_diff
)
{% endif %}
