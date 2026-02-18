{{
    config(
        materialized='incremental',
        unique_key='hub_customer_hk',
        on_schema_change='append_new_columns'
    )
}}

/*
    hub_customer
    ------------
    Data Vault 2.0 Hub table for the Customer business entity.
    Contains only the business key and metadata -- no descriptive attributes.
    Loaded from all source systems that contain customer records.
*/

with source_keys as (

    -- QuickBooks customers
    select
        customer_id                                         as customer_bk,
        'quickbooks'                                        as record_source,
        _loaded_at                                          as load_date
    from {{ ref('stg_quickbooks__customers') }}

    union all

    -- Stripe customers
    select
        stripe_customer_id                                  as customer_bk,
        'stripe'                                            as record_source,
        _loaded_at                                          as load_date
    from {{ ref('stg_stripe__customers') }}

    union all

    -- Salesforce accounts (company = customer)
    select
        sf_account_id                                       as customer_bk,
        'salesforce'                                        as record_source,
        _loaded_at                                          as load_date
    from {{ ref('stg_salesforce__accounts') }}

),

hashed as (

    select
        {{ dbt_utils.generate_surrogate_key(['customer_bk', 'record_source']) }}
                                                            as hub_customer_hk,
        customer_bk,
        record_source,
        load_date
    from source_keys

)

select * from hashed

{% if is_incremental() %}
where hub_customer_hk not in (select hub_customer_hk from {{ this }})
{% endif %}
