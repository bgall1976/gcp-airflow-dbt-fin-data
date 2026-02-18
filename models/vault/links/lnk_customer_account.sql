{{
    config(
        materialized='incremental',
        unique_key='lnk_customer_account_hk'
    )
}}

/*
    lnk_customer_account
    --------------------
    Data Vault 2.0 Link capturing the relationship between customers
    and their financial accounts (e.g., a QuickBooks customer linked
    to the AR account their invoices post to).
*/

with relationships as (

    select distinct
        {{ dbt_utils.generate_surrogate_key(['i.customer_id', "'quickbooks'"]) }}
                                                            as hub_customer_hk,
        {{ dbt_utils.generate_surrogate_key(["'1100'", "'quickbooks'"]) }}
                                                            as hub_account_hk,
        'quickbooks'                                        as record_source,
        i._loaded_at                                        as load_date
    from {{ ref('stg_quickbooks__invoices') }} i
    where i.customer_id is not null

),

hashed as (

    select
        {{ dbt_utils.generate_surrogate_key(['hub_customer_hk', 'hub_account_hk']) }}
                                                            as lnk_customer_account_hk,
        hub_customer_hk,
        hub_account_hk,
        record_source,
        load_date
    from relationships

)

select * from hashed

{% if is_incremental() %}
where lnk_customer_account_hk not in (select lnk_customer_account_hk from {{ this }})
{% endif %}
