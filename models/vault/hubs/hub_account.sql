{{
    config(
        materialized='incremental',
        unique_key='hub_account_hk'
    )
}}

/*
    hub_account
    -----------
    Data Vault 2.0 Hub for financial accounts (chart of accounts entries).
*/

with source_keys as (

    select
        account_id                                          as account_bk,
        'quickbooks'                                        as record_source,
        _loaded_at                                          as load_date
    from {{ ref('stg_quickbooks__accounts') }}

    union all

    select
        account_id                                          as account_bk,
        'netsuite'                                          as record_source,
        _loaded_at                                          as load_date
    from {{ ref('stg_netsuite__accounts') }}

),

hashed as (

    select
        {{ dbt_utils.generate_surrogate_key(['account_bk', 'record_source']) }}
                                                            as hub_account_hk,
        account_bk,
        record_source,
        load_date
    from source_keys

)

select * from hashed

{% if is_incremental() %}
where hub_account_hk not in (select hub_account_hk from {{ this }})
{% endif %}
