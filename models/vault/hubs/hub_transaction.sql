{{
    config(
        materialized='incremental',
        unique_key='hub_transaction_hk'
    )
}}

/*
    hub_transaction
    ---------------
    Data Vault 2.0 Hub for financial transactions across all sources.
*/

with source_keys as (

    select
        transaction_unified_id                              as transaction_bk,
        source_system                                       as record_source,
        _unified_at                                         as load_date
    from {{ ref('int_unified_transactions') }}

)

select
    {{ dbt_utils.generate_surrogate_key(['transaction_bk', 'record_source']) }}
                                                            as hub_transaction_hk,
    transaction_bk,
    record_source,
    load_date
from source_keys

{% if is_incremental() %}
where {{ dbt_utils.generate_surrogate_key(['transaction_bk', 'record_source']) }}
    not in (select hub_transaction_hk from {{ this }})
{% endif %}
