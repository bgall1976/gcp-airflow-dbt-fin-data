{{
    config(
        materialized='incremental',
        unique_key='lnk_transaction_account_hk'
    )
}}

/*
    lnk_transaction_account
    -----------------------
    Data Vault 2.0 Link capturing which account a transaction posts to.
*/

with relationships as (

    select distinct
        {{ dbt_utils.generate_surrogate_key(['t.transaction_unified_id', 't.source_system']) }}
                                                            as hub_transaction_hk,
        {{ dbt_utils.generate_surrogate_key(['t.source_entity_id', 't.source_system']) }}
                                                            as hub_account_hk,
        t.source_system                                     as record_source,
        t._unified_at                                       as load_date
    from {{ ref('int_unified_transactions') }} t
    where t.source_entity_id is not null

)

select
    {{ dbt_utils.generate_surrogate_key(['hub_transaction_hk', 'hub_account_hk']) }}
                                                            as lnk_transaction_account_hk,
    hub_transaction_hk,
    hub_account_hk,
    record_source,
    load_date
from relationships

{% if is_incremental() %}
where {{ dbt_utils.generate_surrogate_key(['hub_transaction_hk', 'hub_account_hk']) }}
    not in (select lnk_transaction_account_hk from {{ this }})
{% endif %}
