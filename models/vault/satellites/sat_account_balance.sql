{{
    config(
        materialized='incremental',
        unique_key='sat_account_balance_hk'
    )
}}

/*
    sat_account_balance
    -------------------
    Satellite tracking account balance changes over time from QuickBooks.
*/

with source_data as (

    select
        {{ dbt_utils.generate_surrogate_key(['account_id', "'quickbooks'"]) }}
                                                            as hub_account_hk,
        account_name,
        current_balance,
        currency_code,
        is_active,
        'quickbooks'                                        as record_source,
        _loaded_at                                          as load_date,
        {{ dbt_utils.generate_surrogate_key([
            'current_balance', 'is_active'
        ]) }}                                               as hash_diff
    from {{ ref('stg_quickbooks__accounts') }}

)

select
    {{ dbt_utils.generate_surrogate_key(['hub_account_hk', 'load_date']) }}
                                                            as sat_account_balance_hk,
    hub_account_hk,
    hash_diff,
    account_name,
    current_balance,
    currency_code,
    is_active,
    record_source,
    load_date
from source_data

{% if is_incremental() %}
where hub_account_hk not in (
    select hub_account_hk from {{ this }}
    where hash_diff = source_data.hash_diff
)
{% endif %}
