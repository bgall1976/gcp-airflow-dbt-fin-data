{{
    config(
        materialized='incremental',
        unique_key='sat_transaction_details_hk'
    )
}}

/*
    sat_transaction_details
    -----------------------
    Satellite storing descriptive attributes for unified transactions.
*/

with source_data as (

    select
        {{ dbt_utils.generate_surrogate_key(['transaction_unified_id', 'source_system']) }}
                                                            as hub_transaction_hk,
        transaction_type,
        transaction_category,
        amount,
        currency_code,
        transaction_date,
        status,
        reference_number,
        memo,
        source_system                                       as record_source,
        _unified_at                                         as load_date,
        {{ dbt_utils.generate_surrogate_key([
            'transaction_type', 'transaction_category', 'amount',
            'status', 'reference_number', 'memo'
        ]) }}                                               as hash_diff
    from {{ ref('int_unified_transactions') }}

)

select
    {{ dbt_utils.generate_surrogate_key(['hub_transaction_hk', 'load_date']) }}
                                                            as sat_transaction_details_hk,
    hub_transaction_hk,
    hash_diff,
    transaction_type,
    transaction_category,
    amount,
    currency_code,
    transaction_date,
    status,
    reference_number,
    memo,
    record_source,
    load_date
from source_data

{% if is_incremental() %}
where hub_transaction_hk not in (
    select hub_transaction_hk from {{ this }}
    where hash_diff = source_data.hash_diff
)
{% endif %}
