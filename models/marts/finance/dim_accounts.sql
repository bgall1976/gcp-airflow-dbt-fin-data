{{
    config(materialized='table')
}}

/*
    dim_accounts
    ------------
    Conformed chart of accounts dimension from the unified account mapping.
*/

select
    account_golden_id                                       as account_key,
    source_account_id,
    source_system,
    account_name,
    account_number,
    account_type,
    account_sub_type,
    classification,
    category,
    financial_statement_line,
    is_balance_sheet,
    is_income_statement,
    is_active,
    _golden_record_updated_at                               as _dim_updated_at

from {{ ref('int_unified_chart_of_accounts') }}
