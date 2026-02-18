{{
    config(materialized='table')
}}

/*
    Unified Chart of Accounts
    -------------------------
    Merges account definitions from QuickBooks and NetSuite into a single
    canonical chart of accounts with standardized classifications.
*/

with qb_accounts as (

    select
        {{ dbt_utils.generate_surrogate_key(['account_id', "'quickbooks'"]) }}
                                                            as account_golden_id,
        account_id                                          as source_account_id,
        account_name,
        account_number,
        account_type,
        account_sub_type,
        classification,
        is_active,
        source_system
    from {{ ref('stg_quickbooks__accounts') }}

),

ns_accounts as (

    select
        {{ dbt_utils.generate_surrogate_key(['account_id', "'netsuite'"]) }}
                                                            as account_golden_id,
        account_id                                          as source_account_id,
        account_name,
        account_number,
        account_type,
        null                                                as account_sub_type,
        classification,
        is_active,
        source_system
    from {{ ref('stg_netsuite__accounts') }}

),

-- Seed file provides the canonical mapping
canonical as (

    select * from {{ ref('seed_chart_of_accounts') }}

),

unified as (

    select * from qb_accounts
    union all
    select * from ns_accounts

),

final as (

    select
        u.account_golden_id,
        u.source_account_id,
        u.source_system,
        u.account_name,
        u.account_number,
        u.account_type,
        u.account_sub_type,

        -- Use canonical mapping if available, otherwise fall back to source
        coalesce(c.canonical_classification, u.classification)
                                                            as classification,
        coalesce(c.canonical_category, u.account_type)      as category,
        coalesce(c.financial_statement_line, 'Other')       as financial_statement_line,
        coalesce(c.is_balance_sheet, false)                 as is_balance_sheet,
        coalesce(c.is_income_statement, false)              as is_income_statement,

        u.is_active,
        current_timestamp()                                 as _golden_record_updated_at

    from unified u
    left join canonical c
        on u.account_number = c.account_code
        and u.source_system = c.source_system

)

select * from final
