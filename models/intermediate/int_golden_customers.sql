{{
    config(
        materialized='table',
        partition_by={
            "field": "updated_at",
            "data_type": "timestamp",
            "granularity": "day"
        }
    )
}}

/*
    Golden Customer Record
    ----------------------
    Unifies customer data across QuickBooks, Stripe, NetSuite, and Salesforce
    into a single canonical record per customer.

    Resolution priority (highest to lowest):
    1. Salesforce - primary CRM, richest company data
    2. NetSuite  - ERP system of record for financials
    3. QuickBooks - accounting system
    4. Stripe    - payment processor

    Matching strategy:
    - Exact match on cross-system IDs stored in Stripe metadata
    - Fuzzy match on email address (case-insensitive)
    - Fuzzy match on normalized company name + postal code
*/

with stripe_customers as (

    select * from {{ ref('stg_stripe__customers') }}

),

qb_customers as (

    select * from {{ ref('stg_quickbooks__customers') }}

),

sf_accounts as (

    select * from {{ ref('stg_salesforce__accounts') }}

),

-- Step 1: Build the cross-reference from Stripe metadata IDs
cross_ref as (

    select
        stripe_customer_id,
        quickbooks_customer_id,
        netsuite_customer_id,
        salesforce_account_id,
        email
    from stripe_customers
    where stripe_customer_id is not null

),

-- Step 2: Match QuickBooks customers that lack a Stripe cross-ref via email
qb_email_match as (

    select
        qb.customer_id                                      as quickbooks_customer_id,
        sc.stripe_customer_id,
        sc.salesforce_account_id,
        sc.netsuite_customer_id
    from qb_customers qb
    inner join stripe_customers sc
        on lower(trim(qb.email)) = lower(trim(sc.email))
    where qb.email is not null
        and sc.email is not null
        and qb.customer_id not in (
            select quickbooks_customer_id from cross_ref
            where quickbooks_customer_id is not null
        )

),

-- Step 3: Combine all cross-references
all_mappings as (

    select
        stripe_customer_id,
        quickbooks_customer_id,
        netsuite_customer_id,
        salesforce_account_id
    from cross_ref

    union all

    select
        stripe_customer_id,
        quickbooks_customer_id,
        netsuite_customer_id,
        salesforce_account_id
    from qb_email_match

),

-- Step 4: Generate a deterministic golden ID
golden_ids as (

    select
        {{ dbt_utils.generate_surrogate_key([
            'coalesce(salesforce_account_id, "")',
            'coalesce(netsuite_customer_id, "")',
            'coalesce(quickbooks_customer_id, "")',
            'coalesce(stripe_customer_id, "")'
        ]) }}                                               as customer_golden_id,
        stripe_customer_id,
        quickbooks_customer_id,
        netsuite_customer_id,
        salesforce_account_id
    from all_mappings

),

-- Step 5: Merge attributes with priority-based coalescing
-- Priority: Salesforce > QuickBooks > Stripe
final as (

    select
        gi.customer_golden_id,

        -- Source system IDs
        gi.stripe_customer_id,
        gi.quickbooks_customer_id,
        gi.netsuite_customer_id,
        gi.salesforce_account_id,

        -- Company name: SF > QB > Stripe
        coalesce(
            sf.company_name,
            qb.company_name,
            sc.customer_name
        )                                                   as company_name,

        -- Contact info: prioritize the most complete
        coalesce(qb.first_name, split(sc.customer_name, ' ')[safe_offset(0)])
                                                            as first_name,
        coalesce(qb.last_name, split(sc.customer_name, ' ')[safe_offset(1)])
                                                            as last_name,
        coalesce(
            lower(trim(qb.email)),
            lower(trim(sc.email))
        )                                                   as email,
        coalesce(qb.phone, sf.phone, sc.phone)              as phone,
        coalesce(sf.website)                                as website,

        -- Address: SF > QB > Stripe
        coalesce(sf.billing_street, qb.billing_address_line1, sc.address_line1)
                                                            as billing_address,
        coalesce(sf.billing_city, qb.billing_city, sc.city) as billing_city,
        coalesce(sf.billing_state, qb.billing_state, sc.state)
                                                            as billing_state,
        coalesce(sf.billing_postal_code, qb.billing_postal_code, sc.postal_code)
                                                            as billing_postal_code,
        coalesce(sf.billing_country, qb.billing_country, sc.country)
                                                            as billing_country,

        -- Business attributes
        sf.industry,
        sf.annual_revenue,
        sf.employee_count,
        sf.account_type                                     as customer_segment,

        -- Flags
        coalesce(qb.is_active, true)                        as is_active_accounting,
        coalesce(sc.is_delinquent, false)                   as is_delinquent,

        -- Metadata
        least(
            coalesce(sf.created_at, timestamp('2099-01-01')),
            coalesce(qb.created_at, timestamp('2099-01-01')),
            coalesce(sc.created_at, timestamp('2099-01-01'))
        )                                                   as first_seen_at,
        greatest(
            coalesce(sf.updated_at, timestamp('2000-01-01')),
            coalesce(qb.updated_at, timestamp('2000-01-01')),
            coalesce(sc.created_at, timestamp('2000-01-01'))
        )                                                   as updated_at,

        -- Source tracking
        case
            when sf.sf_account_id is not null then 'salesforce'
            when qb.customer_id is not null then 'quickbooks'
            when sc.stripe_customer_id is not null then 'stripe'
        end                                                 as primary_source,

        (sf.sf_account_id is not null)
            and (qb.customer_id is not null or sc.stripe_customer_id is not null)
                                                            as is_multi_source_match,

        current_timestamp()                                 as _golden_record_updated_at

    from golden_ids gi
    left join stripe_customers sc
        on gi.stripe_customer_id = sc.stripe_customer_id
    left join qb_customers qb
        on gi.quickbooks_customer_id = qb.customer_id
    left join sf_accounts sf
        on gi.salesforce_account_id = sf.sf_account_id

)

select * from final
