{{
    config(materialized='table')
}}

/*
    dq_golden_record_completeness
    -----------------------------
    Measures how complete each golden customer record is across key attributes.
    Surfaces customers with poor data quality that need enrichment.
*/

with customers as (

    select * from {{ ref('dim_customers') }}

),

completeness_scores as (

    select
        customer_key,
        company_name,
        customer_tier,
        primary_source,

        -- Score each attribute (1 = present, 0 = missing)
        case when company_name is not null then 1 else 0 end
            + case when email is not null then 1 else 0 end
            + case when phone is not null then 1 else 0 end
            + case when billing_address is not null then 1 else 0 end
            + case when billing_city is not null then 1 else 0 end
            + case when billing_state is not null then 1 else 0 end
            + case when billing_postal_code is not null then 1 else 0 end
            + case when billing_country is not null then 1 else 0 end
            + case when industry is not null then 1 else 0 end
            + case when customer_segment is not null then 1 else 0 end
                                                            as fields_populated,
        10                                                  as total_fields,

        -- Individual field flags
        company_name is not null                            as has_company_name,
        email is not null                                   as has_email,
        phone is not null                                   as has_phone,
        billing_address is not null                         as has_address,
        billing_city is not null                            as has_city,
        billing_state is not null                           as has_state,
        billing_postal_code is not null                     as has_postal_code,
        billing_country is not null                         as has_country,
        industry is not null                                as has_industry,
        customer_segment is not null                        as has_segment,
        is_multi_source_match                               as has_cross_system_match

    from customers

),

final as (

    select
        *,
        round(fields_populated / total_fields * 100, 1)     as completeness_pct,
        case
            when fields_populated >= 9 then 'excellent'
            when fields_populated >= 7 then 'good'
            when fields_populated >= 5 then 'fair'
            else 'poor'
        end                                                 as completeness_grade,
        current_timestamp()                                 as _checked_at

    from completeness_scores

)

select * from final
order by completeness_pct asc
