/*
    Singular test: Validate that no more than 5% of transactions in
    fct_transactions lack a customer_key match. A higher rate indicates
    a problem in the golden record entity resolution logic.
*/

with stats as (

    select
        count(*)                                            as total_rows,
        countif(customer_key is null)                       as orphan_rows
    from {{ ref('fct_transactions') }}
    where entity_type = 'customer'

)

select *
from stats
where safe_divide(orphan_rows, total_rows) > 0.05
