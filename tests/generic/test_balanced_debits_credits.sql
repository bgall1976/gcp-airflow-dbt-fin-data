{% test balanced_debits_credits(model, debit_column, credit_column, group_by_column, tolerance=0.01) %}

/*
    Validates that total debits equal total credits within a tolerance.
    Fundamental accounting equation check.
*/

with totals as (

    select
        {{ group_by_column }},
        sum({{ debit_column }})     as total_debits,
        sum({{ credit_column }})    as total_credits
    from {{ model }}
    group by {{ group_by_column }}

)

select *
from totals
where abs(total_debits - total_credits) > {{ tolerance }}

{% endtest %}
