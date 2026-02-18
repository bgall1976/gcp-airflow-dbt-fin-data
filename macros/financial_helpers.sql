{% macro cents_to_dollars(column_name) %}
    cast({{ column_name }} as numeric) / 100
{% endmacro %}


{% macro safe_divide(numerator, denominator, default_value=0) %}
    case
        when {{ denominator }} = 0 or {{ denominator }} is null then {{ default_value }}
        else {{ numerator }} / {{ denominator }}
    end
{% endmacro %}


{% macro classify_amount(column_name) %}
    case
        when {{ column_name }} > 0 then 'debit'
        when {{ column_name }} < 0 then 'credit'
        else 'zero'
    end
{% endmacro %}
