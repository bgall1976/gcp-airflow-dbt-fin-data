{% macro log_dbt_results(results) %}
    {%- if execute -%}
        {%- set ns = namespace(pass=0, fail=0, warn=0, error=0, skip=0) -%}
        {%- for result in results -%}
            {%- if result.status == 'pass' -%}
                {%- set ns.pass = ns.pass + 1 -%}
            {%- elif result.status == 'fail' -%}
                {%- set ns.fail = ns.fail + 1 -%}
            {%- elif result.status == 'warn' -%}
                {%- set ns.warn = ns.warn + 1 -%}
            {%- elif result.status == 'error' -%}
                {%- set ns.error = ns.error + 1 -%}
            {%- else -%}
                {%- set ns.skip = ns.skip + 1 -%}
            {%- endif -%}
        {%- endfor -%}
        {{ log("dbt run complete: " ~ ns.pass ~ " passed, " ~ ns.fail ~ " failed, "
               ~ ns.warn ~ " warnings, " ~ ns.error ~ " errors, "
               ~ ns.skip ~ " skipped", info=True) }}
    {%- endif -%}
{% endmacro %}
