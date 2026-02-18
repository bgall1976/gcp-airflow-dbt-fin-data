{% snapshot snap_dim_customers %}

{{
    config(
        target_schema='snapshots',
        unique_key='customer_key',
        strategy='timestamp',
        updated_at='_dim_updated_at',
        invalidate_hard_deletes=true
    )
}}

select * from {{ ref('dim_customers') }}

{% endsnapshot %}
