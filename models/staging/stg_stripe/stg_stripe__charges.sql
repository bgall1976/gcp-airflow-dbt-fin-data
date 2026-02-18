with source as (

    select * from {{ source('stripe', 'charges') }}

),

renamed as (

    select
        cast(id as string)                                  as charge_id,
        cast(customer as string)                            as stripe_customer_id,
        cast(amount as numeric) / 100                       as charge_amount,
        cast(amount_refunded as numeric) / 100              as refunded_amount,
        upper(currency)                                     as currency_code,
        cast(status as string)                              as charge_status,
        cast(paid as boolean)                               as is_paid,
        cast(refunded as boolean)                           as is_refunded,
        cast(disputed as boolean)                           as is_disputed,
        cast(payment_method as string)                      as payment_method_id,
        json_extract_scalar(payment_method_details, '$.type')
                                                            as payment_method_type,
        cast(description as string)                         as description,
        cast(invoice as string)                             as stripe_invoice_id,
        json_extract_scalar(metadata, '$.order_id')         as external_order_id,
        json_extract_scalar(metadata, '$.customer_email')   as customer_email,
        timestamp_seconds(cast(created as int64))           as created_at,
        _loaded_at,
        'stripe'                                            as source_system

    from source

)

select * from renamed
