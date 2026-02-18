with source as (

    select * from {{ source('netsuite', 'transactions') }}

),

renamed as (

    select
        cast(id as string)                                  as transaction_id,
        cast(tran_id as string)                             as transaction_number,
        cast(type as string)                                as transaction_type,
        cast(status as string)                              as transaction_status,
        cast(entity as string)                              as entity_id,
        cast(subsidiary as string)                          as subsidiary_id,
        cast(department as string)                          as department_id,
        cast(currency as string)                            as currency_id,
        cast(exchange_rate as numeric)                      as exchange_rate,
        cast(total as numeric)                              as total_amount,
        cast(total as numeric) * cast(exchange_rate as numeric)
                                                            as total_amount_base,
        cast(tran_date as date)                             as transaction_date,
        cast(due_date as date)                              as due_date,
        cast(posting as boolean)                            as is_posting,
        cast(voided as boolean)                             as is_voided,
        cast(memo as string)                                as memo,
        timestamp(date_created)                             as created_at,
        timestamp(last_modified_date)                       as updated_at,
        _loaded_at,
        'netsuite'                                          as source_system

    from source
    where cast(voided as boolean) = false

)

select * from renamed
