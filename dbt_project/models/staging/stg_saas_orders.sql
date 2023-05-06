with source as (
    select raw_data, loaded_at
    from {{ source('raw_saas', 'ORDERS') }}
),

parsed as (
    select
        raw_data:id::integer                  as order_id,
        raw_data:user_id::integer             as user_id,
        raw_data:status::varchar              as status,
        raw_data:total_amount::float          as total_amount,
        raw_data:currency::varchar            as currency,
        raw_data:payment_method::varchar      as payment_method,
        raw_data:created_at::timestamp_tz     as created_at,
        raw_data:updated_at::timestamp_tz     as updated_at,
        loaded_at,
        row_number() over (
            partition by raw_data:id::integer
            order by loaded_at desc
        ) as _row_num
    from source
)

select * from parsed
where _row_num = 1
