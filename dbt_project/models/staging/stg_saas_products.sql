with source as (
    select raw_data, loaded_at
    from {{ source('raw_saas', 'PRODUCTS') }}
),

parsed as (
    select
        raw_data:id::integer                  as product_id,
        raw_data:name::varchar                as product_name,
        raw_data:category::varchar            as category,
        raw_data:price::float                 as price,
        raw_data:is_active::boolean           as is_active,
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
