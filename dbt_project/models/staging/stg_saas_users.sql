with source as (
    select raw_data, loaded_at
    from {{ source('raw_saas', 'USERS') }}
),

parsed as (
    select
        raw_data:id::integer                  as user_id,
        raw_data:email::varchar               as email,
        raw_data:username::varchar            as username,
        raw_data:full_name::varchar           as full_name,
        raw_data:plan_type::varchar           as plan_type,
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
