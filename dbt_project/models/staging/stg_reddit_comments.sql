with source as (
    select
        raw_data,
        loaded_at,
        batch_id
    from {{ source('raw_reddit', 'COMMENTS') }}
),

parsed as (
    select
        raw_data:comment_id::varchar          as comment_id,
        raw_data:post_id::varchar             as post_id,
        raw_data:author::varchar              as author,
        raw_data:body::varchar                as body,
        raw_data:score::integer               as score,
        raw_data:created_utc::timestamp_tz    as created_at,
        raw_data:is_submitter::boolean        as is_submitter,
        raw_data:parent_id::varchar           as parent_id,
        raw_data:extracted_at::timestamp_tz   as extracted_at,
        loaded_at,
        batch_id,
        row_number() over (
            partition by raw_data:comment_id::varchar
            order by loaded_at desc
        ) as _row_num
    from source
)

select * from parsed
where _row_num = 1
