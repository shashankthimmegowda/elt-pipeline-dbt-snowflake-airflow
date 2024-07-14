with source as (
    select
        raw_data,
        loaded_at,
        batch_id
    from {{ source('raw_reddit', 'POSTS') }}
),

parsed as (
    select
        raw_data:post_id::varchar             as post_id,
        raw_data:subreddit::varchar           as subreddit,
        raw_data:title::varchar               as title,
        raw_data:selftext::varchar            as selftext,
        raw_data:author::varchar              as author,
        raw_data:score::integer               as score,
        raw_data:upvote_ratio::float          as upvote_ratio,
        raw_data:num_comments::integer        as num_comments,
        raw_data:created_utc::timestamp_tz    as created_at,
        raw_data:url::varchar                 as url,
        raw_data:permalink::varchar           as permalink,
        raw_data:is_self::boolean             as is_self_post,
        raw_data:link_flair_text::varchar     as flair_text,
        raw_data:over_18::boolean             as is_nsfw,
        raw_data:spoiler::boolean             as is_spoiler,
        raw_data:stickied::boolean            as is_stickied,
        raw_data:total_awards_received::int   as total_awards,
        raw_data:extracted_at::timestamp_tz   as extracted_at,
        loaded_at,
        batch_id,
        row_number() over (
            partition by raw_data:post_id::varchar
            order by loaded_at desc
        ) as _row_num
    from source
)

select * from parsed
where _row_num = 1
