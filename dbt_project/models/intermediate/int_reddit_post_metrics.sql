{{
    config(
        materialized='ephemeral'
    )
}}

with posts as (
    select * from {{ ref('stg_reddit_posts') }}
),

comments as (
    select
        post_id,
        count(*) as extracted_comment_count,
        avg(score) as avg_comment_score,
        max(score) as max_comment_score,
        min(created_at) as first_comment_at,
        max(created_at) as last_comment_at
    from {{ ref('stg_reddit_comments') }}
    group by 1
),

enriched as (
    select
        p.post_id,
        p.subreddit,
        p.title,
        p.author,
        p.score,
        p.upvote_ratio,
        p.num_comments,
        p.created_at,
        p.is_self_post,
        p.flair_text,
        p.total_awards,

        -- Engagement metrics
        p.score * p.upvote_ratio as weighted_score,
        case
            when p.num_comments > 0
            then p.score::float / p.num_comments
            else 0
        end as score_to_comment_ratio,

        -- Comment enrichment
        coalesce(c.extracted_comment_count, 0) as extracted_comment_count,
        c.avg_comment_score,
        c.max_comment_score,
        c.first_comment_at,
        c.last_comment_at,
        datediff('minute', p.created_at, c.first_comment_at) as minutes_to_first_comment,

        -- Time dimensions
        date_trunc('day', p.created_at) as post_date,
        date_trunc('hour', p.created_at) as post_hour,
        dayofweek(p.created_at) as day_of_week,
        hour(p.created_at) as hour_of_day

    from posts p
    left join comments c on p.post_id = c.post_id
)

select * from enriched
