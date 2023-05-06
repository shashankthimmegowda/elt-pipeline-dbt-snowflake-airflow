{{
    config(
        materialized='table'
    )
}}

with post_metrics as (
    select * from {{ ref('int_reddit_post_metrics') }}
),

subreddit_stats as (
    select
        subreddit,

        -- All-time stats
        count(*) as total_posts_tracked,
        count(distinct author) as unique_authors,
        sum(score) as total_score,
        avg(score) as avg_post_score,
        sum(num_comments) as total_comments,
        avg(num_comments) as avg_comments_per_post,
        avg(upvote_ratio) as avg_upvote_ratio,

        -- Post type breakdown
        sum(case when is_self_post then 1 else 0 end)::float / count(*) as self_post_ratio,

        -- Activity window
        min(created_at) as first_post_tracked_at,
        max(created_at) as last_post_tracked_at,
        datediff('day', min(created_at), max(created_at)) as tracking_days,

        -- Engagement tiers
        count(case when score >= 100 then 1 end) as high_engagement_posts,
        count(case when score >= 10 and score < 100 then 1 end) as medium_engagement_posts,
        count(case when score < 10 then 1 end) as low_engagement_posts

    from post_metrics
    group by 1
)

select * from subreddit_stats
