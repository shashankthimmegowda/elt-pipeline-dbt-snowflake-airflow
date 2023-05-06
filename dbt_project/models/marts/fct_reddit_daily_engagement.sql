{{
    config(
        materialized='table',
        unique_key=['subreddit', 'post_date']
    )
}}

with post_metrics as (
    select * from {{ ref('int_reddit_post_metrics') }}
),

daily_engagement as (
    select
        subreddit,
        post_date,
        day_of_week,

        -- Volume
        count(*) as total_posts,
        sum(case when is_self_post then 1 else 0 end) as self_posts,
        sum(case when not is_self_post then 1 else 0 end) as link_posts,

        -- Engagement
        sum(score) as total_score,
        avg(score) as avg_score,
        median(score) as median_score,
        max(score) as max_score,
        avg(upvote_ratio) as avg_upvote_ratio,

        -- Comments
        sum(num_comments) as total_comments,
        avg(num_comments) as avg_comments_per_post,
        avg(score_to_comment_ratio) as avg_score_to_comment_ratio,

        -- Awards
        sum(total_awards) as total_awards,

        -- Top post info
        max_by(title, score) as top_post_title,
        max_by(post_id, score) as top_post_id,

        -- Response time
        avg(minutes_to_first_comment) as avg_minutes_to_first_comment

    from post_metrics
    group by 1, 2, 3
)

select * from daily_engagement
