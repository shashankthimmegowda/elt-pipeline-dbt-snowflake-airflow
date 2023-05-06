{{
    config(
        materialized='ephemeral'
    )
}}

with orders as (
    select * from {{ ref('stg_saas_orders') }}
),

users as (
    select * from {{ ref('stg_saas_users') }}
),

products as (
    select * from {{ ref('stg_saas_products') }}
),

enriched as (
    select
        o.order_id,
        o.user_id,
        o.status,
        o.total_amount,
        o.currency,
        o.payment_method,
        o.created_at as order_created_at,

        -- User info
        u.username,
        u.plan_type,
        u.is_active as user_is_active,
        u.created_at as user_created_at,

        -- Derived
        datediff('day', u.created_at, o.created_at) as days_since_signup,
        case
            when datediff('day', u.created_at, o.created_at) <= 7 then 'first_week'
            when datediff('day', u.created_at, o.created_at) <= 30 then 'first_month'
            when datediff('day', u.created_at, o.created_at) <= 90 then 'first_quarter'
            else 'established'
        end as customer_lifecycle_stage,

        -- Order ranking
        row_number() over (
            partition by o.user_id
            order by o.created_at
        ) as user_order_number,

        -- Time dimensions
        date_trunc('day', o.created_at) as order_date,
        date_trunc('month', o.created_at) as order_month

    from orders o
    left join users u on o.user_id = u.user_id
)

select * from enriched
