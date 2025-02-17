{{
    config(
        materialized='table',
        unique_key='order_month'
    )
}}

with orders as (
    select * from {{ ref('int_saas_order_enriched') }}
    where status not in ('cancelled', 'refunded')
),

monthly_revenue as (
    select
        order_month,

        -- Volume
        count(distinct order_id) as total_orders,
        count(distinct user_id) as unique_customers,

        -- Revenue
        sum(total_amount) as gross_revenue,
        avg(total_amount) as avg_order_value,
        median(total_amount) as median_order_value,

        -- Revenue by plan
        sum(case when plan_type = 'free' then total_amount else 0 end) as revenue_free_users,
        sum(case when plan_type = 'starter' then total_amount else 0 end) as revenue_starter,
        sum(case when plan_type = 'pro' then total_amount else 0 end) as revenue_pro,
        sum(case when plan_type = 'enterprise' then total_amount else 0 end) as revenue_enterprise,

        -- Customer lifecycle
        count(case when customer_lifecycle_stage = 'first_week' then 1 end) as new_customer_orders,
        count(case when user_order_number = 1 then 1 end) as first_time_orders,
        count(case when user_order_number > 1 then 1 end) as repeat_orders,

        -- Repeat rate
        case
            when count(distinct user_id) > 0
            then count(case when user_order_number > 1 then 1 end)::float
                 / count(distinct user_id)
            else 0
        end as repeat_order_rate,

        -- Payment methods
        count(case when payment_method = 'credit_card' then 1 end) as cc_orders,
        count(case when payment_method = 'paypal' then 1 end) as paypal_orders

    from orders
    group by 1
)

select
    *,
    -- Month-over-month growth
    lag(gross_revenue) over (order by order_month) as prev_month_revenue,
    case
        when lag(gross_revenue) over (order by order_month) > 0
        then (gross_revenue - lag(gross_revenue) over (order by order_month))
             / lag(gross_revenue) over (order by order_month) * 100
        else null
    end as revenue_growth_pct

from monthly_revenue
