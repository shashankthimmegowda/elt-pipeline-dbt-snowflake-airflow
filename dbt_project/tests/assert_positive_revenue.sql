-- Custom test: ensure no negative revenue in marts
select
    order_month,
    gross_revenue
from {{ ref('fct_saas_revenue') }}
where gross_revenue < 0
