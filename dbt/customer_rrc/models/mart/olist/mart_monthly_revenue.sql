-- MART monthly revenue:
-- Aggregates orders + revenue to calendar month grain for KPI reporting.

with orders as (

    select
        order_id,
        customer_unique_id,
        date_trunc('month', order_purchased_at) as order_month
    from {{ ref('int_orders') }}
    where customer_unique_id is not null

),

revenue as (

    select
        order_id,
        order_gmv,
        payment_total
    from {{ ref('int_order_revenue') }}

),

order_month_facts as (

    select
        o.order_month,
        o.order_id,
        o.customer_unique_id,
        coalesce(r.order_gmv, 0) as order_gmv,
        coalesce(r.payment_total, 0) as payment_total
    from orders o
    left join revenue r
        on o.order_id = r.order_id

),

final as (

    select
        order_month,

        count(distinct order_id) as order_count,
        count(distinct customer_unique_id) as active_customers,

        sum(order_gmv) as total_gmv,
        sum(payment_total) as total_payments,

        sum(payment_total) - sum(order_gmv) as payment_minus_gmv

    from order_month_facts
    group by 1

)

select
    order_month,
    order_count,
    active_customers,
    total_gmv,
    total_payments,
    payment_minus_gmv
from final
order by order_month
