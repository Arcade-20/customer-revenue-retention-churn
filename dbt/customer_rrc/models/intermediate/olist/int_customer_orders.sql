-- Intermediate customer orders model:
-- Rolls up orders + order-level revenue to customer_unique_id for retention/churn features.

with orders as (

    select
        customer_unique_id,
        order_id,
        order_purchased_at,
        is_delivered
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

orders_enriched as (

    select
        o.customer_unique_id,
        o.order_id,
        o.order_purchased_at,
        o.is_delivered,

        coalesce(r.order_gmv, 0)     as order_gmv,
        coalesce(r.payment_total, 0) as payment_total

    from orders o
    left join revenue r
        on o.order_id = r.order_id

),

final as (

    select
        customer_unique_id,

        min(order_purchased_at) as first_order_at,
        max(order_purchased_at) as last_order_at,

        count(*) as order_count,
        sum(case when is_delivered then 1 else 0 end) as delivered_order_count,

        sum(order_gmv) as lifetime_gmv,
        sum(payment_total) as lifetime_payments,

        datediff('day', min(order_purchased_at), max(order_purchased_at)) + 1 as customer_active_days

    from orders_enriched
    group by 1

)

select
    customer_unique_id,
    first_order_at,
    last_order_at,
    order_count,
    delivered_order_count,
    lifetime_gmv,
    lifetime_payments,
    customer_active_days
from final

