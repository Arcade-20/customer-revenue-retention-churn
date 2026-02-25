-- MART monthly revenue by churn segment:
-- Splits monthly revenue into churned vs active customers based on mart_customer_churn.

with churn as (

    select
        customer_unique_id,
        is_churned_90d
    from {{ ref('mart_customer_churn') }}

),

orders as (

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
        coalesce(order_gmv, 0) as order_gmv,
        coalesce(payment_total, 0) as payment_total
    from {{ ref('int_order_revenue') }}

),

facts as (

    select
        o.order_month,
        o.order_id,
        o.customer_unique_id,
        c.is_churned_90d,
        r.order_gmv,
        r.payment_total
    from orders o
    left join revenue r
        on o.order_id = r.order_id
    left join churn c
        on o.customer_unique_id = c.customer_unique_id

),

final as (

    select
        order_month,

        sum(payment_total) as total_payments,

        sum(case when is_churned_90d then payment_total else 0 end) as churned_customer_payments,
        sum(case when not is_churned_90d then payment_total else 0 end) as active_customer_payments

    from facts
    group by 1

)

select
    order_month,
    total_payments,
    churned_customer_payments,
    active_customer_payments
from final
order by order_month
