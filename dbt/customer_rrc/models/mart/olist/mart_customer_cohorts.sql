-- MART customer cohorts:
-- Assigns each customer to a cohort_month (month of first order) for retention analysis.

with customer_orders as (

    select
        customer_unique_id,
        first_order_at,
        last_order_at,
        order_count,
        delivered_order_count,
        lifetime_gmv,
        lifetime_payments,
        customer_active_days
    from {{ ref('int_customer_orders') }}

),

final as (

    select
        customer_unique_id,

        date_trunc('month', first_order_at) as cohort_month,

        first_order_at,
        last_order_at,

        order_count,
        delivered_order_count,

        lifetime_gmv,
        lifetime_payments,
        customer_active_days

    from customer_orders

)

select
    customer_unique_id,
    cohort_month,
    first_order_at,
    last_order_at,
    order_count,
    delivered_order_count,
    lifetime_gmv,
    lifetime_payments,
    customer_active_days
from final
