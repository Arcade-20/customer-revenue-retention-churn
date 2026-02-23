-- MART customer churn:
-- Labels churn using a deterministic as_of_date anchor (max order_purchased_at in dataset)
-- and a 90-day inactivity definition.

with customer_base as (

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
    from {{ ref('mart_customer_cohorts') }}

),

as_of as (

    select
        max(order_purchased_at) as as_of_date
    from {{ ref('int_orders') }}

),

final as (

    select
        b.customer_unique_id,
        b.cohort_month,
        b.first_order_at,
        b.last_order_at,

        a.as_of_date,

        datediff('day', b.last_order_at, a.as_of_date) as days_since_last_order,

        dateadd('day', 90, b.last_order_at) as churned_at,

        case
            when datediff('day', b.last_order_at, a.as_of_date) >= 90 then true
            else false
        end as is_churned_90d,

        b.order_count,
        b.delivered_order_count,
        b.lifetime_gmv,
        b.lifetime_payments,
        b.customer_active_days

    from customer_base b
    cross join as_of a

)

select
    customer_unique_id,
    cohort_month,
    first_order_at,
    last_order_at,
    as_of_date,
    days_since_last_order,
    churned_at,
    is_churned_90d,
    order_count,
    delivered_order_count,
    lifetime_gmv,
    lifetime_payments,
    customer_active_days
from final
