-- Intermediate orders model: one row per order with customer lifetime key and lifecycle metrics for downstream revenue/retention marts.
with orders as (

    select
        order_id,
        customer_id,
        order_status,
        order_purchase_ts,
        order_approved_ts,
        order_delivered_carrier_ts,
        order_delivered_customer_ts,
        order_estimated_delivery_ts
    from {{ ref('stg_olist__orders') }}

),

customers as (

    select
        customer_id,
        customer_unique_id
    from {{ ref('stg_olist__customers') }}

),

joined as (

    select
        o.order_id,
        o.customer_id,
        c.customer_unique_id,

        o.order_status,

        o.order_purchase_ts              as order_purchased_at,
        o.order_approved_ts              as order_approved_at,
        o.order_delivered_carrier_ts     as order_delivered_to_carrier_at,
        o.order_delivered_customer_ts    as order_delivered_to_customer_at,
        o.order_estimated_delivery_ts    as order_estimated_delivery_at

    from orders o
    left join customers c
        on o.customer_id = c.customer_id

),

final as (

    select
        order_id,
        customer_id,
        customer_unique_id,

        order_status,

        order_purchased_at,
        order_approved_at,
        order_delivered_to_carrier_at,
        order_delivered_to_customer_at,
        order_estimated_delivery_at,

        /* Flags */
        case when order_delivered_to_customer_at is not null then true else false end as is_delivered,

        /* Lifecycle metrics */
        datediff('day', order_purchased_at, order_delivered_to_customer_at)           as days_purchase_to_delivery,
        datediff('day', order_purchased_at, order_delivered_to_carrier_at)            as days_purchase_to_carrier,

        /* Delivery performance (positive = delivered late, negative = delivered early) */
        datediff('day', order_estimated_delivery_at, order_delivered_to_customer_at)  as days_from_estimated_delivery

    from joined

)

select * from final