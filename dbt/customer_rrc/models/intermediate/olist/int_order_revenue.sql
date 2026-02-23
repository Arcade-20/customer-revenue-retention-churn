-- Intermediate order revenue model: aggregates items and payments to order-level totals with reconciliation fields.
with order_items as (

    select
        order_id,
        item_price,
        item_freight_value
    from {{ ref('stg_olist__order_items') }}

),

items_agg as (

    select
        order_id,
        sum(item_price)            as items_subtotal,
        sum(item_freight_value)    as freight_total,
        count(*)                   as item_count
    from order_items
    group by 1

),

payments as (

    select
        order_id,
        payment_value
    from {{ ref('stg_olist__order_payments') }}

),

payments_agg as (

    select
        order_id,
        sum(payment_value)         as payment_total
    from payments
    group by 1

),

final as (

    select
        o.order_id,

        coalesce(i.items_subtotal, 0)                         as items_subtotal,
        coalesce(i.freight_total, 0)                          as freight_total,
        coalesce(i.items_subtotal, 0) + coalesce(i.freight_total, 0) as order_gmv,

        coalesce(p.payment_total, 0)                          as payment_total,
        coalesce(i.item_count, 0)                             as item_count,

        /* Reconciliation */
        (coalesce(p.payment_total, 0) - (coalesce(i.items_subtotal, 0) + coalesce(i.freight_total, 0))) as payment_minus_gmv

    from {{ ref('int_orders') }} o
    left join items_agg i
        on o.order_id = i.order_id
    left join payments_agg p
        on o.order_id = p.order_id

)

select * from final