-- Staging model for Olist order items: cleans types + standardizes column names at the order-item grain (order_id, order_item_id).
with source as (

    select
        order_id,
        order_item_id,
        product_id,
        seller_id,
        shipping_limit_date,
        price,
        freight_value
    from {{ source('olist', 'RAW_OLIST_ORDER_ITEMS') }}

),

renamed as (

    select
        /* IDs */
        cast(order_id as varchar)                        as order_id,
        cast(order_item_id as number)                    as order_item_id,
        cast(product_id as varchar)                      as product_id,
        cast(seller_id as varchar)                       as seller_id,

        /* Timestamps */
        cast(shipping_limit_date as timestamp_ntz)       as shipping_limit_at,

        /* Measures */
        cast(price as number(18, 2))                     as item_price,
        cast(freight_value as number(18, 2))             as item_freight_value

    from source

)

select * from renamed