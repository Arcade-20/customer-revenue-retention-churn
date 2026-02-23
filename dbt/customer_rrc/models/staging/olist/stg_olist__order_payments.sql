-- Staging model for Olist order payments: standardizes types/columns at the order-payment grain.
with source as (

    select
        order_id,
        payment_sequential,
        payment_type,
        payment_installments,
        payment_value
    from {{ source('olist', 'RAW_OLIST_ORDER_PAYMENTS') }}

),

cleaned as (

    select
        /* IDs */
        cast(order_id as varchar)                                         as order_id,

        /* Sequence / classification */
        cast(payment_sequential as number)                                as payment_sequential,
        lower(nullif(trim(payment_type), ''))                             as payment_type,

        /* Measures */
        cast(payment_installments as number)                              as payment_installments,
        cast(payment_value as number(18, 2))                              as payment_value

    from source

)

select * from cleaned