-- Create a standardized staging view for orders with normalized status and timestamps.
with source as (
    select * from {{ source('olist_raw', 'RAW_OLIST_ORDERS') }}
),

standardized as (
    select
        cast(order_id as varchar)                           as order_id,
        cast(customer_id as varchar)                        as customer_id,
        trim(lower(order_status))                           as order_status,
        cast(order_purchase_timestamp as timestamp_ntz)      as order_purchase_ts,
        cast(order_approved_at as timestamp_ntz)             as order_approved_ts,
        cast(order_delivered_carrier_date as timestamp_ntz)  as order_delivered_carrier_ts,
        cast(order_delivered_customer_date as timestamp_ntz) as order_delivered_customer_ts,
        cast(order_estimated_delivery_date as timestamp_ntz) as order_estimated_delivery_ts
    from source
)

select * from standardized