-- Create a standardized staging view for customers with clean IDs and normalized location fields.
with source as (
    select * from {{ source('olist', 'RAW_OLIST_CUSTOMERS') }}
),

standardized as (
    select
        cast(customer_id as varchar)              as customer_id,
        cast(customer_unique_id as varchar)       as customer_unique_id,
        cast(customer_zip_code_prefix as varchar) as customer_zip_code_prefix,
        trim(lower(customer_city))                as customer_city,
        trim(upper(customer_state))               as customer_state
    from source
)

select * from standardized