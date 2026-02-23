-- Staging model for Olist sellers: cleans IDs and normalizes seller location fields.
with source as (

    select
        seller_id,
        seller_zip_code_prefix,
        seller_city,
        seller_state
    from {{ source('olist', 'RAW_OLIST_SELLERS') }}

),

cleaned as (

    select
        /* IDs */
        cast(seller_id as varchar)                                     as seller_id,
        cast(seller_zip_code_prefix as varchar)                        as seller_zip_code_prefix,

        /* Location */
        lower(nullif(trim(seller_city), ''))                           as seller_city,
        upper(nullif(trim(seller_state), ''))                          as seller_state

    from source

)

select * from cleaned