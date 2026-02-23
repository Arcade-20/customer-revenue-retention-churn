-- Staging model for Olist products: standardizes product attributes and cleans text fields.
with source as (

    select
        product_id,
        product_category_name,
        product_name_lenght,
        product_description_lenght,
        product_photos_qty,
        product_weight_g,
        product_length_cm,
        product_height_cm,
        product_width_cm
    from {{ source('olist', 'RAW_OLIST_PRODUCTS') }}

),

cleaned as (

    select
        /* IDs */
        cast(product_id as varchar)                                         as product_id,

        /* Category (raw Portuguese category; translation handled separately) */
        lower(nullif(trim(product_category_name), ''))                      as product_category_name,

        /* Text lengths / counts */
        cast(product_name_lenght as number)                                  as product_name_length,
        cast(product_description_lenght as number)                           as product_description_length,
        cast(product_photos_qty as number)                                   as product_photos_qty,

        /* Physical attributes */
        cast(product_weight_g as number)                                     as product_weight_g,
        cast(product_length_cm as number)                                    as product_length_cm,
        cast(product_height_cm as number)                                    as product_height_cm,
        cast(product_width_cm as number)                                     as product_width_cm

    from source

)

select * from cleaned