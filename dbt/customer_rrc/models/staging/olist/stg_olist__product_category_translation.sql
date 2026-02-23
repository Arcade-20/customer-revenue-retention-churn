-- Staging model for product category translation: renames generic RAW columns (C1, C2) and standardizes text.
with source as (

    select
        C1 as product_category_name,
        C2 as product_category_name_english
    from {{ source('olist', 'RAW_PRODUCT_CATEGORY_TRANSLATION') }}

),

cleaned as (

    select
        /* Portuguese category key */
        lower(nullif(trim(product_category_name), ''))      as product_category_name,

        /* English translation */
        lower(nullif(trim(product_category_name_english), '')) 
                                                            as product_category_name_english

    from source

)

select * from cleaned