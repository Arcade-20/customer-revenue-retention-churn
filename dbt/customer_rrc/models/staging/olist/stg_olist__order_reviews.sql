-- Staging model for Olist order reviews: cleans types and standardizes columns at the review grain.
with source as (

    select
        review_id,
        order_id,
        review_score,
        review_comment_title,
        review_comment_message,
        review_creation_date,
        review_answer_timestamp
    from {{ source('olist', 'RAW_OLIST_ORDER_REVIEWS') }}

),

cleaned as (

    select
        /* IDs */
        cast(review_id as varchar)                                       as review_id,
        cast(order_id as varchar)                                        as order_id,

        /* Review metrics */
        cast(review_score as number)                                     as review_score,

        /* Free-text (trim empty strings to null) */
        nullif(trim(review_comment_title), '')                           as review_comment_title,
        nullif(trim(review_comment_message), '')                         as review_comment_message,

        /* Timestamps */
        cast(review_creation_date as timestamp_ntz)                      as review_created_at,
        cast(review_answer_timestamp as timestamp_ntz)                   as review_answered_at

    from source

)

select * from cleaned