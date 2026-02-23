-- Staging model for Olist geolocation: cleans fields and deterministically deduplicates to 1 row per zip_code_prefix.
with source as (

    select
        geolocation_zip_code_prefix,
        geolocation_lat,
        geolocation_lng,
        geolocation_city,
        geolocation_state
    from {{ source('olist', 'RAW_OLIST_GEOLOCATION') }}

),

cleaned as (

    select
        /* Zip prefix (join key for customers/sellers) */
        cast(geolocation_zip_code_prefix as varchar)                   as zip_code_prefix,

        /* Coordinates */
        cast(geolocation_lat as float)                                 as latitude,
        cast(geolocation_lng as float)                                 as longitude,

        /* Location */
        lower(nullif(trim(geolocation_city), ''))                      as city,
        upper(nullif(trim(geolocation_state), ''))                     as state

    from source
    where geolocation_zip_code_prefix is not null

),

latlng_by_zip as (

    select
        zip_code_prefix,
        avg(latitude)  as latitude_avg,
        avg(longitude) as longitude_avg,
        count(*)       as raw_row_count
    from cleaned
    group by 1

),

city_state_counts as (

    select
        zip_code_prefix,
        city,
        state,
        count(*) as cnt
    from cleaned
    group by 1, 2, 3

),

mode_city_state as (

    select
        zip_code_prefix,
        city,
        state
    from city_state_counts
    qualify row_number() over (
        partition by zip_code_prefix
        order by cnt desc, city asc, state asc
    ) = 1

)

select
    z.zip_code_prefix,
    m.city                                        as geolocation_city,
    m.state                                       as geolocation_state,
    z.latitude_avg                                as geolocation_latitude,
    z.longitude_avg                               as geolocation_longitude,
    z.raw_row_count                               as geolocation_raw_row_count
from latlng_by_zip z
left join mode_city_state m
    on z.zip_code_prefix = m.zip_code_prefix