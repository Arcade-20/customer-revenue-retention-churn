# Data Load Log (Olist → Snowflake RAW)

Loaded Olist CSVs into Snowflake `CUSTOMER_ANALYTICS.RAW` as 1:1 raw tables using Snowsight "Upload local files".

## RAW tables loaded
- RAW_OLIST_CUSTOMERS
- RAW_OLIST_ORDERS
- RAW_OLIST_ORDER_ITEMS
- RAW_OLIST_ORDER_PAYMENTS
- RAW_OLIST_ORDER_REVIEWS
- RAW_OLIST_PRODUCTS
- RAW_OLIST_SELLERS
- RAW_OLIST_GEOLOCATION
- RAW_PRODUCT_CATEGORY_TRANSLATION

## Notes
- ZIP code prefixes stored as VARCHAR to preserve leading zeros.
- RAW layer contains no transformations; cleaning and business logic will start in STAGING (dbt).