# Warehouse Architecture

This project follows a layered analytics engineering design:

## Layers
- RAW: 1:1 copies of source CSVs (no business logic)
- STAGING: cleaned and standardized columns, types, keys
- INTERMEDIATE: reusable business building blocks (order revenue, customer-order spine)
- MART: final analytics-ready tables for finance, retention, churn, segmentation

## Snowflake Setup (planned)
- Warehouse: WH_ANALYTICS (XSMALL, auto-suspend)
- Database: CUSTOMER_ANALYTICS
- Schemas: RAW, STAGING, INTERMEDIATE, MART

## Development Approach
- dbt models are the source of truth for transformations
- notebooks are used only for profiling/validation (not final logic)