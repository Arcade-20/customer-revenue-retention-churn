

## Customer Revenue, Retention & Churn Analytics (Olist)

Production-style analytics engineering project using the Olist Brazilian e-commerce marketplace dataset (2016–2018).
This repo builds a warehouse-style modeling layer (Raw → Staging → Intermediate → Marts) to support revenue analytics,
retention/cohort analysis, engineered churn definitions, reactivation logic, and customer segmentation (RFM).

## Why this project exists
Most “churn projects” are toy datasets with pre-labeled churn. In real businesses, churn is almost never labeled.
This project engineers churn using inactivity rules and validates how churn rates change under different definitions.

## Dataset
**Olist Brazilian E-Commerce Dataset (2016–2018)** (multi-table transactional marketplace data):
- customers
- orders
- order_items
- payments
- products
- sellers
- reviews

## What this project will deliver (business outcomes)

### Revenue analytics
- Gross revenue
- Net revenue (exclude canceled/unavailable orders)
- Revenue per customer
- Revenue per cohort (monthly acquisition cohorts)
- AOV (Average Order Value)
- Repeat revenue share (revenue from repeat customers vs first-time)

### Retention analytics
- First purchase identification per customer
- Monthly acquisition cohorts
- Cohort retention curves (customer-level and order-level)
- Repeat purchase rate
- Time between purchases (distribution + median by cohort/segment)

### Churn (engineered, not pre-labeled)
Churn is defined using inactivity windows:
- 60 / 90 / 120-day inactivity-based churn
- Comparison of churn rates under each definition
- Reactivation logic (customer returns after being “churned”)
- Time-to-reactivation
- Revenue from reactivated customers

### Customer segmentation
- RFM scoring (Recency, Frequency, Monetary)
- Revenue contribution by segment
- Churn rate by segment
- Segment movement over time (planned)

## Why Olist fully supports these analyses
Olist is multi-table transactional data with timestamps and customer identifiers that enable:
- Revenue: `order_items.price + freight_value` with order status filters for net revenue
- Customer-level behaviors: repeat purchases via customer unique id across orders
- Cohorts: first purchase month derived from earliest delivered/approved order per customer
- Churn engineering: inactivity gaps computed from each customer’s last purchase date
- Reactivation: customers returning after crossing churn window
- RFM: recency (days since last purchase), frequency (# orders), monetary (net revenue)

Churn is not labeled — and that is intentional. This mirrors how analytics teams define churn differently across industries.

## Tech stack (locked)
- SQL
- Python
- Snowflake (warehouse)
- dbt (modeling + tests)
- Git + GitHub (clean history, documented decisions)

> Visualization layer (Power BI) will be added later and will not block warehouse + metric development.

## Repository structure (high level)
- `docs/` — metric definitions, churn decision log, architecture notes
- `dbt/` — staging/intermediate/marts models, tests, macros
- `sql/` — ad-hoc analysis and validation queries
- `notebooks/` — profiling and exploration (kept minimal; source of truth is dbt models)
- `src/` — Python utilities for data quality and reproducible workflows
- `dashboards/` — BI artifacts (added later)

## Project standards (what “production-style” means here)
- Warehouse-style layered models: Raw → Staging → Intermediate → Marts
- Explicit metric definitions and business logic in `/docs/metrics`
- dbt tests for data quality (uniqueness, not nulls, referential integrity, accepted values)
- Reproducible development workflow and clean commits

## Roadmap
- Day 1: Repo setup + structure + project README (this commit)
- Day 2+: Data acquisition + Snowflake loading plan + dbt project initialization (next steps)

