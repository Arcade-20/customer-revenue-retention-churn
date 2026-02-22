# Customer Revenue, Retention & Churn Analytics

End-to-end production-style analytics engineering project built using Snowflake, dbt, SQL, and GitHub.

---

## 🏗 Project Architecture

Warehouse: Snowflake  
Database: `CUSTOMER_ANALYTICS`

Schemas:
- `RAW` – 1:1 loaded source data (no transformations)
- `STAGING` – Standardized transformation layer (dbt-managed)
- `INTERMEDIATE` – Business logic models (planned)
- `MART` – Analytics-ready models for dashboards (planned)

---

## 📦 Dataset

Olist Brazilian E-Commerce Dataset (2016–2018)

Loaded 9 source tables into Snowflake RAW schema:

- RAW_OLIST_CUSTOMERS
- RAW_OLIST_ORDERS
- RAW_OLIST_ORDER_ITEMS
- RAW_OLIST_ORDER_PAYMENTS
- RAW_OLIST_ORDER_REVIEWS
- RAW_OLIST_PRODUCTS
- RAW_OLIST_SELLERS
- RAW_OLIST_GEOLOCATION
- RAW_PRODUCT_CATEGORY_TRANSLATION

---

## 🧱 Phase 1 — Warehouse Setup (Completed)

- Created Snowflake warehouse: `WH_ANALYTICS`
- Created database: `CUSTOMER_ANALYTICS`
- Created schemas: RAW, STAGING, INTERMEDIATE, MART
- Loaded all 9 CSV files into RAW schema
- Applied strict data typing:
  - IDs stored as VARCHAR
  - ZIP codes stored as VARCHAR
  - Timestamps stored as TIMESTAMP_NTZ
- Performed row-count validation checks

---

## 🛠 Phase 2 — dbt Initialization (Completed)

- Installed `dbt-snowflake` using pipx (Python 3.12)
- Initialized dbt project under `dbt/customer_rrc`
- Configured secure Snowflake profile
- Verified connection via `dbt debug`
- Defined RAW tables as dbt sources

---

## 🚀 Phase 3 — STAGING Layer (In Progress)

- Core staging models built:
  - stg_olist__customers
  - stg_olist__orders

- Added model-level tests:
  - Primary key uniqueness
  - Not-null constraints
  - Accepted value validation
  - Foreign key relationship between orders and customers

- All staging tests successfully validated using `dbt test`

---

## 🔜 Next Steps

- Complete remaining STAGING models
- Add model-level tests
- Build INTERMEDIATE customer metrics
- Implement retention and churn logic
- Create MART tables for reporting
- Add dashboard layer

