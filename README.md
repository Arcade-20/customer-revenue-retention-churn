# Customer Revenue, Retention & Churn Analytics  
Production-Style Analytics Engineering Project (Snowflake + dbt)

---

## 📖 Overview

This project builds a production-style analytics warehouse to analyze:

- Customer acquisition cohorts
- Retention behavior over time
- 90-day inactivity churn
- Monthly revenue trends
- Revenue split by churn segment
- Executive-level KPI summary

The project follows strict layered modeling discipline using dbt:

RAW → STAGING → INTERMEDIATE → MART

---

## 🧰 Tech Stack

- **Warehouse:** Snowflake  
- **Transformation Tool:** dbt (dbt-core 1.11.6)  
- **Adapter:** snowflake 1.11.2  
- **Version Control:** GitHub (milestone-based commits)  
- **Dataset:** Olist Brazilian E-Commerce (2016–2018)

---

## 🏗 Warehouse Architecture

### 🔹 RAW (Snowflake Schema: RAW)
- 1:1 loaded CSV tables
- No transformations
- Strict typing discipline (IDs as VARCHAR, timestamps as TIMESTAMP_NTZ)

Tables include:
- orders
- customers
- order_items
- payments
- products
- reviews
- sellers
- geolocation
- category translation

---

### 🔹 STAGING (Schema: STAGING)

Purpose:
- Standardize column names
- Light cleaning
- Deduplication where needed
- Basic data tests

Tests applied:
- not_null
- unique
- relationships
- accepted_values

---

### 🔹 INTERMEDIATE (Schema: INTERMEDIATE)

Business logic layer.

**Models:**

- `int_orders`  
  One row per order with lifecycle fields and delivery flags.

- `int_order_revenue`  
  Order-level GMV + payment totals + reconciliation difference.

- `int_customer_orders`  
  One row per customer:
  - first_order_at  
  - last_order_at  
  - order_count  
  - delivered_order_count  
  - lifetime_gmv  
  - lifetime_payments  
  - customer_active_days  

---

### 🔹 MART (Schema: MART)

Analytics-ready tables.

#### 1️⃣ mart_customer_cohorts
Assigns:
- cohort_month = month(first_order_at)

---

#### 2️⃣ mart_customer_retention
Cohort retention matrix:
- cohort_month
- months_since_cohort
- active_customers
- retention_rate

---

#### 3️⃣ mart_customer_churn
Deterministic churn logic:

- as_of_date = max(order date in dataset)
- days_since_last_order
- is_churned_90d (>= 90 days inactivity)
- churned_at = last_order_at + 90 days

---

#### 4️⃣ mart_monthly_revenue
Monthly KPIs:
- order_count
- active_customers
- total_gmv
- total_payments
- reconciliation difference

---

#### 5️⃣ mart_monthly_revenue_churn
Monthly revenue split:
- total_payments
- churned_customer_payments
- active_customer_payments

---

#### 6️⃣ mart_kpi_summary
Single-row executive summary including:
- total_customers
- active_customers
- churned_customers
- churn_rate_90d
- latest month revenue
- dataset date range

---

## 📊 Key Definitions

### Cohort Definition
cohort_month = date_trunc('month', first_order_at)


### Retention
retention_rate = active_customers / cohort_customers


### Churn (90-Day Inactivity)
as_of_date = max(order_purchased_at)
days_since_last_order = datediff(day, last_order_at, as_of_date)
is_churned_90d = days_since_last_order >= 90


This ensures reproducibility and deterministic churn labeling.

---

## 🧪 Testing Strategy

- not_null tests
- unique tests
- relationship tests
- business logic validation using `dbt_utils.expression_is_true`
- revenue reconciliation checks

All models pass dbt tests.

---

## ▶️ How to Run

### 1️⃣ Configure profiles.yml
Stored locally at:

This ensures reproducibility and deterministic churn labeling.

---

## 🧪 Testing Strategy

- not_null tests
- unique tests
- relationship tests
- business logic validation using `dbt_utils.expression_is_true`
- revenue reconciliation checks

All models pass dbt tests.

---

## ▶️ How to Run

### 1️⃣ Configure profiles.yml
Stored locally at:
~/.dbt/profiles.yml


Password sourced from environment variable:
export SNOWFLAKE_PASSWORD="xxxxxxxxxxxxxxxxxxxxxxxx"


---

### 2️⃣ Install packages
dbt deps


---

### 3️⃣ Build full project
dbt build


---

### 4️⃣ Generate documentation
dbt docs generate
dbt docs serve


---

## 📈 Next Phase

Visualization layer (Power BI):

Planned dashboards:
- Cohort retention heatmap
- Monthly revenue trend
- Churn rate analysis
- Active vs churned revenue split
- Executive KPI page

---

## 📊 Power BI Dashboard

The final reporting layer was built in Power BI (Import Mode) using the MART schema from Snowflake.

### Dashboard Pages:
1. Executive Summary
2. Revenue Analysis
3. Cohort Retention
4. Customer Value & Revenue Mix
5. Churn Deep Dive

### Key Features:
- 90-day churn definition
- Cohort-based retention matrix
- Revenue attribution (active vs eventually churned customers)
- Clean star-schema modeling (dim_cohort bridge)
- Measures-first design, no calculated relationship hacks

---

## 📸 Dashboard Preview

### Executive Summary
![Executive Summary](assets/executive_summary.png)

### Cohort Retention
![Cohort Retention](assets/cohort_retention.png)

### Customer Value & Revenue Mix
![Revenue Mix](assets/revenue_mix.png)

### Churn Deep Dive
![Churn Deep Dive](assets/churn_deep_dive.png)  

