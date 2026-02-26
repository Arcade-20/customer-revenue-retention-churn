# Data Quality Report (Post-dbt Validation)

**Generated (UTC):** 2026-02-26 19:24:27
**Snowflake DB:** `CUSTOMER_ANALYTICS`  
**MART schema:** `MART`  
**Warehouse:** `WH_ANALYTICS`  

## Summary
- PASS: **11**
- FAIL: **0**
- WARN: **1**

## Checks

| Status | Check | Details |
|---|---|---|
| PASS | CUSTOMER_ANALYTICS.MART.MART_KPI_SUMMARY exists | Table is queryable. |
| PASS | CUSTOMER_ANALYTICS.MART.MART_MONTHLY_REVENUE exists | Table is queryable. |
| PASS | CUSTOMER_ANALYTICS.MART.MART_MONTHLY_REVENUE_CHURN exists | Table is queryable. |
| PASS | CUSTOMER_ANALYTICS.MART.MART_CUSTOMER_CHURN exists | Table is queryable. |
| PASS | CUSTOMER_ANALYTICS.MART.MART_CUSTOMER_RETENTION exists | Table is queryable. |
| PASS | mart_kpi_summary has 1 row | Row count = 1 |
| PASS | mart_kpi_summary KPI consistency | KPI relationships look consistent. |
| PASS | mart_monthly_revenue non-negative totals | No negative totals found. |
| WARN | mart_monthly_revenue reconciliation diff | No recon/diff column found. Columns: ['ORDER_MONTH', 'ORDER_COUNT', 'ACTIVE_CUSTOMERS', 'TOTAL_GMV', 'TOTAL_PAYMENTS', 'PAYMENT_MINUS_GMV'] |
| PASS | mart_monthly_revenue_churn split reconciliation | Split sums + non-negative checks pass (tol=1.0). |
| PASS | mart_customer_churn churn flag logic | Churn flag aligns with days-since logic. |
| PASS | mart_customer_retention retention_rate bounds | All retention_rate values within [0, 1]. |
