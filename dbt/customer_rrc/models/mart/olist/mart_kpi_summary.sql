-- MART KPI summary:
-- One-row executive summary table for dashboarding and README highlights.

with churn as (

    select
        customer_unique_id,
        is_churned_90d,
        as_of_date
    from {{ ref('mart_customer_churn') }}

),

monthly as (

    select *
    from {{ ref('mart_monthly_revenue') }}

),

monthly_churn as (

    select *
    from {{ ref('mart_monthly_revenue_churn') }}

),

as_of as (

    select max(as_of_date) as as_of_date
    from churn

),

customer_counts as (

    select
        count(*) as total_customers,
        sum(case when is_churned_90d then 1 else 0 end) as churned_customers,
        sum(case when not is_churned_90d then 1 else 0 end) as active_customers
    from churn

),

date_bounds as (

    select
        min(order_month) as first_order_month,
        max(order_month) as last_order_month
    from monthly

),

latest_month as (

    select
        max(order_month) as latest_order_month
    from monthly

),

latest_month_metrics as (

    select
        m.order_month,
        m.order_count,
        m.active_customers as active_customers_in_month,
        m.total_gmv,
        m.total_payments,
        m.payment_minus_gmv
    from monthly m
    inner join latest_month lm
        on m.order_month = lm.latest_order_month

),

latest_month_churn_split as (

    select
        mc.order_month,
        mc.total_payments as latest_total_payments,
        mc.churned_customer_payments,
        mc.active_customer_payments
    from monthly_churn mc
    inner join latest_month lm
        on mc.order_month = lm.latest_order_month

)

select
    a.as_of_date,

    d.first_order_month,
    d.last_order_month,

    c.total_customers,
    c.active_customers,
    c.churned_customers,

    c.churned_customers / nullif(c.total_customers, 0) as churn_rate_90d,

    lmm.order_month as latest_order_month,
    lmm.order_count as latest_month_order_count,
    lmm.active_customers_in_month,
    lmm.total_gmv as latest_month_gmv,
    lmm.total_payments as latest_month_payments,
    lmm.payment_minus_gmv as latest_month_payment_minus_gmv,

    lmcs.churned_customer_payments as latest_month_churned_customer_payments,
    lmcs.active_customer_payments as latest_month_active_customer_payments

from as_of a
cross join customer_counts c
cross join date_bounds d
cross join latest_month_metrics lmm
cross join latest_month_churn_split lmcs
