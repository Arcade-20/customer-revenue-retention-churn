-- MART customer retention:
-- Cohort retention matrix based on monthly customer order activity.

with cohorts as (

    select
        customer_unique_id,
        cohort_month
    from {{ ref('mart_customer_cohorts') }}

),

customer_month_activity as (

    select distinct
        customer_unique_id,
        date_trunc('month', order_purchased_at) as activity_month
    from {{ ref('int_orders') }}
    where customer_unique_id is not null

),

cohort_activity as (

    select
        c.cohort_month,
        a.activity_month,
        datediff('month', c.cohort_month, a.activity_month) as months_since_cohort,
        a.customer_unique_id
    from cohorts c
    inner join customer_month_activity a
        on c.customer_unique_id = a.customer_unique_id
    where a.activity_month >= c.cohort_month

),

cohort_sizes as (

    select
        cohort_month,
        count(*) as cohort_customers
    from cohorts
    group by 1

),

retention as (

    select
        cohort_month,
        activity_month,
        months_since_cohort,
        count(distinct customer_unique_id) as active_customers
    from cohort_activity
    group by 1, 2, 3

)

select
    r.cohort_month,
    r.activity_month,
    r.months_since_cohort,

    s.cohort_customers,
    r.active_customers,

    r.active_customers / nullif(s.cohort_customers, 0) as retention_rate

from retention r
inner join cohort_sizes s
    on r.cohort_month = s.cohort_month
