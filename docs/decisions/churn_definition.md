# Churn Definition Decision Log

Churn is not labeled in Olist. This project engineers churn using inactivity rules.

Planned churn windows to compare:
- 60 days inactive
- 90 days inactive
- 120 days inactive

Decisions to finalize later:
- what counts as an "activity" event (approved vs delivered vs paid)
- handling customers near the dataset end (right-censoring)
- reactivation definition (returning after being churned)