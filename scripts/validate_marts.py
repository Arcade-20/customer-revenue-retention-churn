import os
import math
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, List, Optional

import pandas as pd
from snowflake import connector

# Load .env if present (recommended)
try:
    from dotenv import load_dotenv  # type: ignore
    load_dotenv()
except Exception:
    pass


# ---------------------------
# Helpers
# ---------------------------

def env(name: str, required: bool = True, default: Optional[str] = None) -> str:
    val = os.getenv(name, default)
    if required and (val is None or str(val).strip() == ""):
        raise RuntimeError(f"Missing required environment variable: {name}")
    return str(val)


def fqtn(database: str, schema: str, table: str) -> str:
    return f"{database}.{schema}.{table}"


def fetch_df(conn, sql: str) -> pd.DataFrame:
    cur = conn.cursor()
    try:
        cur.execute(sql)
        return cur.fetch_pandas_all()
    finally:
        cur.close()


def fetch_one(conn, sql: str) -> Any:
    cur = conn.cursor()
    try:
        cur.execute(sql)
        row = cur.fetchone()
        return None if row is None else row[0]
    finally:
        cur.close()


@dataclass
class CheckResult:
    name: str
    status: str  # PASS / FAIL / WARN
    details: str
    sample_query: Optional[str] = None


def sf_connect():
    return connector.connect(
        account=env("SNOWFLAKE_ACCOUNT"),
        user=env("SNOWFLAKE_USER"),
        password=env("SNOWFLAKE_PASSWORD"),
        role=env("SNOWFLAKE_ROLE"),
        warehouse=env("SNOWFLAKE_WAREHOUSE"),
        database=env("SNOWFLAKE_DATABASE"),
    )


# ---------------------------
# Checks
# ---------------------------

def check_table_exists(conn, table_fqtn: str) -> CheckResult:
    try:
        _ = fetch_one(conn, f"select count(*) from {table_fqtn}")
        return CheckResult(f"{table_fqtn} exists", "PASS", "Table is queryable.")
    except Exception as e:
        return CheckResult(f"{table_fqtn} exists", "FAIL", f"Cannot query table: {e}")


def check_kpi_summary_single_row(conn, kpi_tbl: str) -> CheckResult:
    n = int(fetch_one(conn, f"select count(*) from {kpi_tbl}"))
    if n == 1:
        return CheckResult("mart_kpi_summary has 1 row", "PASS", f"Row count = {n}")
    return CheckResult("mart_kpi_summary has 1 row", "FAIL", f"Row count = {n} (expected 1)")


def check_kpi_summary_consistency(conn, kpi_tbl: str, churn_tol: float = 0.005) -> CheckResult:
    df = fetch_df(conn, f"select * from {kpi_tbl}")
    if df.shape[0] != 1:
        return CheckResult("mart_kpi_summary KPI consistency", "FAIL", "Not exactly 1 row; cannot validate KPIs.")

    row = {str(k).upper(): v for k, v in df.iloc[0].to_dict().items()}

    required = ["TOTAL_CUSTOMERS", "ACTIVE_CUSTOMERS", "CHURNED_CUSTOMERS", "CHURN_RATE_90D"]
    missing = [c for c in required if c not in row]
    if missing:
        return CheckResult(
            "mart_kpi_summary KPI consistency",
            "WARN",
            f"Missing expected columns: {missing}. Found: {list(row.keys())}"
        )

    total = float(row.get("TOTAL_CUSTOMERS", float("nan")))
    active = float(row.get("ACTIVE_CUSTOMERS", float("nan")))
    churned = float(row.get("CHURNED_CUSTOMERS", float("nan")))
    churn_rate = float(row.get("CHURN_RATE_90D", float("nan")))

    issues = []

    if not math.isnan(total) and not math.isnan(active) and active > total:
        issues.append(f"ACTIVE_CUSTOMERS ({active}) > TOTAL_CUSTOMERS ({total})")

    if not math.isnan(total) and not math.isnan(active) and not math.isnan(churned):
        if abs((active + churned) - total) != 0:
            issues.append(f"ACTIVE + CHURNED != TOTAL ({active}+{churned} != {total})")

    if not math.isnan(total) and total > 0 and not math.isnan(churned) and not math.isnan(churn_rate):
        expected = churned / total
        if abs(expected - churn_rate) > churn_tol:
            issues.append(f"CHURN_RATE_90D mismatch: expected {expected:.6f}, got {churn_rate:.6f} (tol={churn_tol})")

    if not issues:
        return CheckResult("mart_kpi_summary KPI consistency", "PASS", "KPI relationships look consistent.")
    return CheckResult("mart_kpi_summary KPI consistency", "FAIL", " | ".join(issues))


def check_monthly_revenue_non_negative(conn, rev_tbl: str) -> CheckResult:
    sql = f"""
    select *
    from {rev_tbl}
    where total_payments < 0 or total_gmv < 0
    limit 10
    """
    bad = fetch_df(conn, sql)
    if bad.empty:
        return CheckResult("mart_monthly_revenue non-negative totals", "PASS", "No negative totals found.")
    return CheckResult(
        "mart_monthly_revenue non-negative totals",
        "FAIL",
        f"Found {len(bad)} rows with negative totals (showing up to 10).",
        sample_query=sql.strip()
    )


def check_monthly_revenue_recon(conn, rev_tbl: str, abs_tol: float = 1.0) -> CheckResult:
    cols = fetch_df(conn, f"select * from {rev_tbl} limit 1").columns.tolist()
    # Try to detect a recon/diff column name
    candidates = [c for c in cols if "DIFF" in c.upper() or "RECON" in c.upper()]
    if not candidates:
        return CheckResult(
            "mart_monthly_revenue reconciliation diff",
            "WARN",
            f"No recon/diff column found. Columns: {cols}"
        )
    diff_col = candidates[0]

    sql = f"""
    select order_month, {diff_col}
    from {rev_tbl}
    where abs({diff_col}) > {abs_tol}
    order by abs({diff_col}) desc
    limit 10
    """
    bad = fetch_df(conn, sql)
    if bad.empty:
        return CheckResult("mart_monthly_revenue reconciliation diff", "PASS", f"All |{diff_col}| <= {abs_tol}.")
    return CheckResult(
        "mart_monthly_revenue reconciliation diff",
        "FAIL",
        f"Found months with |{diff_col}| > {abs_tol} (showing up to 10).",
        sample_query=sql.strip()
    )


def check_revenue_churn_split(conn, split_tbl: str, abs_tol: float = 1.0) -> CheckResult:
    df_cols = fetch_df(conn, f"select * from {split_tbl} limit 1")
    cols = df_cols.columns.tolist()
    upper_map = {c.upper(): c for c in cols}

    needed = ["TOTAL_PAYMENTS", "CHURNED_CUSTOMER_PAYMENTS", "ACTIVE_CUSTOMER_PAYMENTS"]
    if not all(k in upper_map for k in needed):
        return CheckResult(
            "mart_monthly_revenue_churn split reconciliation",
            "WARN",
            f"Missing required columns. Need {needed}. Found: {cols}"
        )

    total = upper_map["TOTAL_PAYMENTS"]
    churned = upper_map["CHURNED_CUSTOMER_PAYMENTS"]
    active = upper_map["ACTIVE_CUSTOMER_PAYMENTS"]

    sql = f"""
    select *
    from {split_tbl}
    where abs(({churned} + {active}) - {total}) > {abs_tol}
       or {total} < 0 or {churned} < 0 or {active} < 0
    limit 10
    """
    bad = fetch_df(conn, sql)
    if bad.empty:
        return CheckResult(
            "mart_monthly_revenue_churn split reconciliation",
            "PASS",
            f"Split sums + non-negative checks pass (tol={abs_tol})."
        )
    return CheckResult(
        "mart_monthly_revenue_churn split reconciliation",
        "FAIL",
        "Found rows failing split/non-negative checks (showing up to 10).",
        sample_query=sql.strip()
    )


def check_customer_churn_logic(conn, churn_tbl: str) -> CheckResult:
    cols = fetch_df(conn, f"select * from {churn_tbl} limit 1").columns.tolist()
    upper_map = {c.upper(): c for c in cols}

    needed = ["IS_CHURNED_90D", "DAYS_SINCE_LAST_ORDER"]
    if not all(k in upper_map for k in needed):
        return CheckResult(
            "mart_customer_churn churn flag logic",
            "WARN",
            f"Missing columns for churn logic check. Need {needed}. Found: {cols}"
        )

    flag = upper_map["IS_CHURNED_90D"]
    days = upper_map["DAYS_SINCE_LAST_ORDER"]

    sql = f"""
    select *
    from {churn_tbl}
    where ({flag} = 1 and {days} < 90)
       or ({flag} = 0 and {days} >= 90)
    limit 10
    """
    bad = fetch_df(conn, sql)
    if bad.empty:
        return CheckResult("mart_customer_churn churn flag logic", "PASS", "Churn flag aligns with days-since logic.")
    return CheckResult(
        "mart_customer_churn churn flag logic",
        "FAIL",
        "Found rows where churn flag contradicts days_since_last_order (showing up to 10).",
        sample_query=sql.strip()
    )


def check_retention_rate_bounds(conn, retention_tbl: str) -> CheckResult:
    cols = fetch_df(conn, f"select * from {retention_tbl} limit 1").columns.tolist()
    upper_map = {c.upper(): c for c in cols}

    if "RETENTION_RATE" not in upper_map:
        return CheckResult(
            "mart_customer_retention retention_rate bounds",
            "WARN",
            f"RETENTION_RATE column not found. Columns: {cols}"
        )

    rr = upper_map["RETENTION_RATE"]

    sql = f"""
    select *
    from {retention_tbl}
    where {rr} < 0 or {rr} > 1
    limit 10
    """
    bad = fetch_df(conn, sql)
    if bad.empty:
        return CheckResult("mart_customer_retention retention_rate bounds", "PASS", "All retention_rate values within [0, 1].")
    return CheckResult(
        "mart_customer_retention retention_rate bounds",
        "FAIL",
        "Found retention_rate values outside [0, 1] (showing up to 10).",
        sample_query=sql.strip()
    )


# ---------------------------
# Report Writer
# ---------------------------

def write_markdown_report(results: List[CheckResult], out_path: str, meta: dict) -> None:
    lines = []
    lines.append("# Data Quality Report (Post-dbt Validation)")
    lines.append("")
    lines.append(f"**Generated (UTC):** {meta['generated_utc']}")
    lines.append(f"**Snowflake DB:** `{meta['database']}`  ")
    lines.append(f"**MART schema:** `{meta['schema_mart']}`  ")
    lines.append(f"**Warehouse:** `{meta['warehouse']}`  ")
    lines.append("")

    # Summary
    counts = {"PASS": 0, "FAIL": 0, "WARN": 0}
    for r in results:
        counts[r.status] = counts.get(r.status, 0) + 1

    lines.append("## Summary")
    lines.append(f"- PASS: **{counts['PASS']}**")
    lines.append(f"- FAIL: **{counts['FAIL']}**")
    lines.append(f"- WARN: **{counts['WARN']}**")
    lines.append("")

    # Detailed table
    lines.append("## Checks")
    lines.append("")
    lines.append("| Status | Check | Details |")
    lines.append("|---|---|---|")
    for r in results:
        details = r.details.replace("\n", " ")
        lines.append(f"| {r.status} | {r.name} | {details} |")
    lines.append("")

    # Include sample queries for failures
    failures = [r for r in results if r.status == "FAIL" and r.sample_query]
    if failures:
        lines.append("## Failure Sample Queries")
        lines.append("These queries return example failing rows (limited).")
        lines.append("")
        for r in failures:
            lines.append(f"### {r.name}")
            lines.append("```sql")
            lines.append(r.sample_query.strip())
            lines.append("```")
            lines.append("")

    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))


def main():
    database = env("SNOWFLAKE_DATABASE")
    schema_mart = env("SNOWFLAKE_SCHEMA_MART")
    warehouse = env("SNOWFLAKE_WAREHOUSE")

    # MART tables
    tbl_kpi = fqtn(database, schema_mart, "MART_KPI_SUMMARY")
    tbl_rev = fqtn(database, schema_mart, "MART_MONTHLY_REVENUE")
    tbl_rev_churn = fqtn(database, schema_mart, "MART_MONTHLY_REVENUE_CHURN")
    tbl_churn = fqtn(database, schema_mart, "MART_CUSTOMER_CHURN")
    tbl_retention = fqtn(database, schema_mart, "MART_CUSTOMER_RETENTION")

    results: List[CheckResult] = []
    generated_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    conn = sf_connect()
    try:
        # Existence checks first
        for t in [tbl_kpi, tbl_rev, tbl_rev_churn, tbl_churn, tbl_retention]:
            results.append(check_table_exists(conn, t))

        # Only run deeper checks if the table exists (PASS)
        exists_map = {r.name.replace(" exists", ""): (r.status == "PASS") for r in results if r.name.endswith(" exists")}

        if exists_map.get(tbl_kpi, False):
            results.append(check_kpi_summary_single_row(conn, tbl_kpi))
            results.append(check_kpi_summary_consistency(conn, tbl_kpi))

        if exists_map.get(tbl_rev, False):
            results.append(check_monthly_revenue_non_negative(conn, tbl_rev))
            results.append(check_monthly_revenue_recon(conn, tbl_rev, abs_tol=1.0))

        if exists_map.get(tbl_rev_churn, False):
            results.append(check_revenue_churn_split(conn, tbl_rev_churn, abs_tol=1.0))

        if exists_map.get(tbl_churn, False):
            results.append(check_customer_churn_logic(conn, tbl_churn))

        if exists_map.get(tbl_retention, False):
            results.append(check_retention_rate_bounds(conn, tbl_retention))

    finally:
        conn.close()

    meta = {
        "generated_utc": generated_utc,
        "database": database,
        "schema_mart": schema_mart,
        "warehouse": warehouse,
    }

    out_path = os.path.join("reports", "data_quality_report.md")
    write_markdown_report(results, out_path, meta)

    # Exit code behavior (optional): fail CI if any FAIL
    fail_count = sum(1 for r in results if r.status == "FAIL")
    print(f"Report written to: {out_path}")
    print(f"PASS={sum(1 for r in results if r.status == 'PASS')}, FAIL={fail_count}, WARN={sum(1 for r in results if r.status == 'WARN')}")
    if fail_count > 0:
        raise SystemExit(1)


if __name__ == "__main__":
    main()