"""
kpi_summary.py – Compute KPI Summary table in Gold layer.
Pre-aggregated KPIs for BI dashboards.
"""
from datetime import date

import pandas as pd
from sqlalchemy import text

from src.utils.db_utils import get_engine, read_sql
from src.utils.logger import get_logger

logger = get_logger("kpi_summary")


def build_kpi_summary(engine=None) -> int:
    """Compute and persist KPIs to gold.kpi_summary."""
    _engine = engine or get_engine()
    logger.info("Building gold.kpi_summary")

    today   = date.today()
    kpi_rows = []

    # ── 1. Total Revenue ─────────────────────────────────────
    r = read_sql("SELECT SUM(total_item_value) AS v FROM gold.fact_sales", _engine)
    kpi_rows.append(_kpi("total_revenue", r["v"].iloc[0], "BRL", "overall", "all", today))

    # ── 2. Total Orders ──────────────────────────────────────
    r = read_sql("SELECT COUNT(DISTINCT order_id) AS v FROM gold.fact_sales", _engine)
    kpi_rows.append(_kpi("total_orders", r["v"].iloc[0], "count", "overall", "all", today))

    # ── 3. Average Order Value ───────────────────────────────
    r = read_sql("""
        SELECT AVG(s) AS v FROM (
            SELECT order_id, SUM(total_item_value) AS s
            FROM gold.fact_sales GROUP BY order_id
        ) x
    """, _engine)
    kpi_rows.append(_kpi("avg_order_value", r["v"].iloc[0], "BRL", "overall", "all", today))

    # ── 4. Total Customers ───────────────────────────────────
    r = read_sql("SELECT COUNT(*) AS v FROM gold.dim_customer WHERE is_current=TRUE", _engine)
    kpi_rows.append(_kpi("total_customers", r["v"].iloc[0], "count", "overall", "all", today))

    # ── 5. Revenue by State ──────────────────────────────────
    r = read_sql("""
        SELECT c.state, SUM(f.total_item_value) AS v
        FROM gold.fact_sales f
        LEFT JOIN gold.dim_customer c ON f.customer_key = c.customer_key
        WHERE c.state IS NOT NULL
        GROUP BY c.state ORDER BY v DESC LIMIT 10
    """, _engine)
    for _, row in r.iterrows():
        kpi_rows.append(_kpi("revenue_by_state", row["v"], "BRL", "state", str(row["state"]), today))

    # ── 6. Revenue by Category ───────────────────────────────
    r = read_sql("""
        SELECT p.product_category_english AS cat, SUM(f.total_item_value) AS v
        FROM gold.fact_sales f
        LEFT JOIN gold.dim_product p ON f.product_key = p.product_key
        WHERE p.product_category_english IS NOT NULL
        GROUP BY cat ORDER BY v DESC LIMIT 10
    """, _engine)
    for _, row in r.iterrows():
        kpi_rows.append(_kpi("revenue_by_category", row["v"], "BRL", "category", str(row["cat"]), today))

    # ── 7. Late Delivery Rate ────────────────────────────────
    r = read_sql("""
        SELECT
            ROUND(100.0 * SUM(CASE WHEN is_late_delivery THEN 1 ELSE 0 END)
                  / NULLIF(COUNT(*), 0), 2) AS v
        FROM gold.fact_sales WHERE is_late_delivery IS NOT NULL
    """, _engine)
    kpi_rows.append(_kpi("late_delivery_rate_pct", r["v"].iloc[0], "%", "quality", "all", today))

    # ── 8. MoM Revenue Growth (latest) ───────────────────────
    r = read_sql("""
        SELECT mom_growth_pct AS v FROM gold.revenue_mart
        WHERE state = 'SP'
        ORDER BY year DESC, month DESC LIMIT 1
    """, _engine)
    if not r.empty:
        kpi_rows.append(_kpi("latest_mom_revenue_growth_pct", r["v"].iloc[0], "%", "trend", "SP", today))

    # ── Persist ───────────────────────────────────────────────
    df = pd.DataFrame(kpi_rows)
    with _engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE gold.kpi_summary"))
    df.to_sql("kpi_summary", _engine, schema="gold", if_exists="append", index=False, method="multi")

    logger.info(f"  ✔ gold.kpi_summary | {len(df)} KPIs computed")
    return len(df)


def _kpi(name, value, unit, dimension, dimension_value, as_of_date):
    try:
        v = round(float(value), 4) if value is not None else 0.0
    except Exception:
        v = 0.0
    return {
        "kpi_name":        name,
        "kpi_value":       v,
        "kpi_unit":        unit,
        "dimension":       dimension,
        "dimension_value": str(dimension_value),
        "as_of_date":      as_of_date,
    }
