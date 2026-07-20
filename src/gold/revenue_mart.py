"""
revenue_mart.py – Compute Revenue Mart with Window Functions.
Aggregates by year, month, state, region, and product category.
Computes: MoM growth, cumulative revenue, rank within month.
"""
import pandas as pd
from sqlalchemy import text

from src.utils.db_utils import get_engine, read_sql
from src.utils.logger import get_logger

logger = get_logger("revenue_mart")


def build_revenue_mart(engine=None) -> int:
    """
    Build gold.revenue_mart from gold.fact_sales + gold.dim_* tables.
    Uses window functions for ranking, cumulative revenue, MoM growth.
    """
    _engine = engine or get_engine()
    logger.info("Building gold.revenue_mart")

    query = """
        SELECT
            EXTRACT(YEAR  FROM d.full_date)::INT    AS year,
            EXTRACT(MONTH FROM d.full_date)::INT    AS month,
            TRIM(d.month_name)                      AS month_name,
            COALESCE(c.state,  'Unknown')           AS state,
            COALESCE(c.region, 'Unknown')           AS region,
            COALESCE(p.product_category_english, 'Unknown') AS product_category,
            COUNT(DISTINCT f.order_id)              AS total_orders,
            SUM(f.total_item_value)                 AS total_revenue,
            SUM(f.freight_value)                    AS total_freight,
            AVG(f.total_item_value)                 AS avg_order_value,
            COUNT(f.sales_key)                      AS total_items_sold
        FROM gold.fact_sales      f
        JOIN gold.dim_date        d ON f.order_date_key = d.date_key
        LEFT JOIN gold.dim_customer c ON f.customer_key = c.customer_key
        LEFT JOIN gold.dim_product  p ON f.product_key  = p.product_key
        WHERE f.order_date_key IS NOT NULL
        GROUP BY 1, 2, 3, 4, 5, 6
    """

    df = read_sql(query, _engine)
    if df.empty:
        logger.warning("No data for revenue mart")
        return 0

    df["total_revenue"]    = df["total_revenue"].round(2)
    df["total_freight"]    = df["total_freight"].round(2)
    df["avg_order_value"]  = df["avg_order_value"].round(2)

    # ── Window Functions (in pandas) ──────────────────────────

    # 1. Rank within month by revenue (per state)
    df["revenue_rank_in_month"] = (
        df.groupby(["year", "month"])["total_revenue"]
          .rank(method="dense", ascending=False)
          .astype(int)
    )

    # 2. Cumulative revenue (sorted by year + month)
    df = df.sort_values(["year", "month"])
    df["cumulative_revenue"] = df.groupby(["state", "region"])["total_revenue"].cumsum().round(2)

    # 3. Previous month revenue
    df = df.sort_values(["state", "region", "product_category", "year", "month"])
    df["prev_month_revenue"] = (
        df.groupby(["state", "region", "product_category"])["total_revenue"].shift(1)
    )

    # 4. Month-over-Month growth %
    df["mom_growth_pct"] = (
        ((df["total_revenue"] - df["prev_month_revenue"]) / df["prev_month_revenue"].replace(0, None))
        * 100
    ).round(2)

    df["computed_at"] = pd.Timestamp.now()

    # Truncate and reload
    with _engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE gold.revenue_mart"))

    df.to_sql("revenue_mart", _engine, schema="gold", if_exists="append", index=False, method="multi")
    logger.info(f"  ✔ gold.revenue_mart | {len(df):,} rows")
    return len(df)
