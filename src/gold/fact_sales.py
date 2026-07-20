"""
fact_sales.py – Build the Sales Fact table in Gold layer.
Joins: silver.order_items + silver.orders + silver.payments
       with gold dimension surrogate keys.
"""
import pandas as pd
from sqlalchemy import text

from src.utils.db_utils import get_engine, read_sql
from src.utils.logger import get_logger

logger = get_logger("fact_sales")


def build_fact_sales(batch_id: str, engine=None) -> dict:
    """
    Build gold.fact_sales by joining silver tables and resolving surrogate keys.
    """
    _engine = engine or get_engine()
    logger.info("Building gold.fact_sales")

    # Load silver tables
    items    = read_sql("SELECT * FROM silver.order_items", _engine)
    orders   = read_sql("SELECT * FROM silver.orders",      _engine)
    payments = read_sql("SELECT order_id, payment_type, payment_installments, payment_value FROM silver.payments", _engine)

    if items.empty or orders.empty:
        logger.warning("Insufficient silver data for fact_sales")
        return {"inserted": 0}

    # Aggregate payments per order (take first payment record)
    pay_agg = (
        payments.groupby("order_id")
        .agg(payment_value=("payment_value", "sum"),
             payment_type=("payment_type", "first"),
             payment_installments=("payment_installments", "first"))
        .reset_index()
    )

    # Join items → orders → payments
    fact = items.merge(orders[
        ["order_id","customer_id","order_status","order_purchase_timestamp",
         "order_delivered_at","delivery_days","is_late_delivery"]
    ], on="order_id", how="left")

    fact = fact.merge(pay_agg, on="order_id", how="left")

    # Resolve surrogate keys
    dim_cust = read_sql(
        "SELECT customer_key, customer_id FROM gold.dim_customer WHERE is_current = TRUE", _engine)
    dim_prod = read_sql(
        "SELECT product_key, product_id FROM gold.dim_product", _engine)
    dim_sell = read_sql(
        "SELECT seller_key, seller_id FROM gold.dim_seller WHERE is_current = TRUE", _engine)

    fact = fact.merge(dim_cust.rename(columns={"customer_id": "customer_id"}),
                      on="customer_id", how="left")
    fact = fact.merge(dim_prod, on="product_id", how="left")
    fact = fact.merge(dim_sell, on="seller_id",  how="left")

    # Date keys
    def to_date_key(col):
        return pd.to_datetime(col, errors="coerce").dt.strftime("%Y%m%d").astype(float).astype("Int64")

    fact["order_date_key"]    = to_date_key(fact["order_purchase_timestamp"])
    fact["delivery_date_key"] = to_date_key(fact.get("order_delivered_at"))

    # Build final fact DataFrame
    out = pd.DataFrame({
        "order_id":             fact["order_id"],
        "order_item_id":        fact["order_item_id"],
        "customer_key":         fact.get("customer_key"),
        "product_key":          fact.get("product_key"),
        "seller_key":           fact.get("seller_key"),
        "order_date_key":       fact["order_date_key"],
        "delivery_date_key":    fact["delivery_date_key"],
        "price":                fact["price"],
        "freight_value":        fact["freight_value"],
        "total_item_value":     fact["total_item_value"],
        "payment_value":        fact.get("payment_value"),
        "payment_type":         fact.get("payment_type"),
        "payment_installments": fact.get("payment_installments"),
        "delivery_days":        fact.get("delivery_days"),
        "is_late_delivery":     fact.get("is_late_delivery", False),
        "order_status":         fact.get("order_status"),
        "batch_id":             batch_id,
    })

    out = out.dropna(subset=["order_id"])
    
    # Fix NaN for integer columns
    out = out.astype(object).where(pd.notnull(out), None)

    cols = ", ".join(out.columns)
    vals = ", ".join([f":{c}" for c in out.columns])
    with _engine.begin() as conn:
        conn.execute(text(f"DELETE FROM gold.fact_sales WHERE batch_id = :bid"), {"bid": batch_id})
        conn.execute(text(f"INSERT INTO gold.fact_sales ({cols}) VALUES ({vals})"),
                     out.to_dict(orient="records"))

    logger.info(f"  ✔ gold.fact_sales | inserted={len(out):,}")
    return {"inserted": len(out)}
