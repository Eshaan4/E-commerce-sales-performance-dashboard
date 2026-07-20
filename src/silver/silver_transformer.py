"""
silver_transformer.py – Transform Bronze data into Silver layer.
Performs: cleansing, standardization, type casting, lookup mapping,
derived column computation, and enrichment.
"""
import pandas as pd
import numpy as np
from datetime import datetime
from typing import Optional

from src.utils.db_utils import get_engine, bulk_insert, read_sql
from src.utils.logger import get_logger

logger = get_logger("silver_transformer")

VALID_STATES = ["AC","AL","AM","AP","BA","CE","DF","ES","GO","MA","MG","MS","MT",
                "PA","PB","PE","PI","PR","RJ","RN","RO","RR","RS","SC","SE","SP","TO"]
VALID_STATUSES = ["delivered","shipped","canceled","invoiced","processing","created","unavailable"]


# ─────────────────────────────────────────────────────────────
# Customers
# ─────────────────────────────────────────────────────────────
def transform_customers(batch_id: str, engine=None) -> pd.DataFrame:
    """Clean and enrich customer data from bronze → silver."""
    _engine = engine or get_engine()

    logger.info("Transforming: bronze.customers → silver.customers")

    df = read_sql(
        f"SELECT * FROM bronze.customers WHERE batch_id = '{batch_id}' AND is_rejected = FALSE",
        _engine
    )
    if df.empty:
        logger.warning("No customers to transform for this batch.")
        return pd.DataFrame()

    # Load lookups
    state_lk = read_sql("SELECT state_code, state_name, region FROM silver.state_lookup", _engine)
    state_map = dict(zip(state_lk["state_code"], state_lk["state_name"]))
    region_map = dict(zip(state_lk["state_code"], state_lk["region"]))

    out = pd.DataFrame()
    out["customer_id"]        = df["customer_id"].str.strip()
    out["customer_unique_id"] = df["customer_unique_id"].str.strip()
    out["zip_code_prefix"]    = df["zip_code_prefix"].str.strip().str.zfill(5)
    out["city"]               = df["city"].str.strip().str.title()
    out["state"]              = df["state"].str.strip().str.upper()

    # Clean invalid states
    out.loc[~out["state"].isin(VALID_STATES), "state"] = None

    # Lookup mapping
    out["state_name"] = out["state"].map(state_map)
    out["region"]     = out["state"].map(region_map)

    # SCD2 columns (new records default)
    out["effective_start_date"] = datetime.today().date()
    out["effective_end_date"]   = None
    out["is_current"]           = True
    out["version_number"]       = 1
    out["batch_id"]             = batch_id
    out["source_system"]        = "CSV"

    # Drop nulls in PK
    before = len(out)
    out.dropna(subset=["customer_id"], inplace=True)
    if before > len(out):
        logger.warning(f"  Dropped {before - len(out)} rows with null customer_id")

    logger.info(f"  ✔ Customers transformed: {len(out):,} records")
    return out


# ─────────────────────────────────────────────────────────────
# Orders
# ─────────────────────────────────────────────────────────────
def transform_orders(batch_id: str, engine=None) -> pd.DataFrame:
    """Clean and enrich orders from bronze → silver."""
    _engine = engine or get_engine()
    logger.info("Transforming: bronze.orders → silver.orders")

    df = read_sql(
        f"SELECT * FROM bronze.orders WHERE batch_id = '{batch_id}' AND is_rejected = FALSE",
        _engine
    )
    if df.empty:
        return pd.DataFrame()

    out = pd.DataFrame()
    out["order_id"]             = df["order_id"].str.strip()
    out["customer_id"]          = df["customer_id"].str.strip()
    out["order_status"]         = df["order_status"].str.strip().str.lower()

    # Timestamp parsing
    out["order_purchase_timestamp"]  = pd.to_datetime(df["order_purchase_timestamp"],  errors="coerce")
    out["order_approved_at"]         = pd.to_datetime(df["order_approved_at"],          errors="coerce")
    out["order_delivered_at"]        = pd.to_datetime(df["order_delivered_at"],         errors="coerce")
    out["order_estimated_delivery"]  = pd.to_datetime(df["order_estimated_delivery"],   errors="coerce").dt.date

    # Derived: delivery days
    out["delivery_days"] = (
        out["order_delivered_at"] - out["order_purchase_timestamp"]
    ).dt.days.where(out["order_delivered_at"].notnull())

    # Derived: late delivery flag
    out["is_late_delivery"] = (
        out["order_delivered_at"].dt.date > out["order_estimated_delivery"]
    ).where(out["order_delivered_at"].notnull(), other=False)

    # Derived: status category
    out["order_status_category"] = out["order_status"].map({
        "delivered":  "completed",
        "shipped":    "active",
        "invoiced":   "active",
        "processing": "active",
        "created":    "active",
        "canceled":   "cancelled",
        "unavailable":"cancelled",
    }).fillna("unknown")

    out.dropna(subset=["order_id", "customer_id"], inplace=True)
    
    # Fix NaT/NaN issues for SQLAlchemy 1.4 insert
    out = out.astype(object).where(pd.notnull(out), None)

    logger.info(f"  ✔ Orders transformed: {len(out):,} records")
    return out


# ─────────────────────────────────────────────────────────────
# Products
# ─────────────────────────────────────────────────────────────
def transform_products(batch_id: str, engine=None) -> pd.DataFrame:
    """Clean and enrich products from bronze → silver."""
    _engine = engine or get_engine()
    logger.info("Transforming: bronze.products → silver.products")

    df = read_sql(
        f"SELECT * FROM bronze.products WHERE batch_id = '{batch_id}' AND is_rejected = FALSE",
        _engine
    )
    if df.empty:
        return pd.DataFrame()

    # Load category lookup
    cat_lk  = read_sql("SELECT category_pt, category_en FROM silver.category_lookup", _engine)
    cat_map  = dict(zip(cat_lk["category_pt"], cat_lk["category_en"]))

    out = pd.DataFrame()
    out["product_id"]               = df["product_id"].str.strip()
    out["product_category_name"]    = df["product_category_name"].str.strip().str.lower()
    out["product_category_english"] = out["product_category_name"].map(cat_map).fillna("Others")

    # Safe numeric cast
    for col in ["product_name_length","product_description_length","product_photos_qty",
                "product_weight_g","product_length_cm","product_height_cm","product_width_cm"]:
        out[col] = pd.to_numeric(df.get(col, pd.Series()), errors="coerce")

    # Derived: volume
    out["product_volume_cm3"] = (
        out["product_length_cm"] * out["product_height_cm"] * out["product_width_cm"]
    ).round(2)

    out["batch_id"]     = batch_id
    out["source_system"] = "XML"
    out.dropna(subset=["product_id"], inplace=True)

    logger.info(f"  ✔ Products transformed: {len(out):,} records")
    return out


# ─────────────────────────────────────────────────────────────
# Order Items
# ─────────────────────────────────────────────────────────────
def transform_order_items(batch_id: str, engine=None) -> pd.DataFrame:
    """Clean order items from bronze → silver."""
    _engine = engine or get_engine()
    logger.info("Transforming: bronze.order_items → silver.order_items")

    df = read_sql(
        f"SELECT * FROM bronze.order_items WHERE batch_id = '{batch_id}' AND is_rejected = FALSE",
        _engine
    )
    if df.empty:
        return pd.DataFrame()

    out = pd.DataFrame()
    out["order_id"]             = df["order_id"].str.strip()
    out["order_item_id"]        = pd.to_numeric(df["order_item_id"], errors="coerce").astype("Int64")
    out["product_id"]           = df["product_id"].str.strip()
    out["seller_id"]            = df["seller_id"].str.strip()
    out["shipping_limit_date"]  = pd.to_datetime(df["shipping_limit_date"], errors="coerce")
    out["price"]                = pd.to_numeric(df["price"],         errors="coerce").round(2)
    out["freight_value"]        = pd.to_numeric(df["freight_value"], errors="coerce").round(2)
    out["total_item_value"]     = (out["price"] + out["freight_value"]).round(2)

    out["batch_id"]     = batch_id
    out["source_system"] = "CSV"
    out.dropna(subset=["order_id"], inplace=True)

    logger.info(f"  ✔ Order items transformed: {len(out):,} records")
    return out


# ─────────────────────────────────────────────────────────────
# Sellers
# ─────────────────────────────────────────────────────────────
def transform_sellers(batch_id: str, engine=None) -> pd.DataFrame:
    """Clean and enrich sellers from bronze → silver."""
    _engine = engine or get_engine()
    logger.info("Transforming: bronze.sellers → silver.sellers")

    df = read_sql(
        f"SELECT * FROM bronze.sellers WHERE batch_id = '{batch_id}' AND is_rejected = FALSE",
        _engine
    )
    if df.empty:
        return pd.DataFrame()

    state_lk  = read_sql("SELECT state_code, state_name, region FROM silver.state_lookup", _engine)
    state_map  = dict(zip(state_lk["state_code"], state_lk["state_name"]))
    region_map = dict(zip(state_lk["state_code"], state_lk["region"]))

    out = pd.DataFrame()
    out["seller_id"]        = df["seller_id"].str.strip()
    out["zip_code_prefix"]  = df["zip_code_prefix"].str.strip().str.zfill(5)
    out["city"]             = df["city"].str.strip().str.title()
    out["state"]            = df["state"].str.strip().str.upper()
    out.loc[~out["state"].isin(VALID_STATES), "state"] = None
    out["state_name"] = out["state"].map(state_map)
    out["region"]     = out["state"].map(region_map)

    # SCD2 defaults
    out["effective_start_date"] = datetime.today().date()
    out["effective_end_date"]   = None
    out["is_current"]           = True
    out["version_number"]       = 1
    out["batch_id"]     = batch_id
    out["source_system"] = "CSV"
    out.dropna(subset=["seller_id"], inplace=True)

    logger.info(f"  ✔ Sellers transformed: {len(out):,} records")
    return out


# ─────────────────────────────────────────────────────────────
# Payments
# ─────────────────────────────────────────────────────────────
def transform_payments(batch_id: str, engine=None) -> pd.DataFrame:
    """Clean payments from bronze → silver."""
    _engine = engine or get_engine()
    logger.info("Transforming: bronze.payments → silver.payments")

    df = read_sql(
        f"SELECT * FROM bronze.payments WHERE batch_id = '{batch_id}' AND is_rejected = FALSE",
        _engine
    )
    if df.empty:
        return pd.DataFrame()

    PAYMENT_CATEGORY = {
        "credit_card": "card",
        "debit_card":  "card",
        "boleto":      "boleto",
        "voucher":     "voucher",
    }

    out = pd.DataFrame()
    out["order_id"]              = df["order_id"].str.strip()
    out["payment_sequential"]    = pd.to_numeric(df["payment_sequential"], errors="coerce").astype("Int64")
    out["payment_type"]          = df["payment_type"].str.strip().str.lower()
    out["payment_type_category"] = out["payment_type"].map(PAYMENT_CATEGORY).fillna("other")
    out["payment_installments"]  = pd.to_numeric(df["payment_installments"], errors="coerce").astype("Int64")
    out["payment_value"]         = pd.to_numeric(df["payment_value"], errors="coerce").round(2)

    out["batch_id"]     = batch_id
    out["source_system"] = "JSON"
    out.dropna(subset=["order_id"], inplace=True)

    logger.info(f"  ✔ Payments transformed: {len(out):,} records")
    return out
