"""
bronze_loader.py – Load raw data into the Bronze layer.
Immutable storage with audit columns: load_timestamp, source_file,
source_system, batch_id, load_date, partition.
"""
import os
from datetime import datetime, date
from typing import Optional

import pandas as pd

from src.utils.db_utils import get_engine, bulk_insert
from src.utils.logger import get_logger
from src.metadata.metadata_tracker import PipelineRun, update_watermark

logger = get_logger("bronze_loader")


def _add_audit_columns(
    df: pd.DataFrame,
    source_file: str,
    source_system: str,
    batch_id: str,
) -> pd.DataFrame:
    """Append standard Bronze audit columns to the DataFrame."""
    df = df.copy()
    df["source_file"]    = os.path.basename(source_file)
    df["source_system"]  = source_system
    df["batch_id"]       = batch_id
    df["load_timestamp"] = datetime.now()
    df["load_date"]      = date.today()
    df["is_rejected"]    = False
    df["reject_reason"]  = None
    return df


def load_bronze_table(
    df: pd.DataFrame,
    target_table: str,
    source_file: str,
    source_system: str,
    batch_id: str,
    engine=None,
    update_wm: bool = True,
) -> int:
    """
    Load a DataFrame into a bronze.<target_table> table.

    Args:
        df:            Source DataFrame (raw, all strings)
        target_table:  Target table name in bronze schema
        source_file:   Path to the source file
        source_system: Source system name (CSV/JSON/XML)
        batch_id:      Pipeline run batch ID
        engine:        SQLAlchemy engine
        update_wm:     Whether to update the watermark after load

    Returns:
        Number of rows inserted
    """
    _engine = engine or get_engine()

    if df.empty:
        logger.warning(f"Empty DataFrame for bronze.{target_table} — skipping")
        return 0

    df_with_audit = _add_audit_columns(df, source_file, source_system, batch_id)

    # Keep only columns that exist in target table
    df_with_audit = _align_columns(df_with_audit, target_table, _engine)

    rows = bulk_insert(df_with_audit, target_table, "bronze", _engine)
    logger.info(f"  ✔ bronze.{target_table:<20} ← {rows:>6,} rows")

    if update_wm:
        update_watermark(target_table, datetime.now().strftime("%Y-%m-%d %H:%M:%S"), _engine)

    return rows


def _align_columns(df: pd.DataFrame, table: str, engine) -> pd.DataFrame:
    """Keep only columns that exist in the target table schema."""
    from sqlalchemy import text, inspect
    inspector = inspect(engine)
    try:
        table_cols = [col["name"] for col in inspector.get_columns(table, schema="bronze")]
        # Drop surrogate key (auto-generated)
        table_cols = [c for c in table_cols if c != "bronze_id"]
        # Keep intersection
        keep = [c for c in df.columns if c in table_cols]
        missing = [c for c in table_cols if c not in df.columns and c not in
                   ("load_timestamp","load_date","is_rejected","reject_reason",
                    "source_file","source_system","batch_id")]
        if missing:
            logger.debug(f"Columns not in source (will be NULL): {missing}")
        return df[keep]
    except Exception:
        return df  # Fallback: return as-is


# ─────────────────────────────────────────
# Convenience runners (called by DAGs)
# ─────────────────────────────────────────
def ingest_customers_to_bronze(df: pd.DataFrame, source_file: str, batch_id: str, engine=None) -> int:
    return load_bronze_table(df, "customers", source_file, "CSV", batch_id, engine)

def ingest_orders_to_bronze(df: pd.DataFrame, source_file: str, batch_id: str, engine=None) -> int:
    return load_bronze_table(df, "orders", source_file, "JSON", batch_id, engine)

def ingest_products_to_bronze(df: pd.DataFrame, source_file: str, batch_id: str, engine=None) -> int:
    return load_bronze_table(df, "products", source_file, "XML", batch_id, engine)

def ingest_order_items_to_bronze(df: pd.DataFrame, source_file: str, batch_id: str, engine=None) -> int:
    return load_bronze_table(df, "order_items", source_file, "CSV", batch_id, engine)

def ingest_sellers_to_bronze(df: pd.DataFrame, source_file: str, batch_id: str, engine=None) -> int:
    return load_bronze_table(df, "sellers", source_file, "CSV", batch_id, engine)

def ingest_payments_to_bronze(df: pd.DataFrame, source_file: str, batch_id: str, engine=None) -> int:
    return load_bronze_table(df, "payments", source_file, "JSON", batch_id, engine)
