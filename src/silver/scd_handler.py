"""
scd_handler.py – Slowly Changing Dimension (SCD) implementation.
Compatible with SQLAlchemy 1.4 (Airflow 2.8.1 constraint).

SCD Type 1: Overwrite (no history). Used for: products.
SCD Type 2: Full history (effective dates, is_current, version_number).
            Used for: customers, sellers.
"""
from datetime import date, datetime
from typing import List, Optional

import pandas as pd
from sqlalchemy import text

from src.utils.db_utils import get_engine
from src.utils.logger import get_logger

logger = get_logger("scd_handler")


# ─────────────────────────────────────────────────────────────
# SCD Type 1 – Overwrite
# ─────────────────────────────────────────────────────────────
def apply_scd_type1(
    df: pd.DataFrame,
    table: str,
    schema: str,
    natural_key: str,
    engine=None,
) -> dict:
    """
    INSERT new records, UPDATE existing ones in-place.
    No history retained.
    """
    _engine = engine or get_engine()
    if df.empty:
        return {"inserted": 0, "updated": 0}

    cols        = list(df.columns)
    col_names   = ", ".join(cols)
    placeholders= ", ".join([f":{c}" for c in cols])
    update_cols = [c for c in cols if c not in (natural_key, "created_at")]
    update_set  = ", ".join([f"{c} = EXCLUDED.{c}" for c in update_cols])

    sql = (
        f"INSERT INTO {schema}.{table} ({col_names}) "
        f"VALUES ({placeholders}) "
        f"ON CONFLICT ({natural_key}) "
        f"DO UPDATE SET {update_set}, updated_at = NOW()"
    )

    # SQLAlchemy 1.4: pass list of dicts directly
    records = df.to_dict(orient="records")
    with _engine.begin() as conn:
        for rec in records:
            conn.execute(text(sql), rec)

    logger.info("  [SCD1] %s.%s | upserted %d records", schema, table, len(df))
    return {"inserted": len(df), "updated": 0}


# ─────────────────────────────────────────────────────────────
# SCD Type 2 – Full History
# ─────────────────────────────────────────────────────────────
def apply_scd_type2(
    df: pd.DataFrame,
    table: str,
    schema: str,
    natural_key: str,
    compare_columns: List[str],
    engine=None,
) -> dict:
    """
    Expire changed rows and insert new version rows.
    New rows get is_current=True, version incremented.
    """
    _engine = engine or get_engine()
    if df.empty:
        return {"inserted": 0, "updated": 0}

    today  = date.today()
    counts = {"inserted": 0, "updated": 0}

    # Fetch currently-active records
    with _engine.connect() as conn:
        result   = conn.execute(text(f"SELECT * FROM {schema}.{table} WHERE is_current = TRUE"))
        rows     = result.fetchall()
        ex_cols  = list(result.keys())
    existing = pd.DataFrame(rows, columns=ex_cols) if rows else pd.DataFrame(columns=ex_cols)
    existing_keys = set(existing[natural_key].astype(str)) if not existing.empty else set()

    new_rows: list = []
    expired_keys: list = []

    for _, row in df.iterrows():
        nk = str(row[natural_key])

        if nk not in existing_keys:
            rec = _build_record(row, today, version=1)
            new_rows.append(rec)
            counts["inserted"] += 1
        else:
            ex_row = existing[existing[natural_key].astype(str) == nk].iloc[0]
            changed = any(
                str(row.get(c, "")) != str(ex_row.get(c, ""))
                for c in compare_columns
                if c in row.index and c in ex_row.index
            )
            if changed:
                expired_keys.append(nk)
                version = int(ex_row.get("version_number", 1)) + 1
                rec = _build_record(row, today, version=version)
                new_rows.append(rec)
                counts["updated"] += 1

    # Expire old versions
    if expired_keys:
        expire_sql = (
            f"UPDATE {schema}.{table} "
            f"SET is_current = FALSE, effective_end_date = :today, updated_at = NOW() "
            f"WHERE {natural_key} = ANY(:keys) AND is_current = TRUE"
        )
        with _engine.begin() as conn:
            conn.execute(text(expire_sql), {"today": today, "keys": expired_keys})
        logger.info("  [SCD2] Expired %d old versions in %s.%s", len(expired_keys), schema, table)

    # Insert new/updated versions
    if new_rows:
        new_df = pd.DataFrame(new_rows)
        # Drop auto-generated cols
        skip = {"silver_id", "created_at", "updated_at"}
        insert_cols = [c for c in new_df.columns if c not in skip]
        new_df = new_df[insert_cols]

        col_names    = ", ".join(new_df.columns)
        placeholders = ", ".join([f":{c}" for c in new_df.columns])
        ins_sql = f"INSERT INTO {schema}.{table} ({col_names}) VALUES ({placeholders})"

        records = new_df.to_dict(orient="records")
        with _engine.begin() as conn:
            for rec in records:
                conn.execute(text(ins_sql), rec)

    logger.info(
        "  [SCD2] %s.%s | new=%d | updated(versioned)=%d",
        schema, table, counts["inserted"], counts["updated"],
    )
    return counts


def _build_record(row: pd.Series, start_date: date, version: int) -> dict:
    rec = row.to_dict()
    rec["effective_start_date"] = start_date
    rec["effective_end_date"]   = None
    rec["is_current"]           = True
    rec["version_number"]       = version
    return rec
