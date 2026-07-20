"""
dim_seller.py – Build Seller Dimension (SCD Type 2) in Gold layer.
"""
import pandas as pd
from datetime import date
from sqlalchemy import text

from src.utils.db_utils import get_engine, read_sql
from src.utils.logger import get_logger

logger = get_logger("dim_seller")


def build_dim_seller(batch_id: str, engine=None) -> dict:
    """
    Apply SCD Type 2 from silver.sellers → gold.dim_seller.
    """
    _engine = engine or get_engine()
    logger.info("Building gold.dim_seller from silver.sellers")

    silver = read_sql("SELECT * FROM silver.sellers WHERE is_current = TRUE", _engine)
    if silver.empty:
        return {"inserted": 0, "updated": 0}

    existing = read_sql("SELECT * FROM gold.dim_seller WHERE is_current = TRUE", _engine)
    existing_map = (
        {str(r["seller_id"]): r for _, r in existing.iterrows()}
        if not existing.empty else {}
    )

    today = date.today()
    new_rows     = []
    expired_keys = []
    counts = {"inserted": 0, "updated": 0}
    compare_cols = ["city", "state", "zip_code_prefix"]

    for _, row in silver.iterrows():
        sid = str(row["seller_id"])
        if sid not in existing_map:
            new_rows.append(_make_row(row, batch_id, today, 1))
            counts["inserted"] += 1
        else:
            ex = existing_map[sid]
            if any(str(row.get(c, "")) != str(ex.get(c, "")) for c in compare_cols):
                expired_keys.append(sid)
                new_rows.append(_make_row(row, batch_id, today,
                                           int(ex.get("version_number", 1)) + 1))
                counts["updated"] += 1

    if expired_keys:
        with _engine.begin() as conn:
            conn.execute(text("""
                UPDATE gold.dim_seller
                SET is_current = FALSE, effective_end_date = :today, updated_at = NOW()
                WHERE seller_id = ANY(:keys) AND is_current = TRUE
            """), {"today": today, "keys": expired_keys})

    if new_rows:
        df = pd.DataFrame(new_rows)
        cols = ", ".join(df.columns)
        vals = ", ".join([f":{c}" for c in df.columns])
        with _engine.begin() as conn:
            conn.execute(text(f"INSERT INTO gold.dim_seller ({cols}) VALUES ({vals})"),
                         df.to_dict(orient="records"))

    logger.info(f"  ✔ gold.dim_seller | inserted={counts['inserted']} | updated={counts['updated']}")
    return counts


def _make_row(row, batch_id, start_date, version):
    return {
        "seller_id":             str(row.get("seller_id",        "")),
        "zip_code_prefix":       str(row.get("zip_code_prefix",  "")),
        "city":                  str(row.get("city",             "")),
        "state":                 str(row.get("state",            "")),
        "state_name":            str(row.get("state_name",       "")),
        "region":                str(row.get("region",           "")),
        "effective_start_date":  start_date,
        "effective_end_date":    None,
        "is_current":            True,
        "version_number":        version,
    }
