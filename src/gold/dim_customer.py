"""
dim_customer.py – Build and maintain the Customer Dimension in Gold layer.
Applies SCD Type 2 from silver.customers → gold.dim_customer.
"""
import pandas as pd
from datetime import date

from sqlalchemy import text

from src.utils.db_utils import get_engine, read_sql
from src.utils.logger import get_logger

logger = get_logger("dim_customer")


def build_dim_customer(batch_id: str, engine=None) -> dict:
    """
    Load current customers from silver and apply SCD Type 2 to gold.dim_customer.
    Returns: dict with insert/update counts
    """
    _engine = engine or get_engine()
    logger.info("Building gold.dim_customer from silver.customers")

    silver = read_sql("SELECT * FROM silver.customers WHERE is_current = TRUE", _engine)
    if silver.empty:
        logger.warning("No silver customers found.")
        return {"inserted": 0, "updated": 0}

    existing = read_sql("SELECT * FROM gold.dim_customer WHERE is_current = TRUE", _engine)
    existing_map = (
        dict(zip(existing["customer_id"].astype(str),
                 existing["customer_key"].astype(int)))
        if not existing.empty else {}
    )

    today = date.today()
    new_rows     = []
    expired_keys = []
    counts = {"inserted": 0, "updated": 0}

    compare_cols = ["city", "state", "zip_code_prefix"]

    for _, row in silver.iterrows():
        cid = str(row["customer_id"])
        if cid not in existing_map:
            rec = _make_gold_row(row, batch_id, today, version=1)
            new_rows.append(rec)
            counts["inserted"] += 1
        else:
            ex = existing[existing["customer_id"].astype(str) == cid].iloc[0]
            if any(str(row.get(c, "")) != str(ex.get(c, "")) for c in compare_cols):
                expired_keys.append(cid)
                rec = _make_gold_row(row, batch_id, today,
                                     version=int(ex.get("version_number", 1)) + 1)
                new_rows.append(rec)
                counts["updated"] += 1

    if expired_keys:
        with _engine.begin() as conn:
            conn.execute(text("""
                UPDATE gold.dim_customer
                SET is_current = FALSE, effective_end_date = :today, updated_at = NOW()
                WHERE customer_id = ANY(:keys) AND is_current = TRUE
            """), {"today": today, "keys": expired_keys})

    if new_rows:
        _insert_gold_rows(new_rows, "dim_customer", "gold", _engine)

    logger.info(f"  ✔ gold.dim_customer | inserted={counts['inserted']} | updated={counts['updated']}")
    return counts


def _make_gold_row(row: pd.Series, batch_id: str, start_date: date, version: int) -> dict:
    return {
        "customer_id":         str(row.get("customer_id",        "")),
        "customer_unique_id":  str(row.get("customer_unique_id", "")),
        "zip_code_prefix":     str(row.get("zip_code_prefix",    "")),
        "city":                str(row.get("city",               "")),
        "state":               str(row.get("state",              "")),
        "state_name":          str(row.get("state_name",         "")),
        "region":              str(row.get("region",             "")),
        "effective_start_date": start_date,
        "effective_end_date":   None,
        "is_current":           True,
        "version_number":       version,
    }


def _insert_gold_rows(rows: list, table: str, schema: str, engine) -> None:
    if not rows:
        return
    df = pd.DataFrame(rows)
    cols = ", ".join(df.columns)
    vals = ", ".join([f":{c}" for c in df.columns])
    sql  = f"INSERT INTO {schema}.{table} ({cols}) VALUES ({vals})"
    with engine.begin() as conn:
        conn.execute(text(sql), df.to_dict(orient="records"))
