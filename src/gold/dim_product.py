"""
dim_product.py – Build Product Dimension (SCD Type 1) in Gold layer.
"""
import pandas as pd
from sqlalchemy import text

from src.utils.db_utils import get_engine, read_sql
from src.utils.logger import get_logger

logger = get_logger("dim_product")


def build_dim_product(batch_id: str, engine=None) -> dict:
    """
    Upsert products from silver.products → gold.dim_product (SCD Type 1).
    """
    _engine = engine or get_engine()
    logger.info("Building gold.dim_product from silver.products")

    silver = read_sql("SELECT * FROM silver.products", _engine)
    if silver.empty:
        return {"inserted": 0, "updated": 0}

    def size_cat(vol):
        try:
            v = float(vol)
            if v < 500:    return "Small"
            if v < 5000:   return "Medium"
            return "Large"
        except Exception:
            return "Unknown"

    rows = []
    for _, row in silver.iterrows():
        rows.append({
            "product_id":                  str(row.get("product_id",                "")),
            "product_category_name":       str(row.get("product_category_name",     "")),
            "product_category_english":    str(row.get("product_category_english",  "")),
            "product_name_length":         _int(row.get("product_name_length")),
            "product_description_length":  _int(row.get("product_description_length")),
            "product_photos_qty":          _int(row.get("product_photos_qty")),
            "product_weight_g":            _float(row.get("product_weight_g")),
            "product_volume_cm3":          _float(row.get("product_volume_cm3")),
            "size_category":               size_cat(row.get("product_volume_cm3")),
        })

    df = pd.DataFrame(rows)
    cols        = ", ".join(df.columns)
    vals        = ", ".join([f":{c}" for c in df.columns])
    update_cols = [c for c in df.columns if c not in ("product_id", "batch_id")]
    update_set  = ", ".join([f"{c} = EXCLUDED.{c}" for c in update_cols])

    sql = f"""
        INSERT INTO gold.dim_product ({cols})
        VALUES ({vals})
        ON CONFLICT (product_id)
        DO UPDATE SET {update_set}, updated_at = NOW()
    """
    with _engine.begin() as conn:
        conn.execute(text(sql), df.to_dict(orient="records"))

    logger.info(f"  ✔ gold.dim_product | upserted={len(rows)}")
    return {"inserted": len(rows), "updated": 0}


def _int(v):
    try:   return int(float(v))
    except: return None

def _float(v):
    try:   return round(float(v), 2)
    except: return None
