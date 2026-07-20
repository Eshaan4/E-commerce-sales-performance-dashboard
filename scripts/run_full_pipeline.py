"""
run_full_pipeline.py
End-to-end pipeline runner: Bronze → Silver → Gold
Works both inside Docker and locally.
"""
import os
import sys
import time
from pathlib import Path
from datetime import datetime

# ── Path setup ────────────────────────────────────────────────
# Works from inside Docker (/opt/airflow) or locally (project root)
_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(_ROOT))
sys.path.insert(0, "/opt/airflow")   # Docker path

# Load .env when running locally (Docker injects env vars directly)
try:
    from dotenv import load_dotenv
    load_dotenv(_ROOT / ".env")
except ImportError:
    pass

# ── Imports ────────────────────────────────────────────────────
from src.utils.db_utils            import get_engine, bulk_insert, read_sql
from src.utils.logger              import get_logger, log_separator
from src.metadata.metadata_tracker import generate_batch_id

from src.ingestion.csv_reader  import read_csv_source
from src.ingestion.json_reader import read_json_source
from src.ingestion.xml_reader  import read_xml_source

from src.bronze.bronze_loader import (
    ingest_customers_to_bronze,
    ingest_orders_to_bronze,
    ingest_products_to_bronze,
    ingest_order_items_to_bronze,
    ingest_sellers_to_bronze,
    ingest_payments_to_bronze,
)
from src.validation.schema_validator import DataValidator
from src.validation.rejected_handler import save_rejected_records

from src.silver.silver_transformer import (
    transform_customers, transform_orders, transform_products,
    transform_order_items, transform_sellers, transform_payments,
)
from src.silver.scd_handler import apply_scd_type1, apply_scd_type2

from src.gold.dim_customer import build_dim_customer
from src.gold.dim_product  import build_dim_product
from src.gold.dim_seller   import build_dim_seller
from src.gold.fact_sales   import build_fact_sales
from src.gold.revenue_mart import build_revenue_mart
from src.gold.kpi_summary  import build_kpi_summary

from sqlalchemy import text

logger = get_logger("run_full_pipeline")


# ─────────────────────────────────────────────────────────────
# PHASE 1 – BRONZE
# ─────────────────────────────────────────────────────────────
def run_bronze(engine, batch_id: str) -> int:
    log_separator(logger, "PHASE 1 – BRONZE INGESTION")

    sources = [
        # (filename, format, bronze_table, loader_fn, pk_cols_for_validation)
        ("customers.csv",   "csv",  "customers",   ingest_customers_to_bronze,   ["customer_id"]),
        ("sellers.csv",     "csv",  "sellers",     ingest_sellers_to_bronze,     ["seller_id"]),
        ("products.xml",    "xml",  "products",    ingest_products_to_bronze,    ["product_id"]),
        ("orders.json",     "json", "orders",      ingest_orders_to_bronze,      ["order_id", "customer_id"]),
        ("order_items.csv", "csv",  "order_items", ingest_order_items_to_bronze, ["order_id"]),
        ("payments.json",   "json", "payments",    ingest_payments_to_bronze,    ["order_id"]),
    ]

    total = 0
    for filename, fmt, table, loader, pk_cols in sources:
        try:
            logger.info(f"\n→ Ingesting  {filename}")

            if fmt == "csv":
                df, src = read_csv_source(filename)
            elif fmt == "json":
                df, src = read_json_source(filename)
            else:  # xml  — record_tag is singular: "product"
                df, src = read_xml_source(filename, record_tag="product")

            # Validate
            v = DataValidator(df, f"bronze.{table}", batch_id, engine)
            v.check_nulls(pk_cols)
            if len(pk_cols) == 1:
                v.check_unique(pk_cols)
            valid, rejected = v.finalize()

            save_rejected_records(rejected, table, batch_id, engine)
            rows = loader(valid, src, batch_id, engine)
            total += rows

        except Exception as exc:
            logger.error(f"  ✗ {filename}: {exc}")

    logger.info(f"\n  BRONZE TOTAL: {total:,} rows loaded")
    return total


# ─────────────────────────────────────────────────────────────
# PHASE 2 – SILVER
# ─────────────────────────────────────────────────────────────
def run_silver(engine, batch_id: str) -> int:
    log_separator(logger, "PHASE 2 – SILVER TRANSFORMATION")
    total = 0

    # (table, transformer, mode, natural_key, compare_cols)
    transforms = [
        ("customers",   transform_customers,   "scd2",   "customer_id", ["city", "state", "zip_code_prefix"]),
        ("sellers",     transform_sellers,     "scd2",   "seller_id",   ["city", "state", "zip_code_prefix"]),
        ("products",    transform_products,    "scd1",   "product_id",  []),
        ("orders",      transform_orders,      "upsert", "order_id",    []),
        ("order_items", transform_order_items, "insert", "",            []),
        ("payments",    transform_payments,    "insert", "",            []),
    ]

    for table, transformer, mode, nk, compare_cols in transforms:
        try:
            logger.info(f"\n→ Transforming  silver.{table}  [{mode.upper()}]")
            df = transformer(batch_id, engine)
            if df.empty:
                logger.warning(f"  No rows returned for silver.{table} — skipping")
                continue

            if mode == "scd2":
                c = apply_scd_type2(df, table, "silver", nk, compare_cols, engine)
                total += c["inserted"] + c["updated"]

            elif mode == "scd1":
                c = apply_scd_type1(df, table, "silver", nk, engine)
                total += c["inserted"]

            elif mode == "upsert":
                # orders has UNIQUE(order_id) → safe to upsert
                cols    = list(df.columns)
                col_str = ", ".join(cols)
                val_str = ", ".join([f":{c}" for c in cols])
                upd_cols = [c for c in cols if c not in (nk, "created_at")]
                upd_str  = ", ".join([f"{c} = EXCLUDED.{c}" for c in upd_cols])
                sql = (
                    f"INSERT INTO silver.{table} ({col_str}) VALUES ({val_str}) "
                    f"ON CONFLICT ({nk}) DO UPDATE SET {upd_str}, updated_at = NOW()"
                )
                with engine.begin() as conn:
                    conn.execute(text(sql), df.to_dict(orient="records"))
                total += len(df)

            else:  # plain insert (order_items, payments — no unique constraint)
                rows = bulk_insert(df, table, "silver", engine)
                total += rows

        except Exception as exc:
            logger.error(f"  ✗ silver.{table}: {exc}")

    logger.info(f"\n  SILVER TOTAL: {total:,} rows processed")
    return total


# ─────────────────────────────────────────────────────────────
# PHASE 3 – GOLD
# ─────────────────────────────────────────────────────────────
def run_gold(engine) -> int:
    log_separator(logger, "PHASE 3 – GOLD AGGREGATION")
    total = 0

    steps = [
        ("dim_customer",  lambda: build_dim_customer("FULL_RUN", engine)),
        ("dim_product",   lambda: build_dim_product("FULL_RUN",  engine)),
        ("dim_seller",    lambda: build_dim_seller("FULL_RUN",   engine)),
        ("fact_sales",    lambda: build_fact_sales("FULL_RUN",   engine)),
        ("revenue_mart",  lambda: build_revenue_mart(engine)),
        ("kpi_summary",   lambda: build_kpi_summary(engine)),
    ]

    for name, fn in steps:
        try:
            logger.info(f"\n→ Building  gold.{name}")
            result = fn()
            if isinstance(result, dict):
                n = result.get("inserted", 0) + result.get("updated", 0)
            else:
                n = int(result or 0)
            total += n
        except Exception as exc:
            logger.error(f"  ✗ gold.{name}: {exc}")

    logger.info(f"\n  GOLD TOTAL: {total:,} rows processed")
    return total


# ─────────────────────────────────────────────────────────────
# Summary Report
# ─────────────────────────────────────────────────────────────
def print_summary(engine) -> None:
    log_separator(logger, "PIPELINE SUMMARY REPORT")

    check_tables = [
        ("bronze", "customers"), ("bronze", "orders"), ("bronze", "products"),
        ("bronze", "order_items"), ("bronze", "sellers"), ("bronze", "payments"),
        ("silver", "customers"), ("silver", "orders"), ("silver", "products"),
        ("silver", "order_items"),
        ("gold",   "dim_customer"), ("gold", "dim_product"), ("gold", "dim_seller"),
        ("gold",   "fact_sales"), ("gold", "revenue_mart"), ("gold", "kpi_summary"),
    ]

    print(f"\n  {'Table':<40} {'Rows':>8}")
    print(f"  {'─'*40} {'─'*8}")
    for schema, table in check_tables:
        try:
            r = read_sql(f"SELECT COUNT(*) AS c FROM {schema}.{table}", engine)
            print(f"  {schema + '.' + table:<40} {r['c'].iloc[0]:>8,}")
        except Exception:
            print(f"  {schema + '.' + table:<40} {'ERR':>8}")

    # Recent pipeline runs
    try:
        runs = read_sql("""
            SELECT pipeline_name,
                   status,
                   records_inserted  AS ins,
                   records_updated   AS upd,
                   records_rejected  AS rej,
                   ROUND(duration_seconds, 1) AS secs
            FROM metadata.pipeline_runs
            ORDER BY start_time DESC
            LIMIT 12
        """, engine)
        print(f"\n  RECENT PIPELINE RUNS:\n")
        print(runs.to_string(index=False))
    except Exception:
        pass

    # DQ summary
    try:
        dq = read_sql("""
            SELECT table_name,
                   SUM(CASE WHEN passed THEN 1 ELSE 0 END) AS passed,
                   SUM(CASE WHEN NOT passed THEN 1 ELSE 0 END) AS failed
            FROM metadata.dq_results
            GROUP BY table_name
            ORDER BY table_name
        """, engine)
        if not dq.empty:
            print(f"\n  DATA QUALITY SUMMARY:\n")
            print(dq.to_string(index=False))
    except Exception:
        pass


# ─────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────
if __name__ == "__main__":
    start    = datetime.now()
    batch_id = generate_batch_id("FULL_PIPELINE")

    print("\n" + "═" * 65)
    print("  DATA ENGINEERING POC – Full Pipeline Run")
    print(f"  Batch ID : {batch_id}")
    print(f"  Started  : {start.strftime('%Y-%m-%d %H:%M:%S')}")
    print("═" * 65)

    engine = get_engine()

    b = run_bronze(engine, batch_id)
    time.sleep(1)

    s = run_silver(engine, batch_id)
    time.sleep(1)

    g = run_gold(engine)

    print_summary(engine)

    elapsed = (datetime.now() - start).total_seconds()
    print(f"\n{'═' * 65}")
    print(f"  ✔  Pipeline complete in {elapsed:.1f}s")
    print(f"     Bronze rows : {b:,}")
    print(f"     Silver rows : {s:,}")
    print(f"     Gold rows   : {g:,}")
    print(f"{'═' * 65}\n")
