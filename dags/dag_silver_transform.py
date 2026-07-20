"""
dag_silver_transform.py
Airflow DAG: Bronze Layer → Silver Layer (cleanse + SCD)
Schedule: Daily at 01:00 (after bronze completes)
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    "owner":            "de_poc",
    "depends_on_past":  False,
    "retries":          2,
    "retry_delay":      timedelta(minutes=5),
    "email_on_failure": False,
}

dag = DAG(
    dag_id="02_silver_transform",
    description="Transform Bronze → Silver with SCD Type 1 and Type 2",
    default_args=default_args,
    schedule_interval="0 1 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["silver", "transform", "scd", "de_poc"],
)


def _get_latest_batch(table: str, engine) -> str:
    """Get the latest batch_id from a bronze table."""
    from sqlalchemy import text
    with engine.connect() as conn:
        r = conn.execute(text(
            f"SELECT batch_id FROM bronze.{table} ORDER BY load_timestamp DESC LIMIT 1"
        ))
        row = r.fetchone()
        return row[0] if row else "UNKNOWN"


def transform_and_load_customers(**context):
    from src.silver.silver_transformer import transform_customers
    from src.silver.scd_handler        import apply_scd_type2
    from src.metadata.metadata_tracker import PipelineRun
    from src.utils.db_utils            import get_engine

    engine   = get_engine()
    batch_id = _get_latest_batch("customers", engine)

    with PipelineRun("silver_customers", layer="silver",
                     source_table="bronze.customers",
                     target_table="silver.customers", triggered_by="airflow") as run:
        df = transform_customers(batch_id, engine)
        if not df.empty:
            counts = apply_scd_type2(
                df, "customers", "silver", "customer_id",
                compare_columns=["city","state","zip_code_prefix"],
                engine=engine,
            )
            run.records_inserted = counts["inserted"]
            run.records_updated  = counts["updated"]
        run.total_records_read = len(df)


def transform_and_load_orders(**context):
    from src.silver.silver_transformer import transform_orders
    from src.metadata.metadata_tracker import PipelineRun
    from src.utils.db_utils            import get_engine, bulk_insert
    from sqlalchemy import text

    engine   = get_engine()
    batch_id = _get_latest_batch("orders", engine)

    with PipelineRun("silver_orders", layer="silver",
                     source_table="bronze.orders",
                     target_table="silver.orders", triggered_by="airflow") as run:
        df = transform_orders(batch_id, engine)
        run.total_records_read = len(df)
        if not df.empty:
            # Upsert orders
            cols     = ", ".join(df.columns)
            vals     = ", ".join([f":{c}" for c in df.columns])
            upd_cols = [c for c in df.columns if c not in ("order_id","created_at")]
            updates  = ", ".join([f"{c} = EXCLUDED.{c}" for c in upd_cols])
            sql = f"""
                INSERT INTO silver.orders ({cols}) VALUES ({vals})
                ON CONFLICT (order_id) DO UPDATE SET {updates}, updated_at = NOW()
            """
            with engine.begin() as conn:
                conn.execute(text(sql), df.to_dict(orient="records"))
            run.records_inserted = len(df)


def transform_and_load_products(**context):
    from src.silver.silver_transformer import transform_products
    from src.silver.scd_handler        import apply_scd_type1
    from src.metadata.metadata_tracker import PipelineRun
    from src.utils.db_utils            import get_engine

    engine   = get_engine()
    batch_id = _get_latest_batch("products", engine)

    with PipelineRun("silver_products", layer="silver",
                     source_table="bronze.products",
                     target_table="silver.products", triggered_by="airflow") as run:
        df = transform_products(batch_id, engine)
        run.total_records_read = len(df)
        if not df.empty:
            counts = apply_scd_type1(df, "products", "silver", "product_id", engine)
            run.records_inserted = counts["inserted"]


def transform_and_load_order_items(**context):
    from src.silver.silver_transformer import transform_order_items
    from src.metadata.metadata_tracker import PipelineRun
    from src.utils.db_utils            import get_engine, bulk_insert

    engine   = get_engine()
    batch_id = _get_latest_batch("order_items", engine)

    with PipelineRun("silver_order_items", layer="silver",
                     source_table="bronze.order_items",
                     target_table="silver.order_items", triggered_by="airflow") as run:
        df = transform_order_items(batch_id, engine)
        run.total_records_read = len(df)
        if not df.empty:
            run.records_inserted = bulk_insert(df, "order_items", "silver", engine)


def transform_and_load_sellers(**context):
    from src.silver.silver_transformer import transform_sellers
    from src.silver.scd_handler        import apply_scd_type2
    from src.metadata.metadata_tracker import PipelineRun
    from src.utils.db_utils            import get_engine

    engine   = get_engine()
    batch_id = _get_latest_batch("sellers", engine)

    with PipelineRun("silver_sellers", layer="silver",
                     source_table="bronze.sellers",
                     target_table="silver.sellers", triggered_by="airflow") as run:
        df = transform_sellers(batch_id, engine)
        run.total_records_read = len(df)
        if not df.empty:
            counts = apply_scd_type2(
                df, "sellers", "silver", "seller_id",
                compare_columns=["city","state","zip_code_prefix"],
                engine=engine,
            )
            run.records_inserted = counts["inserted"]
            run.records_updated  = counts["updated"]


def transform_and_load_payments(**context):
    from src.silver.silver_transformer import transform_payments
    from src.metadata.metadata_tracker import PipelineRun
    from src.utils.db_utils            import get_engine, bulk_insert

    engine   = get_engine()
    batch_id = _get_latest_batch("payments", engine)

    with PipelineRun("silver_payments", layer="silver",
                     source_table="bronze.payments",
                     target_table="silver.payments", triggered_by="airflow") as run:
        df = transform_payments(batch_id, engine)
        run.total_records_read = len(df)
        if not df.empty:
            run.records_inserted = bulk_insert(df, "payments", "silver", engine)


# ─── Tasks ───────────────────────────────────────────────────
t_cust   = PythonOperator(task_id="silver_customers",   python_callable=transform_and_load_customers,   dag=dag)
t_sell   = PythonOperator(task_id="silver_sellers",     python_callable=transform_and_load_sellers,     dag=dag)
t_prod   = PythonOperator(task_id="silver_products",    python_callable=transform_and_load_products,    dag=dag)
t_orders = PythonOperator(task_id="silver_orders",      python_callable=transform_and_load_orders,      dag=dag)
t_items  = PythonOperator(task_id="silver_order_items", python_callable=transform_and_load_order_items, dag=dag)
t_pay    = PythonOperator(task_id="silver_payments",    python_callable=transform_and_load_payments,    dag=dag)

# Dependencies: dimensions first, then transactional
[t_cust, t_sell, t_prod] >> t_orders >> [t_items, t_pay]
