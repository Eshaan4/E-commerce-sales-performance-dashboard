"""
dag_bronze_ingestion.py
Airflow DAG: Source files → Bronze Layer
Schedule: Daily at midnight
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

# ─── Default args ────────────────────────────────────────────
default_args = {
    "owner":            "de_poc",
    "depends_on_past":  False,
    "retries":          2,
    "retry_delay":      timedelta(minutes=5),
    "email_on_failure": False,
}

# ─── DAG definition ──────────────────────────────────────────
dag = DAG(
    dag_id="01_bronze_ingestion",
    description="Ingest raw source data into Bronze layer",
    default_args=default_args,
    schedule_interval="0 0 * * *",   # Daily midnight
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["bronze", "ingestion", "de_poc"],
)


# ─── Task functions ──────────────────────────────────────────
def ingest_customers(**context):
    from src.ingestion.csv_reader   import read_csv_source
    from src.bronze.bronze_loader   import ingest_customers_to_bronze
    from src.validation.schema_validator import DataValidator
    from src.validation.rejected_handler import save_rejected_records
    from src.metadata.metadata_tracker  import PipelineRun
    from src.utils.db_utils import get_engine

    engine = get_engine()
    with PipelineRun("bronze_customers", layer="bronze",
                     source_system="CSV", source_table="customers",
                     target_table="bronze.customers", triggered_by="airflow") as run:
        df, source_file = read_csv_source("customers.csv")
        run.total_records_read = len(df)

        v = DataValidator(df, "bronze.customers", run.batch_id, engine)
        v.check_nulls(["customer_id"])
        v.check_unique(["customer_id"])
        valid, rejected = v.finalize()

        run.records_rejected = save_rejected_records(rejected, "customers", run.batch_id, engine)
        run.records_inserted = ingest_customers_to_bronze(valid, source_file, run.batch_id, engine)

    context["ti"].xcom_push(key="batch_id", value=run.batch_id)


def ingest_sellers(**context):
    from src.ingestion.csv_reader   import read_csv_source
    from src.bronze.bronze_loader   import ingest_sellers_to_bronze
    from src.validation.schema_validator import DataValidator
    from src.validation.rejected_handler import save_rejected_records
    from src.metadata.metadata_tracker  import PipelineRun
    from src.utils.db_utils import get_engine

    engine = get_engine()
    with PipelineRun("bronze_sellers", layer="bronze",
                     source_system="CSV", source_table="sellers",
                     target_table="bronze.sellers", triggered_by="airflow") as run:
        df, source_file = read_csv_source("sellers.csv")
        run.total_records_read = len(df)

        v = DataValidator(df, "bronze.sellers", run.batch_id, engine)
        v.check_nulls(["seller_id"])
        v.check_unique(["seller_id"])
        valid, rejected = v.finalize()

        run.records_rejected = save_rejected_records(rejected, "sellers", run.batch_id, engine)
        run.records_inserted = ingest_sellers_to_bronze(valid, source_file, run.batch_id, engine)


def ingest_products(**context):
    from src.ingestion.xml_reader   import read_xml_source
    from src.bronze.bronze_loader   import ingest_products_to_bronze
    from src.validation.schema_validator import DataValidator
    from src.validation.rejected_handler import save_rejected_records
    from src.metadata.metadata_tracker  import PipelineRun
    from src.utils.db_utils import get_engine

    engine = get_engine()
    with PipelineRun("bronze_products", layer="bronze",
                     source_system="XML", source_table="products",
                     target_table="bronze.products", triggered_by="airflow") as run:
        df, source_file = read_xml_source("products.xml", record_tag="product")
        run.total_records_read = len(df)

        v = DataValidator(df, "bronze.products", run.batch_id, engine)
        v.check_nulls(["product_id"])
        v.check_unique(["product_id"])
        v.check_numeric(["product_weight_g","product_length_cm"])
        valid, rejected = v.finalize()

        run.records_rejected = save_rejected_records(rejected, "products", run.batch_id, engine)
        run.records_inserted = ingest_products_to_bronze(valid, source_file, run.batch_id, engine)


def ingest_orders(**context):
    from src.ingestion.json_reader  import read_json_source
    from src.bronze.bronze_loader   import ingest_orders_to_bronze
    from src.validation.schema_validator import DataValidator
    from src.validation.rejected_handler import save_rejected_records
    from src.metadata.metadata_tracker  import PipelineRun
    from src.utils.db_utils import get_engine

    engine = get_engine()
    with PipelineRun("bronze_orders", layer="bronze",
                     source_system="JSON", source_table="orders",
                     target_table="bronze.orders", triggered_by="airflow") as run:
        df, source_file = read_json_source("orders.json")
        run.total_records_read = len(df)

        v = DataValidator(df, "bronze.orders", run.batch_id, engine)
        v.check_nulls(["order_id","customer_id"])
        v.check_unique(["order_id"])
        v.check_value_in_set("order_status",
            ["delivered","shipped","canceled","invoiced","processing","created","unavailable"])
        valid, rejected = v.finalize()

        run.records_rejected = save_rejected_records(rejected, "orders", run.batch_id, engine)
        run.records_inserted = ingest_orders_to_bronze(valid, source_file, run.batch_id, engine)

    context["ti"].xcom_push(key="orders_batch_id", value=run.batch_id)


def ingest_order_items(**context):
    from src.ingestion.csv_reader   import read_csv_source
    from src.bronze.bronze_loader   import ingest_order_items_to_bronze
    from src.validation.schema_validator import DataValidator
    from src.validation.rejected_handler import save_rejected_records
    from src.metadata.metadata_tracker  import PipelineRun
    from src.utils.db_utils import get_engine

    engine = get_engine()
    with PipelineRun("bronze_order_items", layer="bronze",
                     source_system="CSV", source_table="order_items",
                     target_table="bronze.order_items", triggered_by="airflow") as run:
        df, source_file = read_csv_source("order_items.csv")
        run.total_records_read = len(df)

        v = DataValidator(df, "bronze.order_items", run.batch_id, engine)
        v.check_nulls(["order_id","product_id","seller_id"])
        v.check_numeric(["price","freight_value"])
        valid, rejected = v.finalize()

        run.records_rejected = save_rejected_records(rejected, "order_items", run.batch_id, engine)
        run.records_inserted = ingest_order_items_to_bronze(valid, source_file, run.batch_id, engine)


def ingest_payments(**context):
    from src.ingestion.json_reader  import read_json_source
    from src.bronze.bronze_loader   import ingest_payments_to_bronze
    from src.validation.schema_validator import DataValidator
    from src.validation.rejected_handler import save_rejected_records
    from src.metadata.metadata_tracker  import PipelineRun
    from src.utils.db_utils import get_engine

    engine = get_engine()
    with PipelineRun("bronze_payments", layer="bronze",
                     source_system="JSON", source_table="payments",
                     target_table="bronze.payments", triggered_by="airflow") as run:
        df, source_file = read_json_source("payments.json")
        run.total_records_read = len(df)

        v = DataValidator(df, "bronze.payments", run.batch_id, engine)
        v.check_nulls(["order_id"])
        v.check_numeric(["payment_value"])
        valid, rejected = v.finalize()

        run.records_rejected = save_rejected_records(rejected, "payments", run.batch_id, engine)
        run.records_inserted = ingest_payments_to_bronze(valid, source_file, run.batch_id, engine)


# ─── Task definitions ─────────────────────────────────────────
t_customers   = PythonOperator(task_id="ingest_customers",   python_callable=ingest_customers,   dag=dag)
t_sellers     = PythonOperator(task_id="ingest_sellers",     python_callable=ingest_sellers,     dag=dag)
t_products    = PythonOperator(task_id="ingest_products",    python_callable=ingest_products,    dag=dag)
t_orders      = PythonOperator(task_id="ingest_orders",      python_callable=ingest_orders,      dag=dag)
t_order_items = PythonOperator(task_id="ingest_order_items", python_callable=ingest_order_items, dag=dag)
t_payments    = PythonOperator(task_id="ingest_payments",    python_callable=ingest_payments,    dag=dag)

# ─── Task dependencies ────────────────────────────────────────
# Customers & Sellers first (referenced by orders)
[t_customers, t_sellers, t_products] >> t_orders >> [t_order_items, t_payments]
