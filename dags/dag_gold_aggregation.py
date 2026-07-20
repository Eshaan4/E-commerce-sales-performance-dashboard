"""
dag_gold_aggregation.py
Airflow DAG: Silver Layer → Gold Layer (Dims + Facts + Marts)
Schedule: Daily at 02:00 (after silver completes)
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
    dag_id="03_gold_aggregation",
    description="Build Gold layer: Dimensions, Facts, Revenue Mart, KPI Summary",
    default_args=default_args,
    schedule_interval="0 2 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["gold", "aggregation", "de_poc"],
)


def build_dim_customer_task(**context):
    from src.gold.dim_customer         import build_dim_customer
    from src.metadata.metadata_tracker import PipelineRun
    from src.utils.db_utils            import get_engine

    engine = get_engine()
    with PipelineRun("gold_dim_customer", layer="gold",
                     source_table="silver.customers",
                     target_table="gold.dim_customer", triggered_by="airflow") as run:
        counts = build_dim_customer("GOLD_RUN", engine)
        run.records_inserted = counts["inserted"]
        run.records_updated  = counts["updated"]


def build_dim_product_task(**context):
    from src.gold.dim_product          import build_dim_product
    from src.metadata.metadata_tracker import PipelineRun
    from src.utils.db_utils            import get_engine

    engine = get_engine()
    with PipelineRun("gold_dim_product", layer="gold",
                     source_table="silver.products",
                     target_table="gold.dim_product", triggered_by="airflow") as run:
        counts = build_dim_product("GOLD_RUN", engine)
        run.records_inserted = counts["inserted"]


def build_dim_seller_task(**context):
    from src.gold.dim_seller           import build_dim_seller
    from src.metadata.metadata_tracker import PipelineRun
    from src.utils.db_utils            import get_engine

    engine = get_engine()
    with PipelineRun("gold_dim_seller", layer="gold",
                     source_table="silver.sellers",
                     target_table="gold.dim_seller", triggered_by="airflow") as run:
        counts = build_dim_seller("GOLD_RUN", engine)
        run.records_inserted = counts["inserted"]
        run.records_updated  = counts["updated"]


def build_fact_sales_task(**context):
    from src.gold.fact_sales           import build_fact_sales
    from src.metadata.metadata_tracker import PipelineRun
    from src.utils.db_utils            import get_engine

    engine = get_engine()
    with PipelineRun("gold_fact_sales", layer="gold",
                     source_table="silver.order_items + silver.orders",
                     target_table="gold.fact_sales", triggered_by="airflow") as run:
        counts = build_fact_sales("GOLD_RUN", engine)
        run.records_inserted = counts["inserted"]


def build_revenue_mart_task(**context):
    from src.gold.revenue_mart         import build_revenue_mart
    from src.metadata.metadata_tracker import PipelineRun
    from src.utils.db_utils            import get_engine

    engine = get_engine()
    with PipelineRun("gold_revenue_mart", layer="gold",
                     source_table="gold.fact_sales",
                     target_table="gold.revenue_mart", triggered_by="airflow") as run:
        rows = build_revenue_mart(engine)
        run.records_inserted = rows


def build_kpi_summary_task(**context):
    from src.gold.kpi_summary          import build_kpi_summary
    from src.metadata.metadata_tracker import PipelineRun
    from src.utils.db_utils            import get_engine

    engine = get_engine()
    with PipelineRun("gold_kpi_summary", layer="gold",
                     source_table="gold.fact_sales + gold.revenue_mart",
                     target_table="gold.kpi_summary", triggered_by="airflow") as run:
        count = build_kpi_summary(engine)
        run.records_inserted = count


# ─── Tasks ───────────────────────────────────────────────────
t_dim_cust  = PythonOperator(task_id="dim_customer",   python_callable=build_dim_customer_task,  dag=dag)
t_dim_prod  = PythonOperator(task_id="dim_product",    python_callable=build_dim_product_task,   dag=dag)
t_dim_sell  = PythonOperator(task_id="dim_seller",     python_callable=build_dim_seller_task,    dag=dag)
t_fact      = PythonOperator(task_id="fact_sales",     python_callable=build_fact_sales_task,    dag=dag)
t_revenue   = PythonOperator(task_id="revenue_mart",   python_callable=build_revenue_mart_task,  dag=dag)
t_kpi       = PythonOperator(task_id="kpi_summary",    python_callable=build_kpi_summary_task,   dag=dag)

# Dims first → fact → marts → KPIs
[t_dim_cust, t_dim_prod, t_dim_sell] >> t_fact >> t_revenue >> t_kpi
