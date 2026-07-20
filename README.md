# 🏗️ Data Engineering PoC – Open Source Data Pipeline Framework

> **End-to-end ETL/ELT pipeline** using Medallion Architecture (Bronze → Silver → Gold) with SCD Type 1 & 2, Great Expectations data quality, Apache Airflow orchestration, and PostgreSQL storage — all containerized with Docker Compose.

---

## 📐 Architecture

```
Source Systems (CSV / JSON / XML)
          │
          ▼
    [Data Ingestion]
    csv_reader.py | json_reader.py | xml_reader.py
          │
          ▼
  ┌───────────────────────────────┐
  │        BRONZE LAYER           │ ← Immutable raw storage
  │  bronze.customers             │   + load_timestamp
  │  bronze.orders                │   + source_file
  │  bronze.products              │   + batch_id
  │  bronze.order_items           │   + load_date
  │  bronze.payments              │
  └───────────────────────────────┘
          │
          ▼
    [Data Validation]
    ✓ Null Checks    ✓ PK Uniqueness
    ✓ Type Checks    ✓ Domain Checks
    ✓ Row Count      ✓ Rejected Handler → bronze.rejected_records
          │
          ▼
  ┌───────────────────────────────┐
  │        SILVER LAYER           │ ← Cleansed & enriched
  │  silver.customers (SCD2)      │   + state_name lookup
  │  silver.orders                │   + derived columns
  │  silver.products (SCD1)       │   + category translation
  │  silver.order_items           │   + effective dates
  │  silver.payments              │   + version_number
  │  silver.sellers (SCD2)        │   + is_current flag
  └───────────────────────────────┘
          │
          ▼
  ┌───────────────────────────────┐
  │         GOLD LAYER            │ ← Analytics-ready
  │  dim_customer (SCD2)          │
  │  dim_product                  │   Window Functions:
  │  dim_seller (SCD2)            │   ✓ Revenue Rank
  │  fact_sales                   │   ✓ Cumulative Revenue
  │  revenue_mart                 │   ✓ MoM Growth %
  │  kpi_summary                  │
  └───────────────────────────────┘
          │
          ▼
     BI / SQL / Dashboards
```

---

## 🛠️ Technology Stack

| Component | Tool | Version |
|-----------|------|---------|
| Processing Engine | Python + pandas + SQLAlchemy | 2.1.4 / 2.0.25 |
| Workflow Orchestration | Apache Airflow | 2.8.1 |
| Database | PostgreSQL | 15 |
| Data Quality | Great Expectations | 0.18.12 |
| Containerization | Docker Compose | v3.8 |
| Fake Data | Faker | 22.2.0 |

---

## 📁 Project Structure

```
data-engineering-poc/
├── docker-compose.yml          # Full infrastructure definition
├── .env                        # Environment variables
├── requirements.txt            # Python dependencies
├── README.md
│
├── data/
│   ├── raw/                    # Auto-generated source files
│   │   ├── customers.csv       # 1,000 customers
│   │   ├── orders.json         # 2,000 orders
│   │   ├── products.xml        # 500 products
│   │   ├── order_items.csv     # 4,000 line items
│   │   ├── sellers.csv         # 100 sellers
│   │   └── payments.json       # 2,500 payment records
│   └── rejected/               # Rejected records (CSV)
│
├── sql/
│   ├── 01_bronze_schema.sql    # Bronze DDL
│   ├── 02_silver_schema.sql    # Silver DDL + Lookup tables
│   ├── 03_gold_schema.sql      # Gold DDL + Date dimension
│   └── 04_metadata_schema.sql  # Metadata, DQ, Watermarks
│
├── src/
│   ├── ingestion/              # Multi-source readers
│   ├── bronze/                 # Bronze loader
│   ├── validation/             # Schema validator + rejected handler
│   ├── silver/                 # Silver transformer + SCD handler
│   ├── gold/                   # Dimensions, Facts, Marts
│   ├── metadata/               # Pipeline run tracker
│   └── utils/                  # DB utils + logger
│
├── dags/
│   ├── dag_bronze_ingestion.py # DAG 01 (daily @00:00)
│   ├── dag_silver_transform.py # DAG 02 (daily @01:00)
│   └── dag_gold_aggregation.py # DAG 03 (daily @02:00)
│
├── scripts/
│   ├── generate_sample_data.py # Generate Olist-like data
│   ├── init_pipeline.py        # Initialize DB schemas
│   └── run_full_pipeline.py    # End-to-end local runner
│
└── architecture/
    └── generate_diagram.py     # Generate architecture PNG
```

---

## 🚀 Quick Start

### Prerequisites
- [Docker Desktop](https://www.docker.com/products/docker-desktop/) installed and running

### 1. Start the full stack

```bash
cd data-engineering-poc
docker-compose up -d
```

This automatically:
1. Starts **PostgreSQL** (port 5432)
2. Generates **sample data** (customers.csv, orders.json, products.xml, ...)
3. Initializes all **4 database schemas** (bronze, silver, gold, metadata)
4. Starts **Airflow** scheduler + webserver (port 8080)

### 2. Access Airflow UI

```
URL:      http://localhost:8080
Username: admin
Password: admin123
```

Trigger any of the 3 DAGs manually or wait for their daily schedule.

### 3. Run pipeline locally (without Airflow)

```bash
# From inside the container or with Python installed locally:
python scripts/run_full_pipeline.py
```

### 4. Connect to PostgreSQL

```
Host:     localhost
Port:     5432
Database: de_poc
Username: de_user
Password: de_password123
```

---

## 📊 Database Schemas

### Bronze Schema (raw)
| Table | Source | Records |
|-------|--------|---------|
| `bronze.customers` | CSV | ~1,000 |
| `bronze.orders` | JSON | ~2,000 |
| `bronze.products` | XML | ~500 |
| `bronze.order_items` | CSV | ~4,000 |
| `bronze.sellers` | CSV | ~100 |
| `bronze.payments` | JSON | ~2,500 |

### Silver Schema (cleansed)
- **SCD Type 2**: `silver.customers`, `silver.sellers`  
  → `effective_start_date`, `effective_end_date`, `is_current`, `version_number`
- **SCD Type 1**: `silver.products` (overwrite)
- **Lookup mapped**: `state_name`, `region`, `product_category_english`
- **Derived**: `delivery_days`, `is_late_delivery`, `total_item_value`, `product_volume_cm3`

### Gold Schema (analytics)
- `dim_customer` — Customer dimension (SCD2, surrogate key)
- `dim_product` — Product dimension (SCD1, surrogate key)
- `dim_seller` — Seller dimension (SCD2, surrogate key)
- `dim_date` — Date dimension (2017–2025, pre-populated)
- `fact_sales` — Grain: order + item, all foreign keys resolved
- `revenue_mart` — Aggregated by month/state/category + window functions
- `kpi_summary` — Pre-computed KPIs for dashboards

### Metadata Schema
- `pipeline_runs` — Every pipeline run logged (status, counts, duration)
- `dq_results` — Every DQ check result persisted
- `watermarks` — Incremental load high-water marks
- `error_log` — Error details with stack traces

---

## 🔄 SCD Implementation

### SCD Type 1 (Products)
```sql
INSERT INTO silver.products (...)
ON CONFLICT (product_id)
DO UPDATE SET col1 = EXCLUDED.col1, ...
```
Current record overwritten. No history retained.

### SCD Type 2 (Customers / Sellers)
```sql
-- Step 1: Expire old record
UPDATE silver.customers
SET is_current = FALSE, effective_end_date = TODAY
WHERE customer_id = 'X' AND is_current = TRUE;

-- Step 2: Insert new version
INSERT INTO silver.customers (...)
VALUES (..., is_current=TRUE, version_number=2, effective_start_date=TODAY);
```

---

## 📋 Sample SQL Queries

### Revenue by Region
```sql
SELECT region, SUM(total_revenue) AS revenue
FROM gold.revenue_mart
GROUP BY region
ORDER BY revenue DESC;
```

### MoM Revenue Growth
```sql
SELECT year, month, state, total_revenue, mom_growth_pct
FROM gold.revenue_mart
WHERE state = 'SP'
ORDER BY year, month;
```

### SCD2 Customer History
```sql
SELECT customer_id, city, state, effective_start_date,
       effective_end_date, is_current, version_number
FROM gold.dim_customer
WHERE customer_id = '<uuid>'
ORDER BY version_number;
```

### Pipeline Audit
```sql
SELECT pipeline_name, status, records_inserted, records_updated,
       records_rejected, duration_seconds
FROM metadata.pipeline_runs
ORDER BY start_time DESC
LIMIT 20;
```

### Data Quality Report
```sql
SELECT table_name, check_name, passed, total_records,
       failed_records, success_pct, severity
FROM metadata.dq_results
ORDER BY checked_at DESC;
```

---

## 🔧 Airflow DAGs

| DAG | Schedule | Dependency |
|-----|----------|------------|
| `01_bronze_ingestion` | Daily 00:00 | None |
| `02_silver_transform` | Daily 01:00 | After Bronze |
| `03_gold_aggregation` | Daily 02:00 | After Silver |

---

## 🧪 Error Handling

| Error Type | Handling |
|------------|----------|
| Schema mismatch | Record rejected → `bronze.rejected_records` + CSV |
| Parsing error | Logged to `metadata.error_log` |
| Null PK | Rejected with reason |
| Duplicate | Rejected (first occurrence kept) |
| DB error | PipelineRun status = FAILED, error logged |

---

## 🛑 Stop Stack

```bash
docker-compose down          # Stop containers
docker-compose down -v       # Stop + remove volumes (clean slate)
```

---

## 📜 License
Open-source PoC for educational purposes.
