-- =============================================================
-- 03_gold_schema.sql  –  Gold Layer (Dimensional Model)
-- =============================================================

-- ─────────────────────────────────────────
-- Gold: Customer Dimension (SCD Type 2)
-- ─────────────────────────────────────────
CREATE TABLE IF NOT EXISTS gold.dim_customer (
    customer_key            BIGSERIAL PRIMARY KEY,   -- Surrogate key
    customer_id             VARCHAR(100)    NOT NULL,
    customer_unique_id      VARCHAR(100),
    zip_code_prefix         VARCHAR(10),
    city                    VARCHAR(100),
    state                   VARCHAR(10),
    state_name              VARCHAR(100),
    region                  VARCHAR(50),
    -- SCD Type 2
    effective_start_date    DATE            NOT NULL,
    effective_end_date      DATE,
    is_current              BOOLEAN         DEFAULT TRUE,
    version_number          INT             DEFAULT 1,
    -- Audit
    created_at              TIMESTAMP       DEFAULT NOW(),
    updated_at              TIMESTAMP       DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_dim_customer_id ON gold.dim_customer(customer_id);
CREATE INDEX IF NOT EXISTS idx_dim_customer_current ON gold.dim_customer(is_current);

-- ─────────────────────────────────────────
-- Gold: Product Dimension
-- ─────────────────────────────────────────
CREATE TABLE IF NOT EXISTS gold.dim_product (
    product_key                 BIGSERIAL PRIMARY KEY,   -- Surrogate key
    product_id                  VARCHAR(100)    NOT NULL UNIQUE,
    product_category_name       VARCHAR(100),
    product_category_english    VARCHAR(100),
    product_name_length         INT,
    product_description_length  INT,
    product_photos_qty          INT,
    product_weight_g            NUMERIC(10,2),
    product_volume_cm3          NUMERIC(12,2),
    size_category               VARCHAR(20),    -- Derived: Small/Medium/Large
    -- Audit
    created_at                  TIMESTAMP       DEFAULT NOW(),
    updated_at                  TIMESTAMP       DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_dim_product_id ON gold.dim_product(product_id);
CREATE INDEX IF NOT EXISTS idx_dim_product_category ON gold.dim_product(product_category_english);

-- ─────────────────────────────────────────
-- Gold: Seller Dimension (SCD Type 2)
-- ─────────────────────────────────────────
CREATE TABLE IF NOT EXISTS gold.dim_seller (
    seller_key              BIGSERIAL PRIMARY KEY,   -- Surrogate key
    seller_id               VARCHAR(100)    NOT NULL,
    zip_code_prefix         VARCHAR(10),
    city                    VARCHAR(100),
    state                   VARCHAR(10),
    state_name              VARCHAR(100),
    region                  VARCHAR(50),
    -- SCD Type 2
    effective_start_date    DATE            NOT NULL,
    effective_end_date      DATE,
    is_current              BOOLEAN         DEFAULT TRUE,
    version_number          INT             DEFAULT 1,
    -- Audit
    created_at              TIMESTAMP       DEFAULT NOW(),
    updated_at              TIMESTAMP       DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_dim_seller_id ON gold.dim_seller(seller_id);

-- ─────────────────────────────────────────
-- Gold: Date Dimension
-- ─────────────────────────────────────────
CREATE TABLE IF NOT EXISTS gold.dim_date (
    date_key            INT         PRIMARY KEY,   -- YYYYMMDD
    full_date           DATE        NOT NULL UNIQUE,
    year                INT,
    quarter             INT,
    month               INT,
    month_name          VARCHAR(20),
    week_of_year        INT,
    day_of_month        INT,
    day_of_week         INT,
    day_name            VARCHAR(20),
    is_weekend          BOOLEAN,
    is_month_end        BOOLEAN
);

-- Populate date dimension (2017-2024)
INSERT INTO gold.dim_date
SELECT
    TO_CHAR(d, 'YYYYMMDD')::INT                    AS date_key,
    d                                               AS full_date,
    EXTRACT(YEAR FROM d)::INT                       AS year,
    EXTRACT(QUARTER FROM d)::INT                    AS quarter,
    EXTRACT(MONTH FROM d)::INT                      AS month,
    TO_CHAR(d, 'Month')                             AS month_name,
    EXTRACT(WEEK FROM d)::INT                       AS week_of_year,
    EXTRACT(DAY FROM d)::INT                        AS day_of_month,
    EXTRACT(DOW FROM d)::INT                        AS day_of_week,
    TO_CHAR(d, 'Day')                               AS day_name,
    CASE WHEN EXTRACT(DOW FROM d) IN (0,6)
         THEN TRUE ELSE FALSE END                   AS is_weekend,
    (d = DATE_TRUNC('month', d) + INTERVAL '1 month - 1 day')::BOOLEAN AS is_month_end
FROM GENERATE_SERIES('2017-01-01'::DATE, '2025-12-31'::DATE, '1 day'::INTERVAL) AS d
ON CONFLICT DO NOTHING;

-- ─────────────────────────────────────────
-- Gold: Sales Fact Table
-- ─────────────────────────────────────────
CREATE TABLE IF NOT EXISTS gold.fact_sales (
    sales_key               BIGSERIAL   PRIMARY KEY,
    order_id                VARCHAR(100) NOT NULL,
    order_item_id           INT,
    -- Foreign keys (surrogate)
    customer_key            BIGINT      REFERENCES gold.dim_customer(customer_key),
    product_key             BIGINT      REFERENCES gold.dim_product(product_key),
    seller_key              BIGINT      REFERENCES gold.dim_seller(seller_key),
    order_date_key          INT         REFERENCES gold.dim_date(date_key),
    delivery_date_key       INT         REFERENCES gold.dim_date(date_key),
    -- Measures
    price                   NUMERIC(12,2),
    freight_value           NUMERIC(12,2),
    total_item_value        NUMERIC(12,2),
    payment_value           NUMERIC(12,2),
    payment_type            VARCHAR(50),
    payment_installments    INT,
    -- Derived measures
    delivery_days           INT,
    is_late_delivery        BOOLEAN,
    order_status            VARCHAR(50),
    -- Audit
    batch_id                VARCHAR(100),
    created_at              TIMESTAMP   DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_fact_sales_order ON gold.fact_sales(order_id);
CREATE INDEX IF NOT EXISTS idx_fact_sales_customer ON gold.fact_sales(customer_key);
CREATE INDEX IF NOT EXISTS idx_fact_sales_product ON gold.fact_sales(product_key);
CREATE INDEX IF NOT EXISTS idx_fact_sales_date ON gold.fact_sales(order_date_key);

-- ─────────────────────────────────────────
-- Gold: Revenue Mart
-- ─────────────────────────────────────────
CREATE TABLE IF NOT EXISTS gold.revenue_mart (
    mart_id             BIGSERIAL   PRIMARY KEY,
    year                INT,
    month               INT,
    month_name          VARCHAR(20),
    state               VARCHAR(10),
    region              VARCHAR(50),
    product_category    VARCHAR(100),
    total_orders        INT,
    total_revenue       NUMERIC(14,2),
    total_freight       NUMERIC(14,2),
    avg_order_value     NUMERIC(12,2),
    total_items_sold    INT,
    -- Window function metrics
    revenue_rank_in_month INT,
    cumulative_revenue  NUMERIC(16,2),
    prev_month_revenue  NUMERIC(14,2),
    mom_growth_pct      NUMERIC(8,2),    -- Month-over-month growth %
    -- Audit
    computed_at         TIMESTAMP       DEFAULT NOW()
);

-- ─────────────────────────────────────────
-- Gold: KPI Summary
-- ─────────────────────────────────────────
CREATE TABLE IF NOT EXISTS gold.kpi_summary (
    kpi_id              BIGSERIAL   PRIMARY KEY,
    kpi_name            VARCHAR(100),
    kpi_value           NUMERIC(16,4),
    kpi_unit            VARCHAR(50),
    dimension           VARCHAR(100),
    dimension_value     VARCHAR(100),
    as_of_date          DATE        DEFAULT CURRENT_DATE,
    computed_at         TIMESTAMP   DEFAULT NOW()
);

COMMENT ON SCHEMA gold IS 'Gold Layer: Analytics-ready dimensional model with fact and dimension tables';
