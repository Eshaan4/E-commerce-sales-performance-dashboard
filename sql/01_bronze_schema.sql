-- =============================================================
-- 01_bronze_schema.sql  –  Bronze Layer (Raw / Immutable)
-- =============================================================

CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;
CREATE SCHEMA IF NOT EXISTS metadata;

-- ─────────────────────────────────────────
-- Bronze: Customers (from CSV)
-- ─────────────────────────────────────────
CREATE TABLE IF NOT EXISTS bronze.customers (
    bronze_id           BIGSERIAL PRIMARY KEY,
    customer_id         VARCHAR(100),
    customer_unique_id  VARCHAR(100),
    zip_code_prefix     VARCHAR(10),
    city                VARCHAR(100),
    state               VARCHAR(10),
    -- Audit columns
    source_file         VARCHAR(255),
    source_system       VARCHAR(50)  DEFAULT 'CSV',
    batch_id            VARCHAR(100),
    load_timestamp      TIMESTAMP    DEFAULT NOW(),
    load_date           DATE         DEFAULT CURRENT_DATE,
    is_rejected         BOOLEAN      DEFAULT FALSE,
    reject_reason       TEXT
);

-- ─────────────────────────────────────────
-- Bronze: Orders (from JSON)
-- ─────────────────────────────────────────
CREATE TABLE IF NOT EXISTS bronze.orders (
    bronze_id               BIGSERIAL PRIMARY KEY,
    order_id                VARCHAR(100),
    customer_id             VARCHAR(100),
    order_status            VARCHAR(50),
    order_purchase_timestamp VARCHAR(50),
    order_approved_at       VARCHAR(50),
    order_delivered_at      VARCHAR(50),
    order_estimated_delivery VARCHAR(50),
    -- Audit columns
    source_file             VARCHAR(255),
    source_system           VARCHAR(50)  DEFAULT 'JSON',
    batch_id                VARCHAR(100),
    load_timestamp          TIMESTAMP    DEFAULT NOW(),
    load_date               DATE         DEFAULT CURRENT_DATE,
    is_rejected             BOOLEAN      DEFAULT FALSE,
    reject_reason           TEXT
);

-- ─────────────────────────────────────────
-- Bronze: Products (from XML)
-- ─────────────────────────────────────────
CREATE TABLE IF NOT EXISTS bronze.products (
    bronze_id               BIGSERIAL PRIMARY KEY,
    product_id              VARCHAR(100),
    product_category_name   VARCHAR(100),
    product_name_length     VARCHAR(20),
    product_description_length VARCHAR(20),
    product_photos_qty      VARCHAR(20),
    product_weight_g        VARCHAR(20),
    product_length_cm       VARCHAR(20),
    product_height_cm       VARCHAR(20),
    product_width_cm        VARCHAR(20),
    -- Audit columns
    source_file             VARCHAR(255),
    source_system           VARCHAR(50)  DEFAULT 'XML',
    batch_id                VARCHAR(100),
    load_timestamp          TIMESTAMP    DEFAULT NOW(),
    load_date               DATE         DEFAULT CURRENT_DATE,
    is_rejected             BOOLEAN      DEFAULT FALSE,
    reject_reason           TEXT
);

-- ─────────────────────────────────────────
-- Bronze: Order Items (from CSV)
-- ─────────────────────────────────────────
CREATE TABLE IF NOT EXISTS bronze.order_items (
    bronze_id               BIGSERIAL PRIMARY KEY,
    order_id                VARCHAR(100),
    order_item_id           VARCHAR(20),
    product_id              VARCHAR(100),
    seller_id               VARCHAR(100),
    shipping_limit_date     VARCHAR(50),
    price                   VARCHAR(30),
    freight_value           VARCHAR(30),
    -- Audit columns
    source_file             VARCHAR(255),
    source_system           VARCHAR(50)  DEFAULT 'CSV',
    batch_id                VARCHAR(100),
    load_timestamp          TIMESTAMP    DEFAULT NOW(),
    load_date               DATE         DEFAULT CURRENT_DATE,
    is_rejected             BOOLEAN      DEFAULT FALSE,
    reject_reason           TEXT
);

-- ─────────────────────────────────────────
-- Bronze: Sellers (from CSV)
-- ─────────────────────────────────────────
CREATE TABLE IF NOT EXISTS bronze.sellers (
    bronze_id               BIGSERIAL PRIMARY KEY,
    seller_id               VARCHAR(100),
    zip_code_prefix         VARCHAR(10),
    city                    VARCHAR(100),
    state                   VARCHAR(10),
    -- Audit columns
    source_file             VARCHAR(255),
    source_system           VARCHAR(50)  DEFAULT 'CSV',
    batch_id                VARCHAR(100),
    load_timestamp          TIMESTAMP    DEFAULT NOW(),
    load_date               DATE         DEFAULT CURRENT_DATE,
    is_rejected             BOOLEAN      DEFAULT FALSE,
    reject_reason           TEXT
);

-- ─────────────────────────────────────────
-- Bronze: Payments (from JSON)
-- ─────────────────────────────────────────
CREATE TABLE IF NOT EXISTS bronze.payments (
    bronze_id               BIGSERIAL PRIMARY KEY,
    order_id                VARCHAR(100),
    payment_sequential      VARCHAR(10),
    payment_type            VARCHAR(50),
    payment_installments    VARCHAR(10),
    payment_value           VARCHAR(30),
    -- Audit columns
    source_file             VARCHAR(255),
    source_system           VARCHAR(50)  DEFAULT 'JSON',
    batch_id                VARCHAR(100),
    load_timestamp          TIMESTAMP    DEFAULT NOW(),
    load_date               DATE         DEFAULT CURRENT_DATE,
    is_rejected             BOOLEAN      DEFAULT FALSE,
    reject_reason           TEXT
);

-- ─────────────────────────────────────────
-- Bronze: Rejected Records Log
-- ─────────────────────────────────────────
CREATE TABLE IF NOT EXISTS bronze.rejected_records (
    reject_id       BIGSERIAL PRIMARY KEY,
    source_table    VARCHAR(100),
    batch_id        VARCHAR(100),
    raw_data        TEXT,
    reject_reason   TEXT,
    rejected_at     TIMESTAMP DEFAULT NOW()
);

COMMENT ON SCHEMA bronze IS 'Bronze Layer: Raw immutable data loaded from source systems';
