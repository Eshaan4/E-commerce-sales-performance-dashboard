-- =============================================================
-- 02_silver_schema.sql  –  Silver Layer (Cleansed / Enriched)
-- =============================================================

-- ─────────────────────────────────────────
-- Silver: Customers (SCD Type 2)
-- ─────────────────────────────────────────
CREATE TABLE IF NOT EXISTS silver.customers (
    silver_id               BIGSERIAL PRIMARY KEY,
    customer_id             VARCHAR(100)    NOT NULL,
    customer_unique_id      VARCHAR(100)    NOT NULL,
    zip_code_prefix         VARCHAR(10),
    city                    VARCHAR(100),
    state                   VARCHAR(10),
    state_name              VARCHAR(100),   -- Lookup mapped full state name
    region                  VARCHAR(50),    -- Derived: N/NE/SE/S/CO
    -- SCD Type 2 columns
    effective_start_date    DATE            NOT NULL DEFAULT CURRENT_DATE,
    effective_end_date      DATE,
    is_current              BOOLEAN         DEFAULT TRUE,
    version_number          INT             DEFAULT 1,
    -- Audit columns
    batch_id                VARCHAR(100),
    source_system           VARCHAR(50),
    created_at              TIMESTAMP       DEFAULT NOW(),
    updated_at              TIMESTAMP       DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS uix_silver_customers_current
    ON silver.customers(customer_id) WHERE is_current = TRUE;

-- ─────────────────────────────────────────
-- Silver: Orders (Cleansed)
-- ─────────────────────────────────────────
CREATE TABLE IF NOT EXISTS silver.orders (
    silver_id                   BIGSERIAL PRIMARY KEY,
    order_id                    VARCHAR(100)    NOT NULL UNIQUE,
    customer_id                 VARCHAR(100)    NOT NULL,
    order_status                VARCHAR(50),
    order_status_category       VARCHAR(50),    -- Derived: active/completed/cancelled
    order_purchase_timestamp    TIMESTAMP,
    order_approved_at           TIMESTAMP,
    order_delivered_at          TIMESTAMP,
    order_estimated_delivery    DATE,
    delivery_days               INT,            -- Derived: actual delivery duration
    is_late_delivery            BOOLEAN,        -- Derived flag
    -- Audit columns
    batch_id                    VARCHAR(100),
    source_system               VARCHAR(50),
    created_at                  TIMESTAMP       DEFAULT NOW(),
    updated_at                  TIMESTAMP       DEFAULT NOW()
);

-- ─────────────────────────────────────────
-- Silver: Products (SCD Type 1 – overwrite)
-- ─────────────────────────────────────────
CREATE TABLE IF NOT EXISTS silver.products (
    silver_id                   BIGSERIAL PRIMARY KEY,
    product_id                  VARCHAR(100)    NOT NULL UNIQUE,
    product_category_name       VARCHAR(100),
    product_category_english    VARCHAR(100),   -- Lookup mapped English name
    product_name_length         INT,
    product_description_length  INT,
    product_photos_qty          INT,
    product_weight_g            NUMERIC(10,2),
    product_length_cm           NUMERIC(10,2),
    product_height_cm           NUMERIC(10,2),
    product_width_cm            NUMERIC(10,2),
    product_volume_cm3          NUMERIC(12,2),  -- Derived: L x H x W
    -- Audit columns
    batch_id                    VARCHAR(100),
    source_system               VARCHAR(50),
    created_at                  TIMESTAMP       DEFAULT NOW(),
    updated_at                  TIMESTAMP       DEFAULT NOW()
);

-- ─────────────────────────────────────────
-- Silver: Order Items (Cleansed)
-- ─────────────────────────────────────────
CREATE TABLE IF NOT EXISTS silver.order_items (
    silver_id           BIGSERIAL PRIMARY KEY,
    order_id            VARCHAR(100)    NOT NULL,
    order_item_id       INT,
    product_id          VARCHAR(100),
    seller_id           VARCHAR(100),
    shipping_limit_date TIMESTAMP,
    price               NUMERIC(12,2),
    freight_value       NUMERIC(12,2),
    total_item_value    NUMERIC(12,2),   -- Derived: price + freight
    -- Audit columns
    batch_id            VARCHAR(100),
    source_system       VARCHAR(50),
    created_at          TIMESTAMP       DEFAULT NOW(),
    updated_at          TIMESTAMP       DEFAULT NOW()
);

-- ─────────────────────────────────────────
-- Silver: Sellers (SCD Type 2)
-- ─────────────────────────────────────────
CREATE TABLE IF NOT EXISTS silver.sellers (
    silver_id               BIGSERIAL PRIMARY KEY,
    seller_id               VARCHAR(100)    NOT NULL,
    zip_code_prefix         VARCHAR(10),
    city                    VARCHAR(100),
    state                   VARCHAR(10),
    state_name              VARCHAR(100),
    region                  VARCHAR(50),
    -- SCD Type 2 columns
    effective_start_date    DATE            NOT NULL DEFAULT CURRENT_DATE,
    effective_end_date      DATE,
    is_current              BOOLEAN         DEFAULT TRUE,
    version_number          INT             DEFAULT 1,
    -- Audit columns
    batch_id                VARCHAR(100),
    source_system           VARCHAR(50),
    created_at              TIMESTAMP       DEFAULT NOW(),
    updated_at              TIMESTAMP       DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS uix_silver_sellers_current
    ON silver.sellers(seller_id) WHERE is_current = TRUE;

-- ─────────────────────────────────────────
-- Silver: Payments (Cleansed)
-- ─────────────────────────────────────────
CREATE TABLE IF NOT EXISTS silver.payments (
    silver_id               BIGSERIAL PRIMARY KEY,
    order_id                VARCHAR(100)    NOT NULL,
    payment_sequential      INT,
    payment_type            VARCHAR(50),
    payment_type_category   VARCHAR(50),    -- Lookup: card/voucher/boleto/debit
    payment_installments    INT,
    payment_value           NUMERIC(12,2),
    -- Audit columns
    batch_id                VARCHAR(100),
    source_system           VARCHAR(50),
    created_at              TIMESTAMP       DEFAULT NOW(),
    updated_at              TIMESTAMP       DEFAULT NOW()
);

-- ─────────────────────────────────────────
-- Silver: Lookup Tables
-- ─────────────────────────────────────────
CREATE TABLE IF NOT EXISTS silver.state_lookup (
    state_code  VARCHAR(5) PRIMARY KEY,
    state_name  VARCHAR(100),
    region      VARCHAR(50)
);

INSERT INTO silver.state_lookup (state_code, state_name, region) VALUES
('AC', 'Acre', 'North'),
('AL', 'Alagoas', 'Northeast'),
('AM', 'Amazonas', 'North'),
('AP', 'Amapá', 'North'),
('BA', 'Bahia', 'Northeast'),
('CE', 'Ceará', 'Northeast'),
('DF', 'Distrito Federal', 'Central-West'),
('ES', 'Espírito Santo', 'Southeast'),
('GO', 'Goiás', 'Central-West'),
('MA', 'Maranhão', 'Northeast'),
('MG', 'Minas Gerais', 'Southeast'),
('MS', 'Mato Grosso do Sul', 'Central-West'),
('MT', 'Mato Grosso', 'Central-West'),
('PA', 'Pará', 'North'),
('PB', 'Paraíba', 'Northeast'),
('PE', 'Pernambuco', 'Northeast'),
('PI', 'Piauí', 'Northeast'),
('PR', 'Paraná', 'South'),
('RJ', 'Rio de Janeiro', 'Southeast'),
('RN', 'Rio Grande do Norte', 'Northeast'),
('RO', 'Rondônia', 'North'),
('RR', 'Roraima', 'North'),
('RS', 'Rio Grande do Sul', 'South'),
('SC', 'Santa Catarina', 'South'),
('SE', 'Sergipe', 'Northeast'),
('SP', 'São Paulo', 'Southeast'),
('TO', 'Tocantins', 'North')
ON CONFLICT DO NOTHING;

CREATE TABLE IF NOT EXISTS silver.category_lookup (
    category_pt     VARCHAR(100) PRIMARY KEY,
    category_en     VARCHAR(100)
);

INSERT INTO silver.category_lookup (category_pt, category_en) VALUES
('beleza_saude', 'Health & Beauty'),
('informatica_acessorios', 'Computers & Accessories'),
('automotivo', 'Auto'),
('cama_mesa_banho', 'Bed Bath Table'),
('moveis_decoracao', 'Furniture & Decor'),
('esporte_lazer', 'Sports & Leisure'),
('perfumaria', 'Perfumery'),
('utilidades_domesticas', 'Housewares'),
('telefonia', 'Telephony'),
('relogios_presentes', 'Watches & Gifts'),
('alimentos_bebidas', 'Food & Drink'),
('bebes', 'Babies'),
('papelaria', 'Stationery'),
('tablets_impressao_imagem', 'Tablets Printing Image'),
('brinquedos', 'Toys'),
('telefonia_fixa', 'Fixed Telephony'),
('ferramentas_jardim', 'Garden Tools'),
('fashion_bolsas_e_acessorios', 'Fashion Bags & Accessories'),
('eletroportateis', 'Small Appliances'),
('outros', 'Others')
ON CONFLICT DO NOTHING;

COMMENT ON SCHEMA silver IS 'Silver Layer: Cleansed, standardized, enriched data with SCD handling';
