-- =============================================================
-- 04_metadata_schema.sql  –  Metadata & Audit Framework
-- =============================================================

-- ─────────────────────────────────────────
-- Pipeline Run Registry
-- ─────────────────────────────────────────
CREATE TABLE IF NOT EXISTS metadata.pipeline_runs (
    run_id              BIGSERIAL   PRIMARY KEY,
    batch_id            VARCHAR(100) UNIQUE NOT NULL,
    pipeline_name       VARCHAR(200) NOT NULL,
    layer               VARCHAR(20),     -- bronze / silver / gold
    source_system       VARCHAR(100),
    source_table        VARCHAR(100),
    target_table        VARCHAR(200),
    status              VARCHAR(20)  DEFAULT 'RUNNING',  -- RUNNING/SUCCESS/FAILED/PARTIAL
    -- Timing
    start_time          TIMESTAMP    DEFAULT NOW(),
    end_time            TIMESTAMP,
    duration_seconds    NUMERIC(10,2),
    -- Record counts
    total_records_read  INT          DEFAULT 0,
    records_inserted    INT          DEFAULT 0,
    records_updated     INT          DEFAULT 0,
    records_rejected    INT          DEFAULT 0,
    records_skipped     INT          DEFAULT 0,
    -- Error info
    error_message       TEXT,
    error_details       TEXT,
    -- Metadata
    triggered_by        VARCHAR(100) DEFAULT 'airflow',
    run_mode            VARCHAR(20)  DEFAULT 'FULL',     -- FULL / INCREMENTAL
    created_at          TIMESTAMP    DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_pipeline_runs_name ON metadata.pipeline_runs(pipeline_name);
CREATE INDEX IF NOT EXISTS idx_pipeline_runs_status ON metadata.pipeline_runs(status);
CREATE INDEX IF NOT EXISTS idx_pipeline_runs_date ON metadata.pipeline_runs(start_time);

-- ─────────────────────────────────────────
-- Schema Evolution / Column Audit Log
-- ─────────────────────────────────────────
CREATE TABLE IF NOT EXISTS metadata.schema_changes (
    change_id           BIGSERIAL   PRIMARY KEY,
    table_name          VARCHAR(200),
    change_type         VARCHAR(50),     -- ADD_COLUMN / DROP_COLUMN / TYPE_CHANGE
    column_name         VARCHAR(100),
    old_definition      TEXT,
    new_definition      TEXT,
    detected_at         TIMESTAMP    DEFAULT NOW(),
    batch_id            VARCHAR(100)
);

-- ─────────────────────────────────────────
-- Data Quality Results
-- ─────────────────────────────────────────
CREATE TABLE IF NOT EXISTS metadata.dq_results (
    dq_id               BIGSERIAL   PRIMARY KEY,
    batch_id            VARCHAR(100),
    table_name          VARCHAR(200),
    expectation_suite   VARCHAR(200),
    check_name          VARCHAR(200),
    check_type          VARCHAR(100),  -- null_check / pk_check / type_check / range_check
    column_name         VARCHAR(100),
    passed              BOOLEAN,
    total_records       INT,
    failed_records      INT,
    success_pct         NUMERIC(6,3),
    observed_value      TEXT,
    expected_value      TEXT,
    severity            VARCHAR(20)  DEFAULT 'ERROR',  -- ERROR / WARNING / INFO
    checked_at          TIMESTAMP    DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_dq_results_batch ON metadata.dq_results(batch_id);
CREATE INDEX IF NOT EXISTS idx_dq_results_table ON metadata.dq_results(table_name);

-- ─────────────────────────────────────────
-- Incremental Watermark Tracking
-- ─────────────────────────────────────────
CREATE TABLE IF NOT EXISTS metadata.watermarks (
    watermark_id        BIGSERIAL   PRIMARY KEY,
    source_table        VARCHAR(200) UNIQUE NOT NULL,
    last_loaded_value   VARCHAR(200),  -- Timestamp or ID of last loaded record
    watermark_column    VARCHAR(100),
    load_type           VARCHAR(20)  DEFAULT 'INCREMENTAL',
    last_updated        TIMESTAMP    DEFAULT NOW()
);

-- Default watermarks (full load initially)
INSERT INTO metadata.watermarks (source_table, last_loaded_value, watermark_column)
VALUES
    ('customers',   '1970-01-01 00:00:00', 'load_timestamp'),
    ('orders',      '1970-01-01 00:00:00', 'load_timestamp'),
    ('products',    '1970-01-01 00:00:00', 'load_timestamp'),
    ('order_items', '1970-01-01 00:00:00', 'load_timestamp'),
    ('sellers',     '1970-01-01 00:00:00', 'load_timestamp'),
    ('payments',    '1970-01-01 00:00:00', 'load_timestamp')
ON CONFLICT DO NOTHING;

-- ─────────────────────────────────────────
-- Error Log
-- ─────────────────────────────────────────
CREATE TABLE IF NOT EXISTS metadata.error_log (
    error_id        BIGSERIAL   PRIMARY KEY,
    batch_id        VARCHAR(100),
    pipeline_name   VARCHAR(200),
    error_type      VARCHAR(100),  -- SCHEMA_MISMATCH / PARSE_ERROR / TRANSFORM_FAIL / DB_ERROR
    error_message   TEXT,
    stack_trace     TEXT,
    record_data     TEXT,
    occurred_at     TIMESTAMP    DEFAULT NOW()
);

COMMENT ON SCHEMA metadata IS 'Metadata schema: pipeline run registry, data quality results, watermarks, audit logs';
