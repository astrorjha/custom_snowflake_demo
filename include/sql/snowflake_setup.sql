-- =============================================================================
-- GalaxyCommerce Snowflake Setup DDL
-- This DDL was run manually. The database DEMO and schemas already exist.
-- =============================================================================

-- Already created manually. Kept for reference.
-- CREATE SCHEMA IF NOT EXISTS DEMO.RAW;
-- CREATE SCHEMA IF NOT EXISTS DEMO.STG;
-- CREATE SCHEMA IF NOT EXISTS DEMO.DW;
-- CREATE SCHEMA IF NOT EXISTS DEMO.MRT;
-- CREATE SCHEMA IF NOT EXISTS DEMO.RPT;


-- =============================================================================
-- External S3 Stage
-- Note: STORAGE_INTEGRATION must be created separately by a Snowflake
-- ACCOUNTADMIN using the IAM role ARN. See Snowflake docs: CREATE STORAGE INTEGRATION.
-- =============================================================================

CREATE OR REPLACE STAGE DEMO.RAW.S3_SALES_STAGE
  URL = 's3://galaxycommerce-sales-raw/raw/sales/'
  STORAGE_INTEGRATION = GALAXYCOMMERCE_S3_INT
  FILE_FORMAT = (TYPE = 'PARQUET' SNAPPY_COMPRESSION = TRUE);


-- =============================================================================
-- RAW Tables
-- =============================================================================

CREATE TABLE IF NOT EXISTS DEMO.RAW.RAW_ORDER (
    order_id        VARCHAR,
    customer_id     VARCHAR,
    region          VARCHAR,
    created_at      TIMESTAMP_NTZ,
    status          VARCHAR,
    currency        VARCHAR(3),
    promo_code      VARCHAR,
    total_amount    FLOAT,
    _loaded_at      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    _source_file    VARCHAR
)
COMMENT = 'Loaded by Airflow ingest_s3_to_snowflake DAG via COPY INTO';

CREATE TABLE IF NOT EXISTS DEMO.RAW.RAW_ORDER_ITEM (
    item_id         VARCHAR,
    order_id        VARCHAR,
    product_id      VARCHAR,
    quantity        INT,
    unit_price      FLOAT,
    amount          FLOAT,
    _loaded_at      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    _source_file    VARCHAR
)
COMMENT = 'Loaded by Airflow ingest_s3_to_snowflake DAG via COPY INTO';

CREATE TABLE IF NOT EXISTS DEMO.RAW.RAW_CUSTOMER (
    customer_id     VARCHAR,
    name            VARCHAR,
    email           VARCHAR,
    region          VARCHAR,
    tier            VARCHAR,
    created_at      TIMESTAMP_NTZ,
    _loaded_at      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    _source_file    VARCHAR
)
COMMENT = 'Loaded by Airflow ingest_s3_to_snowflake DAG via COPY INTO';

CREATE TABLE IF NOT EXISTS DEMO.RAW.RAW_PRODUCT (
    product_id      VARCHAR,
    name            VARCHAR,
    category        VARCHAR,
    unit_price      FLOAT,
    is_active       BOOLEAN,
    _loaded_at      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    _source_file    VARCHAR
)
COMMENT = 'Loaded by Airflow ingest_s3_to_snowflake DAG via COPY INTO';

CREATE TABLE IF NOT EXISTS DEMO.RAW.RAW_MARKETING_EVENT (
    event_id        VARCHAR,
    customer_id     VARCHAR,
    region          VARCHAR,
    event_type      VARCHAR,
    created_at      TIMESTAMP_NTZ,
    _loaded_at      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    _source_file    VARCHAR
)
COMMENT = 'Loaded by Airflow ingest_s3_to_snowflake DAG via COPY INTO';


-- =============================================================================
-- Load Audit Table
-- =============================================================================

CREATE TABLE IF NOT EXISTS DEMO.RAW.LOAD_AUDIT (
    load_id           VARCHAR DEFAULT UUID_STRING(),
    dag_run_id        VARCHAR,
    load_date         DATE,
    region            VARCHAR,
    entity            VARCHAR,
    s3_expected_rows  INT,
    sf_actual_rows    INT,
    loaded_at         TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);
