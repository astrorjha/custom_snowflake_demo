-- =============================================================================
-- GalaxyCommerce — External Iceberg Table Definitions
-- =============================================================================
-- Run by the ingest_s3_to_snowflake DAG (create_raw_tables task) on every
-- execution via SQLExecuteQueryOperator.  All statements are idempotent.
--
-- DROP TABLE IF EXISTS handles the one-time migration from existing BASE
-- TABLEs to external Iceberg tables.  CREATE OR REPLACE ICEBERG TABLE keeps
-- reruns safe thereafter.
--
-- Prerequisites (run once by ACCOUNTADMIN — see include/sql/snowflake_setup.sql):
--   EXTERNAL VOLUME : galaxycommerce_ext_vol
--   CATALOG INTEGRATION : galaxycommerce_glue_catalog
-- =============================================================================


-- ---------------------------------------------------------------------------
-- RAW_ORDER
-- ---------------------------------------------------------------------------
DROP TABLE IF EXISTS DEMO.RAW.RAW_ORDER;

CREATE OR REPLACE ICEBERG TABLE DEMO.RAW.RAW_ORDER
  EXTERNAL_VOLUME    = 'galaxycommerce_ext_vol'
  CATALOG            = 'galaxycommerce_glue_catalog'
  CATALOG_TABLE_NAME = 'raw_order'
  COMMENT = 'External Iceberg table — source of truth in S3, registered via AWS Glue';


-- ---------------------------------------------------------------------------
-- RAW_ORDER_ITEM
-- ---------------------------------------------------------------------------
DROP TABLE IF EXISTS DEMO.RAW.RAW_ORDER_ITEM;

CREATE OR REPLACE ICEBERG TABLE DEMO.RAW.RAW_ORDER_ITEM
  EXTERNAL_VOLUME    = 'galaxycommerce_ext_vol'
  CATALOG            = 'galaxycommerce_glue_catalog'
  CATALOG_TABLE_NAME = 'raw_order_item'
  COMMENT = 'External Iceberg table — source of truth in S3, registered via AWS Glue';


-- ---------------------------------------------------------------------------
-- RAW_CUSTOMER
-- ---------------------------------------------------------------------------
DROP TABLE IF EXISTS DEMO.RAW.RAW_CUSTOMER;

CREATE OR REPLACE ICEBERG TABLE DEMO.RAW.RAW_CUSTOMER
  EXTERNAL_VOLUME    = 'galaxycommerce_ext_vol'
  CATALOG            = 'galaxycommerce_glue_catalog'
  CATALOG_TABLE_NAME = 'raw_customer'
  COMMENT = 'External Iceberg table — source of truth in S3, registered via AWS Glue';


-- ---------------------------------------------------------------------------
-- RAW_PRODUCT
-- ---------------------------------------------------------------------------
DROP TABLE IF EXISTS DEMO.RAW.RAW_PRODUCT;

CREATE OR REPLACE ICEBERG TABLE DEMO.RAW.RAW_PRODUCT
  EXTERNAL_VOLUME    = 'galaxycommerce_ext_vol'
  CATALOG            = 'galaxycommerce_glue_catalog'
  CATALOG_TABLE_NAME = 'raw_product'
  COMMENT = 'External Iceberg table — source of truth in S3, registered via AWS Glue';


-- ---------------------------------------------------------------------------
-- RAW_MARKETING_EVENT
-- ---------------------------------------------------------------------------
DROP TABLE IF EXISTS DEMO.RAW.RAW_MARKETING_EVENT;

CREATE OR REPLACE ICEBERG TABLE DEMO.RAW.RAW_MARKETING_EVENT
  EXTERNAL_VOLUME    = 'galaxycommerce_ext_vol'
  CATALOG            = 'galaxycommerce_glue_catalog'
  CATALOG_TABLE_NAME = 'raw_marketing_event'
  COMMENT = 'External Iceberg table — source of truth in S3, registered via AWS Glue';


-- ---------------------------------------------------------------------------
-- LOAD_AUDIT — native Snowflake table, not Iceberg
-- ---------------------------------------------------------------------------
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
