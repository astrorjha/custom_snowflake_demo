-- =============================================================================
-- GalaxyCommerce — Snowflake External Iceberg Table Definitions
-- =============================================================================
-- Prerequisites (run once by ACCOUNTADMIN — not idempotent, do not re-run):
--
--   CREATE EXTERNAL VOLUME galaxycommerce_iceberg_vol
--     STORAGE_LOCATIONS = (
--       ( NAME = 'galaxycommerce-iceberg'
--         STORAGE_PROVIDER = 'S3'
--         STORAGE_BASE_URL = 's3://galaxycommerce-sales-raw/iceberg/'
--         STORAGE_AWS_ROLE_ARN = '<your-snowflake-s3-iam-role-arn>'
--       )
--     );
--
--   CREATE CATALOG INTEGRATION galaxycommerce_glue_int
--     CATALOG_SOURCE = GLUE
--     CATALOG_NAMESPACE = 'galaxycommerce_raw'
--     TABLE_FORMAT = ICEBERG
--     GLUE_AWS_ROLE_ARN = '<your-snowflake-glue-iam-role-arn>'
--     GLUE_CATALOG_ID = '<your-aws-account-id>'
--     GLUE_REGION = 'us-east-1'
--     ENABLED = TRUE;
--
-- Both commands require Snowflake to trust the IAM roles defined in
-- include/iam/airflow_glue_policy.json.  Snowflake will provide an
-- ExternalId to plug into the role's trust policy.
-- =============================================================================

-- RAW_ORDER
CREATE OR REPLACE ICEBERG TABLE DEMO.RAW.RAW_ORDER
  EXTERNAL_VOLUME = 'galaxycommerce_iceberg_vol'
  CATALOG = 'galaxycommerce_glue_int'
  CATALOG_TABLE_NAME = 'raw_order'
  COMMENT = 'External Iceberg table — source of truth in S3, registered via AWS Glue';

-- RAW_ORDER_ITEM
CREATE OR REPLACE ICEBERG TABLE DEMO.RAW.RAW_ORDER_ITEM
  EXTERNAL_VOLUME = 'galaxycommerce_iceberg_vol'
  CATALOG = 'galaxycommerce_glue_int'
  CATALOG_TABLE_NAME = 'raw_order_item'
  COMMENT = 'External Iceberg table — source of truth in S3, registered via AWS Glue';

-- RAW_CUSTOMER
CREATE OR REPLACE ICEBERG TABLE DEMO.RAW.RAW_CUSTOMER
  EXTERNAL_VOLUME = 'galaxycommerce_iceberg_vol'
  CATALOG = 'galaxycommerce_glue_int'
  CATALOG_TABLE_NAME = 'raw_customer'
  COMMENT = 'External Iceberg table — source of truth in S3, registered via AWS Glue';

-- RAW_PRODUCT
CREATE OR REPLACE ICEBERG TABLE DEMO.RAW.RAW_PRODUCT
  EXTERNAL_VOLUME = 'galaxycommerce_iceberg_vol'
  CATALOG = 'galaxycommerce_glue_int'
  CATALOG_TABLE_NAME = 'raw_product'
  COMMENT = 'External Iceberg table — source of truth in S3, registered via AWS Glue';

-- RAW_MARKETING_EVENT
CREATE OR REPLACE ICEBERG TABLE DEMO.RAW.RAW_MARKETING_EVENT
  EXTERNAL_VOLUME = 'galaxycommerce_iceberg_vol'
  CATALOG = 'galaxycommerce_glue_int'
  CATALOG_TABLE_NAME = 'raw_marketing_event'
  COMMENT = 'External Iceberg table — source of truth in S3, registered via AWS Glue';

-- Load audit table — native Snowflake table, not Iceberg
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
