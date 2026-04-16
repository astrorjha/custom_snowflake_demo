-- =============================================================================
-- GalaxyCommerce — Snowflake One-Time Admin Setup
-- =============================================================================
-- Run ALL statements in this file once as ACCOUNTADMIN before the first
-- DAG execution.  These objects are prerequisites for the external Iceberg
-- tables created by the ingest_s3_to_snowflake DAG.
-- =============================================================================

-- Schemas (already created manually, kept for reference)
-- CREATE SCHEMA IF NOT EXISTS DEMO.RAW;
-- CREATE SCHEMA IF NOT EXISTS DEMO.STG;
-- CREATE SCHEMA IF NOT EXISTS DEMO.DW;
-- CREATE SCHEMA IF NOT EXISTS DEMO.MRT;
-- CREATE SCHEMA IF NOT EXISTS DEMO.RPT;


-- =============================================================================
-- External Volume
-- Tells Snowflake where Iceberg data files live in S3.
--
-- After running this statement:
--   DESC EXTERNAL VOLUME galaxycommerce_ext_vol;
-- Copy the STORAGE_AWS_IAM_USER_ARN and STORAGE_AWS_EXTERNAL_ID from the
-- output and add them to the trust policy of <SNOWFLAKE_IAM_ROLE_ARN> in AWS
-- IAM so Snowflake can assume the role and read from S3.
-- =============================================================================

CREATE EXTERNAL VOLUME galaxycommerce_ext_vol
  STORAGE_LOCATIONS = (
    (
      NAME            = 'galaxycommerce-iceberg'
      STORAGE_PROVIDER = 'S3'
      STORAGE_BASE_URL = 's3://galaxycommerce-sales-raw/iceberg/'
      STORAGE_AWS_ROLE_ARN = '<SNOWFLAKE_IAM_ROLE_ARN>'
    )
  );


-- =============================================================================
-- Catalog Integration
-- Connects Snowflake to AWS Glue as the Iceberg catalog so Snowflake can
-- discover table metadata without needing a direct path to metadata files.
--
-- GLUE_AWS_ROLE_ARN: IAM role Snowflake assumes to call the Glue API.
--   Can be the same role as the External Volume or a separate one — both
--   need the permissions in include/iam/airflow_glue_policy.json.
-- GLUE_CATALOG_ID: your 12-digit AWS account ID.
-- GLUE_REGION: AWS region where the Glue database lives.
-- =============================================================================

CREATE CATALOG INTEGRATION galaxycommerce_glue_catalog
  CATALOG_SOURCE    = GLUE
  CATALOG_NAMESPACE = 'galaxycommerce_raw'
  TABLE_FORMAT      = ICEBERG
  GLUE_AWS_ROLE_ARN = '<SNOWFLAKE_IAM_ROLE_ARN>'
  GLUE_CATALOG_ID   = '<AWS_ACCOUNT_ID>'
  GLUE_REGION       = 'us-east-1'
  ENABLED           = TRUE;
