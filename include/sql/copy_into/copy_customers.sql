COPY INTO DEMO.RAW.RAW_CUSTOMER (
    customer_id,
    name,
    email,
    region,
    tier,
    created_at,
    _source_file
)
FROM (
    SELECT
        $1:customer_id::VARCHAR,
        $1:name::VARCHAR,
        $1:email::VARCHAR,
        $1:region::VARCHAR,
        $1:tier::VARCHAR,
        $1:created_at::TIMESTAMP_NTZ,
        METADATA$FILENAME
    FROM @DEMO.RAW.S3_SALES_STAGE/region={{ params.region }}/date={{ params.date_str }}/hour={{ params.hour }}/
)
FILE_FORMAT = (TYPE = 'PARQUET')
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
ON_ERROR = 'CONTINUE'
PURGE = FALSE;

INSERT INTO DEMO.RAW.LOAD_AUDIT (
    dag_run_id,
    load_date,
    region,
    entity,
    s3_expected_rows,
    sf_actual_rows
)
SELECT
    '{{ params.dag_run_id }}',
    '{{ params.date_str }}'::DATE,
    '{{ params.region }}',
    'RAW_CUSTOMER',
    NULL,
    SUM(rows_loaded)
FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));
