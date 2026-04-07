COPY INTO DEMO.RAW.RAW_ORDER (
    order_id,
    customer_id,
    region,
    created_at,
    status,
    currency,
    promo_code,
    total_amount,
    _source_file
)
FROM (
    SELECT
        $1:order_id::VARCHAR,
        $1:customer_id::VARCHAR,
        $1:region::VARCHAR,
        $1:created_at::TIMESTAMP_NTZ,
        $1:status::VARCHAR,
        $1:currency::VARCHAR(3),
        $1:promo_code::VARCHAR,
        $1:total_amount::FLOAT,
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
    'RAW_ORDER',
    NULL,
    SUM(rows_loaded)
FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));
