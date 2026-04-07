COPY INTO DEMO.RAW.RAW_ORDER_ITEM (
    item_id,
    order_id,
    product_id,
    quantity,
    unit_price,
    amount,
    _source_file
)
FROM (
    SELECT
        $1:item_id::VARCHAR,
        $1:order_id::VARCHAR,
        $1:product_id::VARCHAR,
        $1:quantity::INT,
        $1:unit_price::FLOAT,
        $1:amount::FLOAT,
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
    'RAW_ORDER_ITEM',
    NULL,
    SUM(rows_loaded)
FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));
