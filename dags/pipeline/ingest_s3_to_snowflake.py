"""
dags/pipeline/ingest_s3_to_snowflake.py
=========================================
Ingests Parquet files from S3 into Snowflake's RAW schema.

Triggered by PIPELINE_ASSET — the combined S3 prefix asset emitted by
regional_aggregator.py once all 6 region generation DAGs have completed.
On success, emits RAW_ASSET to trigger the downstream dbt transformation DAG.

Asset topology
--------------
Inlet  (schedule):  s3://galaxycommerce-sales-raw/raw/sales/
Outlet (emitted):   snowflake://DEMO/RAW
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta
from pathlib import Path

from airflow.decorators import dag, task
from airflow.sdk import Asset

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Asset definitions
# ---------------------------------------------------------------------------

PIPELINE_ASSET = Asset("s3://galaxycommerce-sales-raw/raw/sales/")
RAW_ASSET = Asset("snowflake://DEMO/RAW")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

SNOWFLAKE_CONN_ID = "snowflake_default"
S3_BUCKET = "galaxycommerce-sales-raw"
S3_PREFIX = "raw/sales"
REGIONS = ["us-east", "us-west", "eu-west", "eu-central", "apac-au", "apac-jp"]

INCLUDE_DIR = Path(os.environ.get("AIRFLOW_HOME", "/usr/local/airflow")) / "include"
SETUP_SQL = str(INCLUDE_DIR / "sql/snowflake_setup.sql")

RAW_TABLES = {
    "RAW_ORDER": "DEMO.RAW.RAW_ORDER",
    "RAW_ORDER_ITEM": "DEMO.RAW.RAW_ORDER_ITEM",
    "RAW_CUSTOMER": "DEMO.RAW.RAW_CUSTOMER",
    "RAW_PRODUCT": "DEMO.RAW.RAW_PRODUCT",
    "RAW_MARKETING_EVENT": "DEMO.RAW.RAW_MARKETING_EVENT",
}

# ---------------------------------------------------------------------------
# SQL helpers (executed at DAG parse time — lightweight file I/O only)
# ---------------------------------------------------------------------------

def _load_copy_sql(entity: str) -> list[str]:
    """
    Read a copy_into SQL template and expand it for all 6 regions.

    Swaps {{ params.* }} placeholders for Airflow Jinja macros so they
    are rendered correctly at task execution time.  The region placeholder
    is resolved immediately (parse time) to produce one SQL block per region.
    Returns a flat list of SQL statement strings for all regions.
    """
    sql_path = INCLUDE_DIR / f"sql/copy_into/copy_{entity}.sql"
    template = sql_path.read_text()

    # Replace params.* with equivalent Airflow macros rendered at runtime
    template = (
        template
        .replace("{{ params.date_str }}", "{{ ds }}")
        .replace("{{ params.hour }}", "{{ data_interval_start.hour }}")
        .replace("{{ params.dag_run_id }}", "{{ run_id }}")
    )

    # Expand for every region — each item is a self-contained SQL block
    # (COPY INTO + INSERT INTO LOAD_AUDIT)
    statements: list[str] = []
    for region in REGIONS:
        statements.append(template.replace("{{ params.region }}", region))

    return statements


# Pre-load at parse time so the operator sql arg is ready
_COPY_SQL: dict[str, list[str]] = {
    "orders": _load_copy_sql("orders"),
    "order_items": _load_copy_sql("order_items"),
    "customers": _load_copy_sql("customers"),
    "products": _load_copy_sql("products"),
    "marketing_events": _load_copy_sql("marketing_events"),
}

# ---------------------------------------------------------------------------
# DAG
# ---------------------------------------------------------------------------

_DEFAULT_ARGS = {
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "sla": timedelta(minutes=30),
}


@dag(
    dag_id="ingest_s3_to_snowflake",
    schedule=[PIPELINE_ASSET],
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["galaxycommerce", "pipeline", "project-2"],
    default_args=_DEFAULT_ARGS,
)
def ingest_s3_to_snowflake():

    # ------------------------------------------------------------------
    # Task 1: Verify that Parquet files have landed in S3 for every region
    # ------------------------------------------------------------------

    @task()
    def check_s3_manifest() -> dict[str, int]:
        """
        Confirm at least one Parquet file exists per region for the current
        logical date and hour.  Raises AirflowSkipException if any region
        has zero files so that the run is marked as skipped (not failed) when
        data genuinely hasn't arrived yet.
        """
        import boto3
        from airflow.exceptions import AirflowSkipException
        from airflow.operators.python import get_current_context

        context = get_current_context()
        logical_date = context["logical_date"]
        date_str = logical_date.strftime("%Y-%m-%d")
        hour = logical_date.strftime("%H")

        s3 = boto3.client("s3")
        file_counts: dict[str, int] = {}

        for region in REGIONS:
            prefix = f"{S3_PREFIX}/region={region}/date={date_str}/hour={hour}/"
            response = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix=prefix)
            parquet_files = [
                obj for obj in response.get("Contents", [])
                if obj["Key"].endswith(".parquet")
            ]
            file_counts[region] = len(parquet_files)
            log.info(
                "S3 manifest | region=%s | date=%s | hour=%s | files=%d",
                region, date_str, hour, len(parquet_files),
            )

        missing = [r for r, count in file_counts.items() if count == 0]
        if missing:
            raise AirflowSkipException(
                f"No Parquet files found for regions: {missing} "
                f"(date={date_str}, hour={hour}). Skipping run."
            )

        return file_counts

    # ------------------------------------------------------------------
    # Task 2: Ensure RAW tables exist (idempotent DDL)
    # ------------------------------------------------------------------

    from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

    create_raw_tables = SQLExecuteQueryOperator(
        task_id="create_raw_tables",
        conn_id=SNOWFLAKE_CONN_ID,
        sql=SETUP_SQL,
    )

    # ------------------------------------------------------------------
    # Tasks 3a-3e: COPY INTO — 5 parallel deferrable tasks, one per entity
    # Each task runs 6 SQL blocks (one per region) sequentially within the
    # same Snowflake API call, keeping the task graph to 5 nodes.
    # ------------------------------------------------------------------

    from airflow.providers.snowflake.operators.snowflake import SnowflakeSqlApiOperator

    copy_orders = SnowflakeSqlApiOperator(
        task_id="copy_orders",
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql=_COPY_SQL["orders"],
        deferrable=True,
    )

    copy_order_items = SnowflakeSqlApiOperator(
        task_id="copy_order_items",
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql=_COPY_SQL["order_items"],
        deferrable=True,
    )

    copy_customers = SnowflakeSqlApiOperator(
        task_id="copy_customers",
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql=_COPY_SQL["customers"],
        deferrable=True,
    )

    copy_products = SnowflakeSqlApiOperator(
        task_id="copy_products",
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql=_COPY_SQL["products"],
        deferrable=True,
    )

    copy_marketing_events = SnowflakeSqlApiOperator(
        task_id="copy_marketing_events",
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql=_COPY_SQL["marketing_events"],
        deferrable=True,
    )

    # ------------------------------------------------------------------
    # Task 4: Validate that every RAW table received rows for today's load
    # Equivalent to running SQLColumnCheckOperator row-count assertions
    # across all 5 tables, unified into a single task with consolidated
    # logging and a single AirflowFailException on any zero-row table.
    # ------------------------------------------------------------------

    @task()
    def validate_row_counts() -> dict[str, int]:
        """
        Query each RAW table for rows loaded today (partitioned by _loaded_at date).
        Logs counts for all 5 tables.  Raises AirflowFailException if any table
        has zero rows, indicating the COPY INTO produced no output.
        """
        from airflow.exceptions import AirflowFailException
        from airflow.operators.python import get_current_context
        from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

        context = get_current_context()
        date_str = context["ds"]

        hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
        row_counts: dict[str, int] = {}

        for name, fq_table in RAW_TABLES.items():
            result = hook.get_first(
                f"SELECT COUNT(*) FROM {fq_table} "
                f"WHERE _loaded_at::DATE = '{date_str}'"
            )
            count = result[0] if result else 0
            row_counts[name] = count
            log.info("Row count | table=%s | date=%s | rows=%d", name, date_str, count)

        empty_tables = [name for name, count in row_counts.items() if count == 0]
        if empty_tables:
            raise AirflowFailException(
                f"COPY INTO produced 0 rows for date={date_str} "
                f"in tables: {empty_tables}"
            )

        return row_counts

    # ------------------------------------------------------------------
    # Task 5: Emit the downstream RAW Asset to trigger the dbt DAG
    # ------------------------------------------------------------------

    @task(outlets=[RAW_ASSET])
    def emit_raw_asset(_row_counts: dict[str, int]) -> bool:
        """
        Emit RAW_ASSET to signal that Snowflake's RAW schema is populated and
        ready for dbt transformation.  _row_counts is accepted only to wire the
        TaskFlow dependency from validate_row_counts.
        """
        log.info(
            "RAW load confirmed. Emitting Asset '%s' to trigger downstream DAG.",
            RAW_ASSET.uri,
        )
        return True

    # ------------------------------------------------------------------
    # Wire the task graph
    # ------------------------------------------------------------------

    manifest = check_s3_manifest()

    manifest >> create_raw_tables >> [
        copy_orders,
        copy_order_items,
        copy_customers,
        copy_products,
        copy_marketing_events,
    ]

    counts = validate_row_counts()

    [
        copy_orders,
        copy_order_items,
        copy_customers,
        copy_products,
        copy_marketing_events,
    ] >> counts

    emit_raw_asset(counts)


ingest_s3_to_snowflake()
