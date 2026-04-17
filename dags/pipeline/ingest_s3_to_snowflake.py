"""
dags/pipeline/ingest_s3_to_snowflake.py
=========================================
Ingests sales data from S3 into Snowflake via Iceberg.

Pipeline overview
-----------------
1. check_s3_manifest   — verify Parquet files have landed in S3 for every region
2. convert_to_iceberg  — read Parquet files, write as Iceberg to S3, register in AWS Glue
3. create_raw_tables   — ensure Snowflake external Iceberg table definitions exist (idempotent)
4. validate_row_counts — confirm each Snowflake RAW table is queryable with rows for today
5. emit_raw_asset      — emit RAW_ASSET to trigger the downstream dbt DAG

Open table format strategy
---------------------------
Data is written as Apache Iceberg to s3://galaxycommerce-sales-raw/iceberg/ and registered
in AWS Glue (galaxycommerce_raw database).  Snowflake reads these as external Iceberg tables
via a Catalog Integration + External Volume — no COPY INTO, no vendor lock-in.

The same Iceberg tables can be queried from Databricks or any Iceberg-compatible engine
pointing at the same Glue catalog, enabling multi-engine access without duplication.

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

from airflow.sdk import Asset, dag, task

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
ICEBERG_S3_PREFIX = "s3://galaxycommerce-sales-raw/iceberg/"
GLUE_DATABASE = "galaxycommerce_raw"
REGIONS = ["us-east", "us-west", "eu-west", "eu-central", "apac-au", "apac-jp"]

# Maps pipeline entity names to Glue/Iceberg table names (and Snowflake RAW table names)
ENTITY_TABLE_MAP = {
    "orders": "raw_order",
    "order_items": "raw_order_item",
    "customers": "raw_customer",
    "products": "raw_product",
    "marketing_events": "raw_marketing_event",
}

RAW_TABLES = {
    "RAW_ORDER": "DEMO.RAW.RAW_ORDER",
    "RAW_ORDER_ITEM": "DEMO.RAW.RAW_ORDER_ITEM",
    "RAW_CUSTOMER": "DEMO.RAW.RAW_CUSTOMER",
    "RAW_PRODUCT": "DEMO.RAW.RAW_PRODUCT",
    "RAW_MARKETING_EVENT": "DEMO.RAW.RAW_MARKETING_EVENT",
}

INCLUDE_DIR = Path(os.environ.get("AIRFLOW_HOME", "/usr/local/airflow")) / "include"
# Idempotent CREATE OR REPLACE ICEBERG TABLE statements run each DAG run.
# The CATALOG INTEGRATION and EXTERNAL VOLUME they reference must be created
# once by a Snowflake ACCOUNTADMIN — see include/sql/snowflake_setup.sql.
ICEBERG_DDL_SQL = (INCLUDE_DIR / "sql/create_iceberg_tables.sql").read_text()

# ---------------------------------------------------------------------------
# DAG
# ---------------------------------------------------------------------------

_DEFAULT_ARGS = {
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    dag_id="ingest_s3_to_snowflake",
    schedule=[PIPELINE_ASSET],
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["galaxycommerce", "pipeline", "project-2", "iceberg"],
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
        from airflow.sdk import get_current_context

        context = get_current_context()
        logical_date = context["dag_run"].logical_date
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
    # Task 2: Convert Parquet files to Iceberg and register in AWS Glue
    # ------------------------------------------------------------------

    @task()
    def convert_to_iceberg(file_counts: dict[str, int]) -> dict[str, int]:
        """
        For each entity (orders, customers, etc.) and each region, reads all
        Parquet files for the current logical date/hour from S3, appends
        _source_file and _loaded_at audit columns, then writes the combined
        result as an Iceberg table to s3://galaxycommerce-sales-raw/iceberg/
        and registers (or updates) the table in the AWS Glue catalog.

        On first run a new Iceberg table is created; on subsequent runs rows
        are appended so the table accumulates a full history of loads.
        """
        import datetime as dt

        import io

        import boto3
        import pyarrow as pa
        import pyarrow.parquet as pq
        from airflow.sdk import get_current_context
        from pyiceberg.catalog import load_catalog
        from pyiceberg.exceptions import NoSuchNamespaceError, NoSuchTableError
        from pyiceberg.io.pyarrow import pyarrow_to_schema  # used in except NoSuchTableError

        context = get_current_context()
        logical_date = context["dag_run"].logical_date
        date_str = logical_date.strftime("%Y-%m-%d")
        hour = logical_date.strftime("%H")
        loaded_at = dt.datetime.now(tz=dt.timezone.utc)

        catalog = load_catalog(
            "glue",
            **{
                "type": "glue",
                "warehouse": ICEBERG_S3_PREFIX,
            },
        )

        try:
            catalog.load_namespace_properties(GLUE_DATABASE)
        except NoSuchNamespaceError:
            catalog.create_namespace(GLUE_DATABASE)
            log.info("Created Glue database: %s", GLUE_DATABASE)

        s3_client = boto3.client("s3")
        row_counts: dict[str, int] = {}

        for entity, iceberg_table_name in ENTITY_TABLE_MAP.items():
            frames: list[pa.Table] = []

            for region in REGIONS:
                # File names are exactly {entity}.parquet — one file per
                # entity per region/date/hour, written by gen_sales_regions.
                key = f"{S3_PREFIX}/region={region}/date={date_str}/hour={hour}/{entity}.parquet"

                try:
                    buf = io.BytesIO(
                        s3_client.get_object(Bucket=S3_BUCKET, Key=key)["Body"].read()
                    )
                except s3_client.exceptions.NoSuchKey:
                    log.warning("File not found: s3://%s/%s — skipping region", S3_BUCKET, key)
                    continue

                arrow_tbl = pq.read_table(buf)

                # Cast any remaining dictionary-encoded columns to their
                # plain value type so all frames have a uniform schema.
                for i, field in enumerate(arrow_tbl.schema):
                    if pa.types.is_dictionary(field.type):
                        arrow_tbl = arrow_tbl.set_column(
                            i, field.name,
                            arrow_tbl.column(i).cast(field.type.value_type),
                        )

                n = len(arrow_tbl)
                arrow_tbl = arrow_tbl.append_column(
                    "_source_file",
                    pa.array([f"s3://{S3_BUCKET}/{key}"] * n, type=pa.string()),
                ).append_column(
                    "_loaded_at",
                    pa.array([loaded_at] * n, type=pa.timestamp("us", tz="UTC")),
                )
                frames.append(arrow_tbl)

            if not frames:
                log.warning("No Parquet files found for entity=%s — skipping", entity)
                continue

            combined = pa.concat_tables(frames)

            try:
                iceberg_tbl = catalog.load_table((GLUE_DATABASE, iceberg_table_name))
                iceberg_tbl.append(combined)
                log.info(
                    "Iceberg append | table=%s.%s | rows=%d",
                    GLUE_DATABASE, iceberg_table_name, len(combined),
                )
            except NoSuchTableError:
                from pyiceberg.table.name_mapping import MappedField, NameMapping

                # Source Parquet files are written by pandas and have no
                # embedded Iceberg field IDs.  Build a NameMapping that
                # assigns sequential IDs by column position so
                # pyarrow_to_schema can convert without them.
                name_mapping = NameMapping([
                    MappedField(field_id=i + 1, names=[field.name])
                    for i, field in enumerate(combined.schema)
                ])
                iceberg_schema = pyarrow_to_schema(combined.schema, name_mapping=name_mapping)
                iceberg_tbl = catalog.create_table(
                    identifier=(GLUE_DATABASE, iceberg_table_name),
                    schema=iceberg_schema,
                    location=f"{ICEBERG_S3_PREFIX}{iceberg_table_name}",
                )
                iceberg_tbl.append(combined)
                log.info(
                    "Iceberg create | table=%s.%s | rows=%d",
                    GLUE_DATABASE, iceberg_table_name, len(combined),
                )

            row_counts[entity] = len(combined)

        return row_counts

    # ------------------------------------------------------------------
    # Task 3: Ensure Snowflake external Iceberg table definitions exist
    # ------------------------------------------------------------------

    from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

    create_raw_tables = SQLExecuteQueryOperator(
        task_id="create_raw_tables",
        conn_id=SNOWFLAKE_CONN_ID,
        sql=ICEBERG_DDL_SQL,
    )

    # ------------------------------------------------------------------
    # Task 4: Validate that every Snowflake RAW Iceberg table has rows
    # ------------------------------------------------------------------

    @task()
    def validate_row_counts() -> dict[str, int]:
        """
        Query each Snowflake external Iceberg table for rows loaded today.
        Raises AirflowFailException if any table returns zero rows, indicating
        the Iceberg write or Snowflake catalog sync did not produce queryable data.
        """
        from airflow.exceptions import AirflowFailException
        from airflow.sdk import get_current_context
        from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

        context = get_current_context()
        date_str = context["dag_run"].logical_date.strftime("%Y-%m-%d")

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
                f"Zero rows queryable from Snowflake for date={date_str} "
                f"in tables: {empty_tables}"
            )

        return row_counts

    # ------------------------------------------------------------------
    # Task 5: Emit the downstream RAW Asset to trigger the dbt DAG
    # ------------------------------------------------------------------

    @task(outlets=[RAW_ASSET])
    def emit_raw_asset(_row_counts: dict[str, int]) -> bool:
        """
        Emit RAW_ASSET to signal that Snowflake's RAW Iceberg tables are populated
        and ready for dbt transformation.
        """
        log.info(
            "RAW Iceberg load confirmed. Emitting Asset '%s' to trigger downstream DAG.",
            RAW_ASSET.uri,
        )
        return True

    # ------------------------------------------------------------------
    # Wire the task graph
    # ------------------------------------------------------------------

    manifest = check_s3_manifest()
    iceberg_counts = convert_to_iceberg(manifest)
    iceberg_counts >> create_raw_tables
    counts = validate_row_counts()
    create_raw_tables >> counts
    emit_raw_asset(counts)


ingest_s3_to_snowflake()
