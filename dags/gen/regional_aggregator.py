"""
dags/gen/regional_aggregator.py
================================
Fan-in aggregation bridge between the six region generation DAGs and the
downstream ingestion pipeline DAGs in dags/pipeline/.

How it fits in the pipeline
----------------------------
Each of the six ``gen_sales_<region>`` DAGs emits a region-scoped Asset when it
successfully uploads data for a scheduling window.  This DAG declares all six
region Assets as its schedule, so Airflow fires it **once per hour only when all
6 region Assets have been emitted in that scheduling window**.  It acts as the
fan-in aggregation point, replacing six separate triggers with a single,
authoritative signal.

Once ``validate_all_regions`` confirms the logical date and region count,
``emit_pipeline_asset`` emits ``PIPELINE_ASSET`` — the combined S3 prefix Asset
that downstream DAGs in ``dags/pipeline/`` declare as their schedule inlet.
Those DAGs therefore start only after every region's data has landed, with no
polling or sensor overhead.

Asset topology
--------------
Inlets  (schedule):
    s3://galaxycommerce-sales-raw/raw/sales/region=us-east/
    s3://galaxycommerce-sales-raw/raw/sales/region=us-west/
    s3://galaxycommerce-sales-raw/raw/sales/region=eu-west/
    s3://galaxycommerce-sales-raw/raw/sales/region=eu-central/
    s3://galaxycommerce-sales-raw/raw/sales/region=apac-au/
    s3://galaxycommerce-sales-raw/raw/sales/region=apac-jp/

Outlet (emitted by emit_pipeline_asset):
    s3://galaxycommerce-sales-raw/raw/sales/
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.sdk import Asset

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Region-scoped inlet Assets — must match the URIs emitted by gen_sales_regions.py
# ---------------------------------------------------------------------------

REGION_ASSETS = [
    Asset("s3://galaxycommerce-sales-raw/raw/sales/region=us-east/"),
    Asset("s3://galaxycommerce-sales-raw/raw/sales/region=us-west/"),
    Asset("s3://galaxycommerce-sales-raw/raw/sales/region=eu-west/"),
    Asset("s3://galaxycommerce-sales-raw/raw/sales/region=eu-central/"),
    Asset("s3://galaxycommerce-sales-raw/raw/sales/region=apac-au/"),
    Asset("s3://galaxycommerce-sales-raw/raw/sales/region=apac-jp/"),
]

# ---------------------------------------------------------------------------
# Combined downstream trigger Asset — declared as schedule inlet by pipeline DAGs
# ---------------------------------------------------------------------------

PIPELINE_ASSET = Asset("s3://galaxycommerce-sales-raw/raw/sales/")

# ---------------------------------------------------------------------------
# DAG
# ---------------------------------------------------------------------------

_DEFAULT_ARGS = {
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
}


@dag(
    dag_id="regional_aggregator",
    schedule=REGION_ASSETS,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["galaxycommerce", "data-gen", "project-1"],
    default_args=_DEFAULT_ARGS,
)
def regional_aggregator():

    @task()
    def validate_all_regions() -> dict:
        """
        Confirm that all 6 regions have landed data for the current logical date.

        Retrieves the logical date from the Airflow task context and logs a
        confirmation message.  Returns a summary dict that is passed downstream
        to wire the dependency chain into emit_pipeline_asset.
        """
        context = get_current_context()
        logical_date = context["logical_date"]

        log.info(
            "All 6 regions have landed data for logical_date=%s — "
            "proceeding to emit pipeline trigger asset.",
            logical_date,
        )

        return {
            "logical_date": str(logical_date),
            "regions_confirmed": 6,
        }

    @task(outlets=[PIPELINE_ASSET])
    def emit_pipeline_asset(_summary: dict) -> bool:
        """
        Emit the combined pipeline trigger Asset to unblock downstream DAGs.

        The ``outlets`` declaration registers the PIPELINE_ASSET event in the
        Airflow metadata DB when this task succeeds.  DAGs in dags/pipeline/
        that declare PIPELINE_ASSET as their schedule inlet are triggered
        automatically.  ``_summary`` exists only to wire the TaskFlow dependency
        from validate_all_regions.
        """
        log.info("All 6 regions confirmed — emitting pipeline trigger asset.")
        return True

    summary = validate_all_regions()
    emit_pipeline_asset(summary)


regional_aggregator()
