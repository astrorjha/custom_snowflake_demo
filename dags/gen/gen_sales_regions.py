"""
dags/gen_sales_regions.py
==========================
Defines one hourly Airflow DAG per GalaxyCommerce region using a factory function.

Why per-region DAGs instead of one monolithic DAG?
---------------------------------------------------
* **Independent retry granularity** — a transient S3 error in ap-au no longer
  blocks or retries us-east.  Each region's task failure is isolated.
* **Independent scheduling** — regions can be paused, backfilled, or
  rate-limited individually without affecting the others.
* **Per-region Assets** — each DAG emits a region-scoped Asset URI, allowing
  downstream DAGs in Project 2 to subscribe to only the region(s) they care
  about rather than waiting for all six to succeed.
* **Cleaner Airflow UI** — six small DAGs are easier to monitor than one DAG
  with 18 tasks whose failures can be hard to attribute to a specific region.

Factory pattern
---------------
``make_region_dag(region)`` closes over the region string and the region-scoped
Asset, producing a fully independent DAG object.  All six DAGs are registered at
module level via ``globals()`` so Airflow's DagBag discovers them from this single
file.

DAG IDs produced
----------------
    gen_sales_us_east
    gen_sales_us_west
    gen_sales_eu_west
    gen_sales_eu_central
    gen_sales_apac_au
    gen_sales_apac_jp

Asset URIs produced (one per DAG)
----------------------------------
    s3://galaxycommerce-sales-raw/raw/sales/region=us-east/
    s3://galaxycommerce-sales-raw/raw/sales/region=us-west/
    ... (one per region)

Environment variables required on workers
------------------------------------------
    AWS_ACCESS_KEY_ID          )
    AWS_SECRET_ACCESS_KEY      ) standard boto3 env-var chain
    AWS_DEFAULT_REGION         )
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.exceptions import AirflowSkipException
from airflow.operators.python import get_current_context
from airflow.sdk import Asset

log = logging.getLogger(__name__)

# Heavy dependencies (boto3, pandas, generate_all) are imported inside task
# function bodies so the Airflow scheduler does not pay their import cost on
# every DAG parse cycle.

_ENTITIES = ["orders", "order_items", "customers", "products", "marketing_events"]

_REGIONS = ["us-east", "us-west", "eu-west", "eu-central", "apac-au", "apac-jp"]

_DEFAULT_ARGS = {
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
}


# ---------------------------------------------------------------------------
# Factory
# ---------------------------------------------------------------------------

def make_region_dag(region: str):
    """
    Build and return a fully wired Airflow DAG for a single region.

    The region string is captured in a closure so that all three task functions
    and the Asset URI reference the correct region without any global state.

    Parameters
    ----------
    region : str
        One of the six GalaxyCommerce regions (e.g. "us-east").

    Returns
    -------
    Airflow DAG instance, ready to be registered in the DagBag.
    """
    # Region-scoped Asset.  Downstream DAGs in Project 2 declare this URI as
    # their schedule inlet to be triggered only when this region's data lands.
    REGION_ASSET = Asset(f"s3://galaxycommerce-sales-raw/raw/sales/region={region}/")

    @dag(
        dag_id=f"gen_sales_{region.replace('-', '_')}",
        schedule="@hourly",
        start_date=datetime(2025, 1, 1),
        catchup=False,
        max_active_runs=1,
        tags=["galaxycommerce", "data-gen", "project-1", region],
        default_args=_DEFAULT_ARGS,
        doc_md=f"Hourly sales data generation for region **{region}**. "
               f"See module docstring in `gen_sales_regions.py` for full details.",
    )
    def _region_dag():

        @task()
        def generate_and_upload() -> dict:
            """
            Generate synthetic sales data for this region and upload to S3.

            Calls generate_all() with the logical hour as the seed anchor so
            that retrying the same interval always produces the same Parquet
            files (idempotent).  S3 put_object overwrites the same key on
            retry, so partial uploads are corrected automatically.

            Returns
            -------
            dict
                { entity_name: row_count } for the five entities written.
                Passed downstream to validate_upload via XCom.
            """
            import io

            import boto3
            import pandas as pd

            from include.scripts.generate_sales_data import generate_all

            context = get_current_context()
            # context["logical_date"] is a pendulum DateTime (tz-aware UTC).
            # Strip tzinfo before passing to generate_all: Faker's
            # date_time_between() raises TypeError on tz-aware bounds.
            logical_dt: datetime = context["logical_date"].replace(tzinfo=None)
            date_str = logical_dt.strftime("%Y-%m-%d")
            hour = logical_dt.hour

            s3 = boto3.client("s3")
            bucket = "galaxycommerce-sales-raw"

            result = generate_all(region, logical_dt=logical_dt)
            log.info("region=%s injection_summary=%s", region, result["injection_summary"])

            row_counts: dict[str, int] = {}
            for entity in _ENTITIES:
                df: pd.DataFrame = result[entity]
                key = (
                    f"raw/sales/"
                    f"region={region}/"
                    f"date={date_str}/"
                    f"hour={hour:02d}/"
                    f"{entity}.parquet"
                )
                buf = io.BytesIO()
                df.to_parquet(buf, index=False, engine="pyarrow")
                s3.put_object(Bucket=bucket, Key=key, Body=buf.getvalue())
                row_counts[entity] = len(df)
                log.info("  Uploaded s3://%s/%s  (%d rows)", bucket, key, len(df))

            return row_counts

        @task()
        def validate_upload(row_counts: dict) -> dict:
            """
            Assert that at least one order row was written for this region.

            Logs a one-line summary for quick visual inspection in the task log.
            Raises AirflowSkipException (not AirflowFailException) when orders == 0
            so that the retry policy is not consumed on what is a data-logic
            issue rather than a transient infrastructure failure.  The skip state
            propagates to emit_region_asset, preventing the Asset from being
            emitted until a human clears the run.
            """
            n_orders = row_counts.get("orders", 0)

            log.info(
                "%s: %d orders, %d items, %d customers, %d products, %d mktg_events",
                region,
                n_orders,
                row_counts.get("order_items", 0),
                row_counts.get("customers", 0),
                row_counts.get("products", 0),
                row_counts.get("marketing_events", 0),
            )

            if n_orders == 0:
                raise AirflowSkipException(
                    f"{region}: 0 order rows written — skipping asset emission. "
                    "Investigate generate_and_upload logs before retrying."
                )

            return row_counts

        @task(outlets=[REGION_ASSET])
        def emit_region_asset(_row_counts: dict) -> bool:
            """
            Emit the region-scoped S3 Asset to unblock downstream DAGs.

            The outlets declaration on the decorator registers the asset event
            in the Airflow metadata DB when this task succeeds.  Downstream DAGs
            that declare this region's Asset as their schedule inlet are triggered
            automatically.  This task contains no business logic; the decorator
            does the work.  ``_row_counts`` is received only to wire the TaskFlow
            dependency (validate_upload must succeed first).
            """
            log.info("Asset emitted: %s", REGION_ASSET.uri)
            return True

        # Task wiring
        counts = generate_and_upload()
        validated = validate_upload(counts)
        emit_region_asset(validated)

    return _region_dag()


# ---------------------------------------------------------------------------
# Register all six DAGs in the DagBag
# ---------------------------------------------------------------------------

for _region in _REGIONS:
    globals()[f"gen_sales_{_region.replace('-', '_')}"] = make_region_dag(_region)
