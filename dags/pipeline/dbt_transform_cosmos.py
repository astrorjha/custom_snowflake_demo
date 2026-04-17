"""
dags/pipeline/dbt_transform_cosmos.py
=======================================
Runs the full GalaxyCommerce dbt project using Astronomer Cosmos.

How Cosmos works here
---------------------
Cosmos parses the dbt project at DAG load time and converts every dbt model,
test, seed, and snapshot into an individual Airflow task.  This gives
model-level visibility in the Airflow UI — you can see exactly which model
failed, retry it in isolation, and inspect its logs — without any hand-written
task definitions.  Full asset lineage (model → model → table) is visible in
Astro Observe.

Queue routing
-------------
The ``queue="dbt"`` setting in ``operator_args`` routes every dbt model
execution task to a dedicated larger worker pool in Astro Hosted.  dbt
compilation is CPU- and memory-intensive; keeping it off the default worker
queue prevents it from starving other DAGs during heavy transform runs.

Asset emission
--------------
``RenderConfig(emit_datasets=True)`` causes Cosmos to emit an individual
Airflow Asset for each dbt model when it completes successfully.  Future
pipelines can declare any of those model-level Assets as their schedule inlet,
enabling fine-grained asset-based scheduling without coupling to this DAG's
run cadence directly.

Asset topology
--------------
Inlet  (schedule):  snowflake://DEMO/RAW
Outlet (emitted):   snowflake://DEMO/RPT/RPT_DAILY_KPIS
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta

from airflow.sdk import dag, task
from airflow.sdk import Asset
from cosmos import (
    DbtTaskGroup,
    ExecutionConfig,
    ProfileConfig,
    ProjectConfig,
    RenderConfig,
)
from cosmos.profiles import SnowflakeUserPasswordProfileMapping

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Asset definitions
# ---------------------------------------------------------------------------

RAW_ASSET = Asset("snowflake://DEMO/RAW")
RPT_ASSET = Asset("snowflake://DEMO/RPT/RPT_DAILY_KPIS")

# ---------------------------------------------------------------------------
# Cosmos configuration
# ---------------------------------------------------------------------------

profile_config = ProfileConfig(
    profile_name="galaxycommerce",
    target_name="prod",
    profile_mapping=SnowflakeUserPasswordProfileMapping(
        conn_id="snowflake_default",
        profile_args={
            "database": "DEMO",
            "schema": "STG",
            "warehouse": "HUMANS",
            "role": "RAVIJHA",
        },
    ),
)

execution_config = ExecutionConfig(
    dbt_executable_path="/usr/local/airflow/dbt_venv/bin/dbt",
)

project_config = ProjectConfig(
    dbt_project_path="/usr/local/airflow/dbt",
)

render_config = RenderConfig(
    emit_datasets=True,
)

# ---------------------------------------------------------------------------
# DAG
# ---------------------------------------------------------------------------

_DEFAULT_ARGS = {
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    dag_id="dbt_transform_cosmos",
    schedule=[RAW_ASSET],
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["galaxycommerce", "pipeline", "project-2", "dbt", "cosmos"],
    default_args=_DEFAULT_ARGS,
)
def dbt_transform_cosmos():

    # ------------------------------------------------------------------
    # Task 1: Assert RAW tables are fresh before running dbt
    # ------------------------------------------------------------------

    @task(queue='default')
    def check_raw_freshness() -> int:
        """
        Confirm that RAW_ORDER has received rows within the last 2 hours.
        Raises AirflowFailException if the count is zero, preventing dbt
        from running against stale source data and producing misleading results.
        """
        from airflow.exceptions import AirflowFailException
        from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

        hook = SnowflakeHook(snowflake_conn_id="snowflake_default")

        result = hook.get_first(
            """
            SELECT COUNT(*)
            FROM DEMO.RAW.RAW_ORDER
            WHERE _loaded_at >= DATEADD('hour', -2, CURRENT_TIMESTAMP())
            """
        )
        count = result[0] if result else 0
        log.info("RAW_ORDER rows loaded in last 2 hours: %d", count)

        if count == 0:
            raise AirflowFailException(
                "RAW tables are stale — no data loaded in last 2 hours"
            )

        return count

    # ------------------------------------------------------------------
    # Task group 2: Full dbt project via Cosmos
    # Each dbt model becomes an individual Airflow task inside this group.
    # ------------------------------------------------------------------

    dbt_transform = DbtTaskGroup(
        group_id="dbt_transform",
        project_config=project_config,
        profile_config=profile_config,
        execution_config=execution_config,
        render_config=render_config,
        default_args={"retries": 2, "queue": "dbt"},
        operator_args={"install_deps": True},
    )

    # ------------------------------------------------------------------
    # Task 3: Emit the reporting Asset to signal dashboards can refresh
    # ------------------------------------------------------------------

    @task(outlets=[RPT_ASSET], queue='default')
    def emit_rpt_asset(_count: int) -> bool:
        """
        Emit RPT_ASSET once dbt has successfully populated RPT_DAILY_KPIS.
        Downstream dashboard refresh jobs or alert DAGs can declare this Asset
        as their schedule inlet.
        """
        log.info("dbt pipeline complete — RPT_DAILY_KPIS ready for dashboards.")
        return True

    # ------------------------------------------------------------------
    # Wire the task graph
    # ------------------------------------------------------------------

    freshness = check_raw_freshness()
    freshness >> dbt_transform >> emit_rpt_asset(freshness)


dbt_transform_cosmos()
