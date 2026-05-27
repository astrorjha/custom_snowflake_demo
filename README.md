Overview
========
GalaxyCommerce is a fully self-contained demo pipeline for Astronomer Sales Engineers. It simulates a production-grade e-commerce data platform across 6 global regions, covering synthetic data generation, S3 landing, Apache Iceberg ingestion into Snowflake, and dbt transformation via Astronomer Cosmos — all orchestrated on Astro using Airflow 3 Asset-based scheduling.
 
No external data sources are required. Everything is generated synthetically and deterministically by the pipeline itself, making this safe to run in any demo environment.
 
Pipeline Architecture
=====================
The pipeline is composed of 4 DAGs connected entirely through Airflow 3 Assets. There are no sensors, no polling, and no manual triggers — each stage fires exactly when its upstream data is ready.
 
    gen_sales_{region} (x6, @hourly)
        Each of the 6 region DAGs generates synthetic sales data and uploads it to S3.
        When successful, it emits a region-scoped S3 Asset.
 
    regional_aggregator
        Fires automatically once all 6 region Assets have been emitted for a scheduling
        window. Acts as a fan-in bridge and emits a single combined PIPELINE_ASSET.
 
    ingest_s3_to_snowflake
        Triggered by PIPELINE_ASSET. Reads Parquet files from S3, writes them as Apache
        Iceberg to S3 via PyIceberg, registers tables in AWS Glue, and surfaces them in
        Snowflake as External Iceberg Tables. Emits RAW_ASSET on success.
 
    dbt_transform_cosmos
        Triggered by RAW_ASSET. Runs the full dbt project via Astronomer Cosmos,
        transforming RAW Iceberg tables through staging, intermediate, marts, and
        reporting layers. Emits RPT_ASSET when rpt_daily_kpis is ready.
 
Project Contents
================
- dags/gen/gen_sales_regions.py: Defines 6 independent hourly DAGs using a factory
  pattern — one per GalaxyCommerce region (us-east, us-west, eu-west, eu-central,
  apac-au, apac-jp). Each DAG generates synthetic sales data for 5 entities (orders,
  order_items, customers, products, marketing_events), uploads them as Parquet to S3,
  and emits a region-scoped Asset URI. The factory pattern means a failure in one
  region never blocks or retries another.
- dags/gen/regional_aggregator.py: Fan-in DAG that declares all 6 region Assets as
  its schedule inlet. Fires once per hour only after every region has landed data,
  then emits a single combined PIPELINE_ASSET to unblock the ingestion DAGs.
- dags/pipeline/ingest_s3_to_snowflake.py: 5-task ingestion pipeline. Validates S3
  manifest, converts Parquet to Iceberg via PyIceberg and registers in AWS Glue,
  runs idempotent External Iceberg Table DDL in Snowflake, validates row counts, then
  emits RAW_ASSET. Data lands as open-format Iceberg — readable by Snowflake,
  Databricks, or any Iceberg-compatible engine without duplication.
- dags/pipeline/dbt_transform_cosmos.py: Runs the full dbt project as an Airflow DAG
  using Astronomer Cosmos. Every dbt model becomes an individual Airflow task inside a
  DbtTaskGroup, giving model-level visibility, retry granularity, and lineage in the
  Airflow UI. dbt tasks are routed to a dedicated 'dbt' worker queue to avoid
  competing with default pipeline tasks on Astro Hosted.
- dbt/: Full dbt project (galaxycommerce) with 4 layers:
    - staging/ — Views over RAW Iceberg sources. Type casting, null filters,
      lowercase normalization.
    - intermediate/ — Enrichment and aggregation views. Joins orders to customers
      and regions, enriches order items with product data, and aggregates regional
      daily metrics including the marketing funnel.
    - marts/ — Fact and dimension tables (fact_orders, fact_order_items,
      dim_customers, dim_products) with surrogate keys via dbt_utils.
    - reporting/ — rpt_daily_kpis: the terminal reporting model. One row per region
      per day with gross revenue, return rate, CTR, conversion rate, revenue per
      impression, gold customer order mix, promo rate, and 7-day rolling averages.
- include/scripts/generate_sales_data.py: Seeded synthetic data engine. Generates
  realistic e-commerce data with a full volume model (intraday curve, day-of-week,
  monthly seasonality, regional holiday calendar). Fully deterministic — the same
  region and logical hour always produce identical Parquet files, making retries
  idempotent. Also injects a small fraction of intentional data quality issues on
  every run (see Data Quality section below).
- include/config/dq_config.py: Configures the DQ injection rates, regional currency
  codes, and product taxonomy used by the data generator.
- include/config/volume_config.py: Configures the order volume model including the
  base rate, intraday multipliers, day-of-week and monthly seasonality, and the
  regional holiday calendar (Black Friday, Cyber Monday, Boxing Day AU, Singles Day
  JP, Golden Week JP, etc.).
- include/sql/create_iceberg_tables.sql: Idempotent DDL for creating External Iceberg
  Tables in Snowflake. Run on every pipeline execution via SQLExecuteQueryOperator.
  Requires a one-time ACCOUNTADMIN setup of the External Volume and Catalog
  Integration (see Deploy section below).
- include/sql/copy_into/: Legacy COPY INTO SQL files retained for reference. These
  are not used in the active pipeline, which has migrated to External Iceberg Tables.
- include/iam/airflow_glue_policy.json: Least-privilege IAM policy granting Airflow
  workers the minimum permissions needed to read/write S3 and register tables in
  AWS Glue.
- Dockerfile: Extends Astro Runtime with a separate dbt virtual environment at
  /usr/local/airflow/dbt_venv to avoid dependency conflicts between dbt-snowflake
  and the Airflow provider packages.
- requirements.txt: Python dependencies including astronomer-cosmos[dbt-snowflake],
  apache-airflow-providers-snowflake, pyiceberg[s3filesystem,glue], boto3, pandas,
  and faker.
Data Quality
============
The data generator deliberately injects a small number of corrupted records on every
run to simulate real-world data quality problems. These issues are seeded and
deterministic — the same rows are corrupted on every retry of the same DAG interval.
 
- Duplicate orders (2%): Row appended verbatim. Simulates upstream retry or CDC fan-out.
- Null customer_id (1%): FK set to NULL. Simulates guest checkout or failed lookup.
- Negative total_amount (0.5%): Amount flipped negative. Simulates a sign error in a
  currency conversion step or a refund recorded against the wrong table.
- Revenue mismatch (1%): Order header inflated 15% while line items are unchanged.
  Creates a detectable discrepancy between total_amount and the sum of order_items.
- Orphaned orders (0.5%): All order_items rows deleted for a sample of orders. The
  order header remains, leaving orders with no line items.
- Invalid currency (0.3%): currency replaced with 'XXX' (not a valid ISO 4217 code).
  Simulates a missing entry in a currency lookup table.
These issues make the pipeline ideal for demonstrating Astro Observe. Real, findable
data quality problems are present in every run across the full lineage from RAW Iceberg
through to rpt_daily_kpis.
 
Demo Highlights
===============
Use this section to plan which parts of the pipeline to show depending on what the
prospect cares about.
 
Airflow 3 Asset-Based Scheduling
---------------------------------
Best for: Customers asking about event-driven pipelines, replacing sensors, or
simplifying cross-DAG dependencies.
 
Show the Asset lineage graph spanning all 4 DAGs in the Airflow UI. Point out that
regional_aggregator fires automatically the moment all 6 region DAGs have emitted —
no polling, no sensors, no ExternalTaskSensor workarounds. The fan-in pattern
(wait for all N upstreams before triggering downstream) is a real architectural
problem that previously required custom sensors or external orchestration. Here it
is solved natively with Asset scheduling in a few lines of Python.
 
Key line: "The pipeline doesn't run on a schedule. It runs when data is ready.
Each stage fires only when its upstream stage emits an Asset event — which is how
Airflow 3 replaces the entire class of sensor-based wait logic most teams have
accumulated over years."
 
DAG Factory Pattern
--------------------
Best for: Customers with many similar pipelines, template-driven DAG generation,
or concerns about DAG sprawl and operational overhead.
 
Show that gen_sales_regions.py is a single Python file that produces 6 independent
DAGs via a factory function. Each is independently schedulable, independently
retryable, and independently pauseable from the Airflow UI.
 
Key line: "Instead of one DAG with 18 tasks where a failure in APAC retries and
blocks everything else, you get 6 small DAGs — one per region — each with its own
retry policy and its own Asset signal. A transient S3 error in Japan doesn't slow
down US East."
 
Astronomer Cosmos
------------------
Best for: Customers already using dbt, evaluating dbt + Airflow integration, or
coming from dbt Cloud.
 
Expand the dbt_transform task group in the Airflow UI. Every model — stg_orders,
int_orders_enriched, fact_orders, rpt_daily_kpis — is its own Airflow task with
its own logs and its own retry button. Cosmos parsed the dbt project at DAG load
time and built the task graph automatically.
 
Key line: "Cosmos converts your dbt project into an Airflow task graph without any
hand-written task code. You write dbt models. You get model-level observability,
lineage, and retry granularity in the Airflow UI for free."
 
Also point to queue="dbt" on the DbtTaskGroup. dbt compilation is CPU-intensive —
routing it to a dedicated larger worker pool prevents it from starving other DAGs
during heavy transform runs. This is a one-line config change in Cosmos.
 
Apache Iceberg + Snowflake External Tables
-------------------------------------------
Best for: Customers evaluating open table formats, multi-cloud or multi-engine
strategies, or looking to avoid Snowflake-proprietary storage.
 
Walk through ingest_s3_to_snowflake and show the flow: PyIceberg writes to S3,
registers in AWS Glue, Snowflake reads via External Volume and Catalog Integration.
Snowflake never owns the data — it queries open Iceberg directly from S3.
 
Key line: "The source of truth lives in S3 as open Iceberg. Snowflake reads it
through an External Volume — so you get full Snowflake SQL performance on top of
data that Databricks or any other Iceberg engine can also query directly from the
same location. No duplication. No vendor lock-in."
 
Data Quality with Astro Observe
---------------------------------
Best for: Customers asking about data observability, pipeline reliability, or
catching issues before they reach reporting.
 
Point to dq_config.py and walk through the injection types. Every pipeline run
introduces duplicates, null FKs, revenue mismatches between order headers and line
items, and orphaned orders. These are all detectable in Astro Observe using data
lineage and anomaly detection before they reach rpt_daily_kpis.
 
Key line: "We bake real data quality issues into every run — the same kinds of
problems that show up in production pipelines. The question isn't whether bad data
exists. It's whether you catch it before it reaches the dashboard. That's what
Observe is built for."
 
Worker Queue Routing
---------------------
Best for: Customers on Astro Hosted asking about resource management, worker sizing,
or cost optimization.
 
In dbt_transform_cosmos.py, point to queue="dbt" on the DbtTaskGroup and
queue="default" on the surrounding lightweight tasks. This routes compute-intensive
dbt compilation to a dedicated larger worker pool without touching the rest of the
pipeline.
 
Key line: "On Astro Hosted you define multiple worker queues with different sizes.
Tasks declare which queue they need. You're not paying for a large worker to sit
idle between dbt runs — and your lightweight tasks aren't waiting behind a heavy
dbt compilation job."
 
Deploy Your Project Locally
============================
Prerequisites: Astro CLI, Docker or OrbStack
 
Start Airflow on your local machine by running 'astro dev start'.
 
This command will spin up five Docker containers on your machine, each for a
different Airflow component:
 
- Postgres: Airflow's Metadata Database
- Scheduler: The Airflow component responsible for monitoring and triggering tasks
- DAG Processor: The Airflow component responsible for parsing DAGs
- API Server: The Airflow component responsible for serving the Airflow UI and API
- Triggerer: The Airflow component responsible for triggering deferred tasks
When all five containers are ready the command will open the browser to the Airflow
UI at http://localhost:8080/.
 
You will need the following Airflow connections configured before running the full
pipeline:
 
- snowflake_default: Snowflake account gp21411.us-east-1, database DEMO, warehouse
  HUMANS, role RAVIJHA, private key PEM authentication.
- AWS credentials available via the standard boto3 env-var chain
  (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_DEFAULT_REGION) for S3 and Glue.
One-time Snowflake setup (requires ACCOUNTADMIN):
- Create External Volume: galaxycommerce_ext_vol
- Create Catalog Integration: galaxycommerce_glue_catalog
Note: If you already have either of the above ports allocated, you can either
stop your existing Docker containers or change the port.
https://www.astronomer.io/docs/astro/cli/troubleshoot-locally#ports-are-not-available-for-my-local-airflow-webserver
 
Deploy Your Project to Astronomer
===================================
If you have an Astronomer account, pushing code to a Deployment on Astronomer is
simple. For deploying instructions, refer to Astronomer documentation:
https://www.astronomer.io/docs/astro/deploy-code/
 
This pipeline is designed for Astro Hosted. The Dockerfile references the Astro
Runtime base image, and the worker queue configuration in dbt_transform_cosmos.py
(queue="dbt") assumes a multi-queue worker setup available on Astro Hosted
deployments.
