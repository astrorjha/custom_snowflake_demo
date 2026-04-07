"""
include/config/dq_config.py
===========================
Central configuration for the GalaxyCommerce synthetic data generator.

All tuneable knobs live here so that the data-generation scripts and the
Airflow DAG never contain magic numbers.  Change a value here and every
component picks it up automatically on the next run.

Contents
--------
DQ_RATES
    Probabilities / fractions that control how many records are deliberately
    corrupted when inject_quality_issues() is called.  These are intentionally
    small so that the "dirty" dataset is realistic — a minority of bad rows
    mixed in with mostly-good data, mirroring real-world pipelines.

REGIONS
    The six geographic regions that GalaxyCommerce operates in.  Each region
    maps to its own customer pool, order stream, and S3 partition.

REGION_CURRENCY
    ISO 4217 currency code for each region.  Used to populate the currency
    column on order records and to drive the invalid_currency DQ injection
    (which overwrites the correct code with the sentinel value "XXX").

PRODUCT_CATEGORIES
    Taxonomy used both by the product generator (to assign categories) and by
    the noun-lookup dictionary that constructs realistic product names.

Note: order volume configuration (base rate, intraday curve, seasonality,
holidays) lives in include/config/volume_config.py, not here.
"""

# ---------------------------------------------------------------------------
# Data-quality injection rates
# ---------------------------------------------------------------------------

DQ_RATES = {
    # Fraction of orders that will be duplicated (row appended verbatim).
    # Simulates double-processing from an upstream retry or CDC fan-out.
    "duplicate_order_rate": 0.02,

    # Fraction of orders whose customer_id will be set to NULL.
    # Simulates a failed FK lookup or a guest-checkout record with no account.
    "null_customer_rate": 0.01,

    # Fraction of orders whose total_amount will be flipped to a negative value.
    # Simulates a refund being recorded against the wrong table, or a sign error
    # in a currency-conversion step.
    "negative_revenue_rate": 0.005,

    # Fraction of orders whose total_amount will be inflated by 15%, creating a
    # mismatch between the order header and the sum of its line items.
    # Simulates a downstream aggregation bug or a partial late-update.
    "revenue_mismatch_rate": 0.01,

    # Fraction of orders for which ALL order_items rows will be deleted, leaving
    # the order header with no line items ("orphaned order").
    # Simulates a failed write to the items table while the header committed.
    "orphaned_items_rate": 0.005,

    # Fraction of orders whose currency will be replaced with the sentinel "XXX"
    # (not a valid ISO 4217 code).
    # Simulates a missing currency-mapping in an ETL lookup table.
    "invalid_currency_rate": 0.003,

    # Probability that a region's data batch arrives late to the landing zone.
    # Reserved for future use by a late-arrival simulation task.
    "late_arriving_region_probability": 0.05,

    # Probability that a region's Parquet file will have a column added or
    # renamed, breaking downstream schema expectations.
    # Reserved for future use by a schema-drift simulation task.
    "schema_drift_probability": 0.01,
}

# ---------------------------------------------------------------------------
# Regions and currencies
# ---------------------------------------------------------------------------

# The six GalaxyCommerce operating regions.  Order is significant: it determines
# the processing sequence inside generate_and_upload and the display order in
# the validate_upload log table.
REGIONS = ["us-east", "us-west", "eu-west", "eu-central", "apac-au", "apac-jp"]

# Primary trading currency for each region (ISO 4217).
# Used to populate orders.currency in the clean dataset; the invalid_currency
# DQ injection overrides this value with "XXX" on a small fraction of rows.
REGION_CURRENCY = {
    "us-east":   "USD",
    "us-west":   "USD",
    "eu-west":   "EUR",
    "eu-central": "EUR",
    "apac-au":   "AUD",
    "apac-jp":   "JPY",
}

# ---------------------------------------------------------------------------
# Product taxonomy
# ---------------------------------------------------------------------------

# Each category maps to a fixed noun list in generate_products()
# and is stored on every product row so downstream queries can filter by category.
PRODUCT_CATEGORIES = [
    "Electronics",
    "Accessories",
    "Audio",
    "Networking",
    "Storage",
    "Wearables",
]
