"""
tests/test_data_generation.py
==============================
Unit tests for the GalaxyCommerce synthetic data generator.

These tests exercise include.scripts.generate_sales_data in isolation —
no S3 connection, no Airflow runtime, and no external services are required.
They can be run locally with:

    pytest tests/test_data_generation.py -v
"""

import pytest
from datetime import datetime

import pandas as pd

from include.config.dq_config import REGIONS
from include.scripts.generate_sales_data import generate_all


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def us_east_result():
    """
    Run generate_all once for us-east and share the result across all tests
    in this module that need it.  scope="module" means the (relatively slow)
    generation step runs only once per pytest session.
    """
    return generate_all("us-east", datetime(2025, 6, 1, 14, 0, 0))


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def test_generate_all_us_east(us_east_result):
    """
    Smoke-test that generate_all returns all expected keys and non-empty
    DataFrames, that referential integrity holds between order_items and
    orders, and that the DQ injection summary is consistent with the data.
    """
    result = us_east_result

    # --- All 5 entity keys present ---
    expected_keys = {"orders", "order_items", "customers", "products", "marketing_events"}
    assert expected_keys.issubset(result.keys()), (
        f"Missing keys: {expected_keys - result.keys()}"
    )

    orders      = result["orders"]
    order_items = result["order_items"]

    # --- Non-empty core tables ---
    assert len(orders) > 0, "orders DataFrame is empty"
    assert len(order_items) > 0, "order_items DataFrame is empty"

    # --- Referential integrity: every item references a known order ---
    # DQ injection (orphaned_items_rate) removes items whose orders exist but
    # have no items — it does NOT create items whose orders are missing.
    # So order_items.order_id must always be a subset of orders.order_id.
    order_ids       = set(orders["order_id"])
    item_order_ids  = set(order_items["order_id"])
    orphaned = item_order_ids - order_ids
    assert not orphaned, (
        f"{len(orphaned)} order_items rows reference unknown order_ids: "
        f"{list(orphaned)[:5]}"
    )

    # --- injection_summary is a non-empty dict ---
    summary = result["injection_summary"]
    assert isinstance(summary, dict), "injection_summary is not a dict"
    assert len(summary) > 0, "injection_summary is empty"

    # --- Negative revenue count matches the injection summary exactly ---
    # inject_quality_issues() samples from the original (pre-duplication) rows
    # and negates only those rows; duplicated copies retain the positive original
    # value.  Therefore the count of negative total_amount rows in the final
    # DataFrame must equal summary["negative_revenue_rate"] precisely.
    n_negative = int((orders["total_amount"] < 0).sum())
    assert n_negative == summary["negative_revenue_rate"], (
        f"Expected {summary['negative_revenue_rate']} negative-revenue rows "
        f"(from injection_summary) but found {n_negative} in orders DataFrame"
    )


def test_determinism():
    """
    Calling generate_all with the same arguments twice must return identical
    orders DataFrames — confirming that seeding is deterministic and the
    pipeline is safe to retry without producing different data.
    """
    kwargs = {"region": "eu-west", "logical_dt": datetime(2025, 3, 15, 9, 0, 0)}

    result_a = generate_all(**kwargs)
    result_b = generate_all(**kwargs)

    pd.testing.assert_frame_equal(
        result_a["orders"].reset_index(drop=True),
        result_b["orders"].reset_index(drop=True),
        check_like=False,
        obj="orders (determinism check)",
    )


def test_all_regions_generate():
    """
    generate_all must produce at least one order row for every supported region.
    Uses a fixed logical_dt so the volume model's intraday multiplier is held
    constant across all regions (14:00 UTC is a daytime hour for all of them).
    """
    logical_dt = datetime(2025, 6, 1, 14, 0, 0)

    for region in REGIONS:
        result = generate_all(region, logical_dt)
        n_orders = len(result["orders"])
        assert n_orders > 0, (
            f"generate_all produced 0 order rows for region '{region}' "
            f"at {logical_dt}"
        )
