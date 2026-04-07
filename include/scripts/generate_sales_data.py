"""
include/scripts/generate_sales_data.py
=======================================
Synthetic sales-data generator for the GalaxyCommerce demo pipeline.

Overview
--------
This module produces five interrelated DataFrames that together represent one
hour of e-commerce activity for a single geographic region:

    products         — static product catalogue (same across all regions/hours)
    customers        — stable regional customer pool (same across all hours for
                       a given region)
    orders           — one row per order placed during the logical hour
    order_items      — one row per line item; FK → orders and products
    marketing_events — impression / click / purchase funnel events for the hour

After the clean data is generated, inject_quality_issues() deliberately corrupts
a small, configurable fraction of records to simulate real-world data-quality
problems (duplicates, nulls, sign errors, schema mismatches, etc.).  The
injection rates are defined in include/config/dq_config.py.

Determinism and idempotency
---------------------------
Every generator function is fully seeded:

* generate_products()  — fixed seed 42; identical output on every call.
* generate_customers() — seeded on region name; same pool for a given region
                         regardless of when the DAG runs.
* generate_orders() and generate_marketing_events()
                       — seeded on (region, logical_hour); re-running the same
                         DAG interval always produces byte-identical Parquet files.
* inject_quality_issues()
                       — seeded via the ``seed`` argument (derived from region +
                         logical_hour in generate_all); same rows are corrupted
                         on every retry of the same interval.

All UUID generation goes through Faker (fake.uuid4()) rather than the stdlib
uuid.uuid4(), which draws from the OS entropy pool and would break reproducibility.

Seeding model
-------------
Faker.seed() is a *class-level* call that mutates shared state and would bleed
across generators called in sequence within the same process.  This module uses
fake.seed_instance() exclusively, which gives each Faker object its own isolated
random generator.  The two generator seeds used per region/hour are:

    orders seed        = _region_dt_seed(region, logical_dt)
    marketing seed     = _region_dt_seed(region, logical_dt) + 1
    DQ seed            = _region_dt_seed(region, logical_dt) + 999  (in generate_all)

The +1 / +999 offsets ensure the three generators never share a seed value.

Usage
-----
Call generate_all() from the Airflow task; it orchestrates the full pipeline:

    from include.scripts.generate_sales_data import generate_all
    from datetime import datetime

    result = generate_all("us-east", datetime(2025, 6, 1, 14, 0, 0))
    orders_df          = result["orders"]
    order_items_df     = result["order_items"]
    customers_df       = result["customers"]
    products_df        = result["products"]
    marketing_events_df = result["marketing_events"]
    injection_summary  = result["injection_summary"]

Important: logical_dt must be a **naive UTC datetime** (no tzinfo).  The Airflow
DAG strips tzinfo from the pendulum-aware context["logical_date"] before calling
this module, because Faker's date_time_between() does not accept tz-aware bounds.

The module is safe to import; no side effects occur at import time.
"""

from __future__ import annotations

import math
import random
from collections import OrderedDict
from datetime import datetime, timedelta

import pandas as pd
from faker import Faker

from include.config.dq_config import (
    DQ_RATES,
    PRODUCT_CATEGORIES,
    REGION_CURRENCY,
)
from include.config.volume_config import (
    BASE_ORDERS_PER_REGION_PER_HOUR,
    DOW_MULTIPLIERS,
    INTRADAY_MULTIPLIERS,
    MONTHLY_MULTIPLIERS,
    REGION_UTC_OFFSETS,
    VOLUME_NOISE_PCT,
    regional_holidays,
)

# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------

def _region_seed(region: str) -> int:
    """
    Convert a region name string into a stable 31-bit integer seed.

    The bytes of the region name are interpreted as a little-endian integer and
    then reduced modulo 2^31 to stay within the range accepted by most random
    generators.  The same region string always produces the same seed value,
    across Python versions and platforms, because the encoding (UTF-8) and byte
    order (little-endian) are fixed.

    Used as the seed for generate_customers() so that each region has a stable,
    independent customer pool that does not change between hourly runs.
    """
    return int.from_bytes(region.encode(), "little") % (2**31)


def _region_dt_seed(region: str, logical_dt: datetime) -> int:
    """
    Combine a region name and a logical datetime into a stable 31-bit seed.

    The seed is computed by adding the region seed (see _region_seed) to the
    Unix timestamp of the *start of the logical hour* (minutes/seconds/microseconds
    zeroed out), then reducing modulo 2^31.  Flooring to the hour means that any
    datetime within the same hour produces the same seed, which is what we want:
    the same hour re-run always generates the same data.

    Used as the base seed for generate_orders() and generate_marketing_events().
    """
    hour_stamp = logical_dt.replace(minute=0, second=0, microsecond=0).timestamp()
    return (_region_seed(region) + int(hour_stamp)) % (2**31)


def _seeded_faker(seed: int) -> Faker:
    """
    Create and return a Faker instance with its own isolated random state.

    seed_instance() sets a *per-object* random generator, leaving the class-level
    shared state untouched.  This means multiple Faker objects created in the same
    process (e.g. one per region inside a single Airflow task) do not interfere
    with each other's random sequences, which is essential for determinism.

    Contrast with Faker.seed() (class method), which reseeds the shared generator
    and would cause later-created instances to see a different sequence than
    intended.
    """
    fake = Faker()
    fake.seed_instance(seed)
    return fake


def _random_timestamp_in_hour(fake: Faker, logical_dt: datetime) -> datetime:
    """
    Return a random datetime uniformly distributed within the calendar hour of
    logical_dt.

    The result always falls in [HH:00:00, HH:59:59] on the same date as
    logical_dt.  The offset in seconds is drawn from the provided Faker instance
    so the result is deterministic given the caller's seed.
    """
    base = logical_dt.replace(minute=0, second=0, microsecond=0)
    offset_seconds = fake.random_int(min=0, max=3599)
    return base + timedelta(seconds=offset_seconds)


# ---------------------------------------------------------------------------
# Volume model
# ---------------------------------------------------------------------------

def compute_order_volume(region: str, logical_dt: datetime, seed: int) -> int:
    """
    Compute the number of orders to generate for a given region and hour.

    Applies a chain of five independent multipliers anchored to the region's
    local time (not UTC), then adds a small seeded noise term so consecutive
    runs of the same hour are not byte-identical:

        volume = BASE
                 × intraday[local_hour]
                 × day_of_week[local_weekday]
                 × monthly[month]
                 × holiday_multiplier(region, local_date)
                 × uniform_noise(±VOLUME_NOISE_PCT%)

    All multipliers and the base rate are defined in volume_config.py.

    The noise uses stdlib ``random.Random(seed)`` rather than Faker so that
    volume computation is self-contained and does not consume draws from the
    Faker sequences used by the data generators.  The seed is derived from
    (region, logical_dt) in generate_all(), making the result fully reproducible
    on retry.

    Parameters
    ----------
    region : str
        Must be one of REGIONS.
    logical_dt : datetime
        Naive UTC datetime representing the start of the logical hour.
    seed : int
        Seed for the noise RNG.  Must be distinct from the Faker seeds used by
        generate_orders() and generate_marketing_events() for the same hour.

    Returns
    -------
    int
        Number of orders to generate.  Always >= 1.

    Examples
    --------
    Quiet overnight hour (us-east, 03:00 UTC = 22:00 local, Tuesday, March):
        BASE(200) × intraday(0.04) × dow(0.90) × monthly(0.95) × holiday(1.0)
        × noise ≈ 7 orders

    Black Friday evening peak (us-east, 00:00 UTC = 19:00 local EST):
        BASE(200) × intraday(1.65) × dow(1.05) × monthly(1.35) × holiday(4.0)
        × noise ≈ 1,868 orders
    """
    # Shift UTC to approximate local time using the fixed regional offset.
    # timedelta handles negative offsets (e.g. us-east = -5h) correctly.
    local_dt   = logical_dt + timedelta(hours=REGION_UTC_OFFSETS[region])
    local_hour = local_dt.hour
    local_date = local_dt.date()

    intraday = INTRADAY_MULTIPLIERS[local_hour]
    dow      = DOW_MULTIPLIERS[logical_dt.weekday()]
    monthly  = MONTHLY_MULTIPLIERS[logical_dt.month]

    # Holiday lookup uses local_date.year so timezone-shifted dates that cross
    # a year boundary (e.g. apac-jp on UTC Dec 31 → local Jan 1) resolve correctly.
    holidays = regional_holidays(local_date.year)
    holiday  = holidays.get(region, {}).get(local_date, 1.0)

    # Seeded uniform noise: ±VOLUME_NOISE_PCT%
    rng   = random.Random(seed)
    noise = rng.uniform(1.0 - VOLUME_NOISE_PCT / 100, 1.0 + VOLUME_NOISE_PCT / 100)

    return max(1, round(BASE_ORDERS_PER_REGION_PER_HOUR * intraday * dow * monthly * holiday * noise))


# ---------------------------------------------------------------------------
# Public generators
# ---------------------------------------------------------------------------

def generate_products(n: int = 50) -> pd.DataFrame:
    """
    Generate the static GalaxyCommerce product catalogue.

    The catalogue is identical on every call because it uses a fixed seed (42).
    All other generators that need product data should call this function once
    and reuse the result rather than calling it repeatedly.

    Product names are constructed by combining a random adjective, a
    category-specific noun, and a random alphanumeric model suffix, e.g.
    "GX Ultra Headphones AB-042".  This gives names that look plausible in a
    retail context without requiring an external lookup table.

    Parameters
    ----------
    n : int, default 50
        Number of products to generate.  Must be >= len(PRODUCT_CATEGORIES) (6)
        to ensure every category has at least one representative.

    Returns
    -------
    pd.DataFrame
        Columns:
          product_id  (str)   — UUID v4 string
          name        (str)   — human-readable product name
          category    (str)   — one of PRODUCT_CATEGORIES
          unit_price  (float) — retail price in the range [9.99, 1499.99]
          is_active   (bool)  — True for ~90% of products; inactive products are
                                excluded from order line-item sampling
    """
    fake = _seeded_faker(42)

    adjectives = [
        "Pro", "Ultra", "Elite", "Smart", "Nano", "Flex", "Max", "Mini",
        "Turbo", "Edge", "Swift", "Pulse", "Core", "Apex", "Prime",
    ]
    nouns = {
        "Electronics": ["Tablet", "Laptop", "Monitor", "Projector", "Camera"],
        "Accessories": ["Charging Pad", "Phone Case", "Screen Protector", "Cable", "Hub"],
        "Audio": ["Headphones", "Speaker", "Earbuds", "Soundbar", "Microphone"],
        "Networking": ["Router", "Mesh Node", "Switch", "Access Point", "Extender"],
        "Storage": ["SSD", "Flash Drive", "NAS Drive", "Memory Card", "External HDD"],
        "Wearables": ["Smartwatch", "Fitness Band", "Smart Ring", "AR Glasses", "Tracker"],
    }

    rows = []
    # Cycle through categories in order so every category is represented evenly
    # before any category gets a second product.
    categories = PRODUCT_CATEGORIES * math.ceil(n / len(PRODUCT_CATEGORIES))
    for i in range(n):
        category = categories[i]
        noun = fake.random_element(nouns[category])
        adj = fake.random_element(adjectives)
        name = f"GX {adj} {noun} {fake.bothify('??-###').upper()}"
        rows.append(
            {
                "product_id": fake.uuid4(),
                "name": name,
                "category": category,
                "unit_price": round(fake.pyfloat(min_value=9.99, max_value=1499.99), 2),
                "is_active": fake.boolean(chance_of_getting_true=90),
            }
        )

    return pd.DataFrame(rows)


def generate_customers(region: str, logical_dt: datetime, n: int = 500) -> pd.DataFrame:
    """
    Generate the stable customer pool for a given region.

    The customer pool for each region is deterministic: the same region always
    produces the same 500 customers because the Faker instance is seeded with a
    hash of the region name alone (not the logical hour).  This models the idea
    that customers are pre-existing accounts; new ones don't appear every hour.

    created_at timestamps are bounded by [logical_dt - 2 years, logical_dt] so
    that all customers appear to have registered before the orders being generated.
    Using logical_dt (rather than datetime.utcnow()) as the upper bound makes
    the output reproducible: calling this function twice with the same arguments
    always returns the same DataFrame.

    Parameters
    ----------
    region : str
        Must be one of REGIONS (e.g. "us-east").
    logical_dt : datetime
        Naive UTC datetime representing the "current" time for this run.
        Used as the upper bound for created_at and as the anchor for the
        two-year lookback window.
    n : int, default 500
        Number of customers to generate.

    Returns
    -------
    pd.DataFrame
        Columns:
          customer_id (str)      — UUID v4 string
          name        (str)      — full name
          email       (str)      — email address
          region      (str)      — same as the ``region`` argument
          tier        (str)      — "bronze" (60%), "silver" (30%), or "gold" (10%)
          created_at  (datetime) — account creation timestamp within the last 2 years
    """
    fake = _seeded_faker(_region_seed(region))

    two_years_ago = logical_dt - timedelta(days=730)

    # Generate all tier assignments up front so they consume a fixed number of
    # random draws before the per-customer loop begins, keeping the sequence stable.
    # random_elements() does not accept a weights argument in Faker >=24; use
    # random_element() with an OrderedDict (weights as values) instead.
    tier_dist = OrderedDict([("bronze", 0.60), ("silver", 0.30), ("gold", 0.10)])
    tiers = [fake.random_element(tier_dist) for _ in range(n)]

    rows = []
    for i in range(n):
        created_at = fake.date_time_between(start_date=two_years_ago, end_date=logical_dt)
        rows.append(
            {
                "customer_id": fake.uuid4(),
                "name": fake.name(),
                "email": fake.email(),
                "region": region,
                "tier": tiers[i],
                "created_at": created_at,
            }
        )

    return pd.DataFrame(rows)


def generate_orders(
    region: str,
    logical_dt: datetime,
    products_df: pd.DataFrame,
    customers_df: pd.DataFrame,
    n_orders: int = BASE_ORDERS_PER_REGION_PER_HOUR,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Generate the orders and order_items DataFrames for one region/hour.

    Each order is assigned:
    - a customer drawn randomly from customers_df
    - a created_at timestamp distributed uniformly within the logical hour
    - a status drawn from a weighted distribution that reflects a realistic
      order lifecycle (most orders have shipped or been delivered)
    - a 20% chance of carrying a promo code
    - between 1 and 5 line items, each referencing an active product

    The total_amount on the order header is computed as the exact sum of its
    line-item amounts (quantity × unit_price), so the clean dataset has zero
    revenue-mismatch issues before inject_quality_issues() is called.

    Both DataFrames are seeded with _region_dt_seed(region, logical_dt), so
    re-running the same region/hour always produces identical output.

    Parameters
    ----------
    region : str
        Must be one of REGIONS.
    logical_dt : datetime
        Naive UTC datetime for the logical hour being generated.
    products_df : pd.DataFrame
        Output of generate_products().  Only rows where is_active=True are
        eligible to appear as order line items.
    customers_df : pd.DataFrame
        Output of generate_customers() for the same region.

    Returns
    -------
    tuple[pd.DataFrame, pd.DataFrame]
        orders_df:
          order_id      (str)            — UUID v4
          customer_id   (str)            — FK → customers.customer_id
          region        (str)
          created_at    (datetime)       — within the logical hour
          status        (str)            — pending/confirmed/shipped/delivered/returned
          currency      (str)            — ISO 4217 code from REGION_CURRENCY
          promo_code    (str | None)     — present on ~20% of orders
          total_amount  (float)          — sum of order_items.amount for this order

        order_items_df:
          item_id       (str)            — UUID v4
          order_id      (str)            — FK → orders.order_id
          product_id    (str)            — FK → products.product_id
          quantity      (int)            — 1–5 units
          unit_price    (float)          — copied from products at order time
          amount        (float)          — quantity × unit_price
    """
    fake = _seeded_faker(_region_dt_seed(region, logical_dt))

    currency = REGION_CURRENCY[region]

    # Weights reflect a mature order pipeline: most orders have progressed past
    # pending; returns are rare.  OrderedDict form required — random_elements()
    # does not accept a weights argument in Faker >=24.
    status_dist = OrderedDict([
        ("pending", 10), ("confirmed", 20), ("shipped", 30),
        ("delivered", 35), ("returned", 5),
    ])

    customer_ids = customers_df["customer_id"].tolist()
    active_products = products_df[products_df["is_active"]].reset_index(drop=True)

    order_rows = []
    item_rows = []

    for _ in range(n_orders):
        order_id = fake.uuid4()
        customer_id = fake.random_element(customer_ids)
        created_at = _random_timestamp_in_hour(fake, logical_dt)
        status = fake.random_element(status_dist)
        promo_code = (
            fake.bothify("PROMO-????-####").upper()
            if fake.boolean(chance_of_getting_true=20)
            else None
        )

        # Select 1–5 distinct active products for this order.
        # random_state is drawn from the seeded Faker instance so product
        # selection is reproducible.
        n_items = fake.random_int(min=1, max=5)
        selected = active_products.sample(
            n=min(n_items, len(active_products)),
            random_state=fake.random_int(0, 9999),
        )

        order_total = 0.0
        for _, product in selected.iterrows():
            quantity = fake.random_int(min=1, max=5)
            unit_price = product["unit_price"]
            amount = round(quantity * unit_price, 2)
            order_total += amount
            item_rows.append(
                {
                    "item_id": fake.uuid4(),
                    "order_id": order_id,
                    "product_id": product["product_id"],
                    "quantity": quantity,
                    "unit_price": unit_price,
                    "amount": amount,
                }
            )

        order_rows.append(
            {
                "order_id": order_id,
                "customer_id": customer_id,
                "region": region,
                "created_at": created_at,
                "status": status,
                "currency": currency,
                "promo_code": promo_code,
                # Exact sum of line items — no rounding drift at the order level.
                "total_amount": round(order_total, 2),
            }
        )

    return pd.DataFrame(order_rows), pd.DataFrame(item_rows)


def generate_marketing_events(
    region: str,
    logical_dt: datetime,
    customers_df: pd.DataFrame,
    n_orders: int = BASE_ORDERS_PER_REGION_PER_HOUR,
) -> pd.DataFrame:
    """
    Generate a three-stage marketing funnel for one region/hour.

    The funnel is monotonically decreasing in the clean state:

        impressions  = ORDERS_PER_REGION_PER_HOUR × 3   (600 at the default volume)
        clicks       ≈ impressions × 40%                 (~240)
        purchases    ≈ clicks × 60%                      (~144)

    Events are not correlated at the customer level (the same customer_id can
    appear in any stage), which is intentional: it keeps the generator simple
    and still produces a realistic funnel shape for aggregate queries.

    The Faker seed is offset by +1 from the orders seed for the same region/hour
    to ensure the two generators produce independent sequences even though they
    share the same base seed formula.

    Parameters
    ----------
    region : str
        Must be one of REGIONS.
    logical_dt : datetime
        Naive UTC datetime for the logical hour being generated.
    customers_df : pd.DataFrame
        Output of generate_customers() for the same region.  customer_ids are
        sampled from this pool and assigned to events randomly.

    Returns
    -------
    pd.DataFrame
        Columns:
          event_id    (str)      — UUID v4
          customer_id (str)      — FK → customers.customer_id
          region      (str)
          event_type  (str)      — "impression", "click", or "purchase"
          created_at  (datetime) — within the logical hour
    """
    # +1 offset ensures this seed never collides with the orders seed for the
    # same region/hour, giving independent random sequences.
    fake = _seeded_faker(_region_dt_seed(region, logical_dt) + 1)

    customer_ids = customers_df["customer_id"].tolist()

    n_impressions = n_orders * 3
    n_clicks = round(n_impressions * 0.40)
    n_purchases = round(n_clicks * 0.60)

    rows = []
    for event_type, count in [
        ("impression", n_impressions),
        ("click", n_clicks),
        ("purchase", n_purchases),
    ]:
        for _ in range(count):
            rows.append(
                {
                    "event_id": fake.uuid4(),
                    "customer_id": fake.random_element(customer_ids),
                    "region": region,
                    "event_type": event_type,
                    "created_at": _random_timestamp_in_hour(fake, logical_dt),
                }
            )

    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# DQ injection
# ---------------------------------------------------------------------------

def inject_quality_issues(
    orders_df: pd.DataFrame,
    order_items_df: pd.DataFrame,
    seed: int = 0,
) -> tuple[pd.DataFrame, pd.DataFrame, dict]:
    """
    Deliberately corrupt a small fraction of rows to simulate data-quality issues.

    Each injection type is applied independently to the *original* (pre-duplication)
    set of orders, using a different random_state per step so the samples don't
    overlap deterministically.  Because all sampling uses a fixed ``seed``, the
    same rows are corrupted on every call with the same arguments — making the
    injected dataset fully reproducible.

    Injection steps (in order)
    --------------------------
    1. **Duplicates** (duplicate_order_rate)
       A sample of order rows is appended verbatim to orders_df.  The DataFrame
       index is reset so downstream code sees a clean integer index.  All
       subsequent steps sample from the *original* rows only, so duplicates
       cannot be double-corrupted by later steps.

    2. **Null customer_id** (null_customer_rate)
       customer_id is set to None on a sample of original orders.
       Simulates guest checkouts or a failed FK resolution.

    3. **Negative total_amount** (negative_revenue_rate)
       total_amount is flipped to its negative absolute value.
       Simulates a refund recorded in the wrong direction or a sign error in
       a currency-conversion pipeline.

    4. **Revenue mismatch** (revenue_mismatch_rate)
       total_amount is inflated by 15% while order_items is left unchanged.
       This creates a detectable discrepancy between the order header and the
       sum of its line items.

    5. **Orphaned orders** (orphaned_items_rate)
       All order_items rows belonging to a sample of orders are deleted.
       The order header remains, so the order exists but has no line items.
       Simulates a partial write failure in a two-phase commit.

    6. **Invalid currency** (invalid_currency_rate)
       currency is replaced with "XXX" (not a valid ISO 4217 code).
       Simulates a missing entry in a currency lookup table.

    Note: customers_df is not modified.  No DQ issues are injected into the
    customer pool in this version of the pipeline.

    Parameters
    ----------
    orders_df : pd.DataFrame
        Clean orders output from generate_orders().
    order_items_df : pd.DataFrame
        Clean order_items output from generate_orders().
    seed : int, default 0
        Base random seed for pandas DataFrame.sample().  Each injection step
        uses seed + step_index so the six samples are independent.
        In generate_all() this is derived from (region, logical_dt) to keep
        injection deterministic across retries.

    Returns
    -------
    tuple[pd.DataFrame, pd.DataFrame, dict]
        orders_df       — modified copy of the input (original not mutated)
        order_items_df  — modified copy of the input (original not mutated)
        injection_summary — dict mapping each DQ rate key to the number of
                            records affected, e.g.:
                            {"duplicate_order_rate": 4, "null_customer_rate": 2, ...}
    """
    # Snapshot the original rows before any mutation.  All six _sample_index
    # calls draw from this fixed population, so duplication in step 1 does not
    # widen the sampling pool for steps 2–6.
    original = orders_df.copy()
    orders = orders_df.copy()
    items = order_items_df.copy()
    summary: dict[str, int] = {}

    n = len(original)

    def _sample_index(rate: float, step: int) -> pd.Index:
        """
        Return the index of a deterministic sample from the original orders.

        ``step`` is added to ``seed`` so each call produces an independent
        sample even when rates are equal.
        """
        k = max(1, round(n * rate)) if n > 0 else 0
        if k == 0:
            return pd.Index([])
        return original.sample(n=min(k, n), random_state=seed + step).index

    # Step 1: duplicates
    dup_idx = _sample_index(DQ_RATES["duplicate_order_rate"], step=0)
    orders = pd.concat([orders, orders.loc[dup_idx]], ignore_index=True)
    summary["duplicate_order_rate"] = len(dup_idx)

    # Step 2: null customer_id
    null_idx = _sample_index(DQ_RATES["null_customer_rate"], step=1)
    orders.loc[null_idx, "customer_id"] = None
    summary["null_customer_rate"] = len(null_idx)

    # Step 3: negative total_amount
    neg_idx = _sample_index(DQ_RATES["negative_revenue_rate"], step=2)
    orders.loc[neg_idx, "total_amount"] = -orders.loc[neg_idx, "total_amount"].abs()
    summary["negative_revenue_rate"] = len(neg_idx)

    # Step 4: inflate total_amount by 15% (order/item mismatch)
    mismatch_idx = _sample_index(DQ_RATES["revenue_mismatch_rate"], step=3)
    orders.loc[mismatch_idx, "total_amount"] = (
        orders.loc[mismatch_idx, "total_amount"] * 1.15
    ).round(2)
    summary["revenue_mismatch_rate"] = len(mismatch_idx)

    # Step 5: orphan orders (delete all line items for sampled orders)
    orphan_idx = _sample_index(DQ_RATES["orphaned_items_rate"], step=4)
    orphan_order_ids = set(orders.loc[orphan_idx, "order_id"].tolist())
    items = items[~items["order_id"].isin(orphan_order_ids)]
    summary["orphaned_items_rate"] = len(orphan_order_ids)

    # Step 6: invalid currency
    currency_idx = _sample_index(DQ_RATES["invalid_currency_rate"], step=5)
    orders.loc[currency_idx, "currency"] = "XXX"
    summary["invalid_currency_rate"] = len(currency_idx)

    return orders, items, summary


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------

def generate_all(region: str, logical_dt: datetime) -> dict:
    """
    Generate a complete, DQ-injected synthetic dataset for one region/hour.

    This is the single entry point used by the Airflow task.  It calls all
    individual generators in the correct order, then passes the clean data
    through inject_quality_issues() before returning.

    Seed isolation
    --------------
    Three distinct seeds are used within a single call:

    * Products seed  : 42                              (fixed — same catalogue every run)
    * Customers seed : _region_seed(region)
    * Orders seed    : _region_dt_seed(region, logical_dt)       (+0)
    * Marketing seed : _region_dt_seed(region, logical_dt) + 1   (+1)
    * Volume seed    : _region_dt_seed(region, logical_dt) + 500  (+500)
    * DQ seed        : _region_dt_seed(region, logical_dt) + 999  (+999)

    The gaps between offsets mean accidental seed collisions are extremely
    unlikely if more generators are added in future.

    Parameters
    ----------
    region : str
        Must be one of REGIONS (e.g. "us-east").
    logical_dt : datetime
        **Naive UTC datetime** for the logical hour.  The caller (Airflow task)
        is responsible for stripping tzinfo from the pendulum-aware context
        value before passing it here.

    Returns
    -------
    dict with keys:
        "products"          : pd.DataFrame
        "customers"         : pd.DataFrame
        "orders"            : pd.DataFrame  (post DQ injection)
        "order_items"       : pd.DataFrame  (post DQ injection)
        "marketing_events"  : pd.DataFrame
        "injection_summary" : dict[str, int]  — records affected per issue type
    """
    base_seed = _region_dt_seed(region, logical_dt)
    # Each derived seed uses a distinct offset to guarantee independence.
    # +500 = volume noise  |  +999 = DQ injection
    n_orders  = compute_order_volume(region, logical_dt, seed=(base_seed + 500) % (2**31))
    dq_seed   = (base_seed + 999) % (2**31)

    products = generate_products()
    customers = generate_customers(region, logical_dt)
    orders, order_items = generate_orders(region, logical_dt, products, customers, n_orders=n_orders)
    marketing_events = generate_marketing_events(region, logical_dt, customers, n_orders=n_orders)
    orders, order_items, injection_summary = inject_quality_issues(
        orders, order_items, seed=dq_seed
    )

    return {
        "products": products,
        "customers": customers,
        "orders": orders,
        "order_items": order_items,
        "marketing_events": marketing_events,
        "injection_summary": injection_summary,
    }
