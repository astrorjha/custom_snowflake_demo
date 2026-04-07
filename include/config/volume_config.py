"""
include/config/volume_config.py
================================
Volume and seasonality configuration for the GalaxyCommerce synthetic data generator.

This module owns everything that determines *how many* orders are generated for a
given region and hour.  Data-quality injection rates live separately in dq_config.py.

How the volume model works
--------------------------
The number of orders for a region/hour is computed by multiplying a base rate through
a chain of independent multipliers, then adding a small seeded noise term:

    volume = BASE_ORDERS_PER_REGION_PER_HOUR
             × INTRADAY_MULTIPLIERS[local_hour]
             × DOW_MULTIPLIERS[local_weekday]
             × MONTHLY_MULTIPLIERS[month]
             × holiday_multiplier(region, local_date)
             × uniform_noise(±VOLUME_NOISE_PCT%)

Each multiplier is applied to local time for the region (using REGION_UTC_OFFSETS),
so the evening peak, weekends, and holidays all fire at the right moment for each
geography rather than all synchronising on UTC.

Typical volume ranges (with base = 200, before noise)
------------------------------------------------------
Quiet overnight weekday (local 03:00, Tuesday, March):
    200 × 0.04 × 0.90 × 0.95  ≈  7 orders/hour

Average (mean across all hours/days):
    ~143 orders/hour/region

Saturday evening peak (local 19:00, Saturday, November):
    200 × 1.65 × 1.20 × 1.35  ≈  535 orders/hour

Black Friday evening peak (us-east or us-west):
    200 × 1.65 × 1.20 × 1.35 × 4.0  ≈  2,138 orders/hour

Holiday calendar notes
----------------------
*   Black Friday and Cyber Monday are computed as floating dates (day after the
    4th Thursday of November, and the following Monday) so they track the real
    calendar year to year.
*   Golden Week (Japan) is always 29 Apr – 5 May; volume drops because physical
    travel competes with online shopping.
*   Click Frenzy AU (mid-November) and the Rakuten Super Sale JP (mid-June) use
    approximate fixed dates; the exact dates vary by year but the pattern is
    consistent enough for a demo.
*   Christmas Day and New Year's Day have sub-1.0 multipliers — most fulfilment
    centres are closed or reduced, and shoppers are offline.
*   DST is intentionally ignored.  Using fixed UTC offsets keeps the code simple
    and produces a convincing-enough intraday pattern for demo purposes.
"""

from __future__ import annotations

import calendar
from datetime import date, timedelta

# ---------------------------------------------------------------------------
# Base rate
# ---------------------------------------------------------------------------

# Reference order volume per region per hour, representing a typical mid-evening
# peak.  The intraday multiplier scales this down to near-zero overnight and up
# to ~1.65× at the actual evening peak — see INTRADAY_MULTIPLIERS below.
BASE_ORDERS_PER_REGION_PER_HOUR = 200

# ---------------------------------------------------------------------------
# Regional time offsets
# ---------------------------------------------------------------------------

# Fixed UTC offsets (hours).  DST is intentionally ignored for simplicity.
# The intraday pattern is anchored to local time for each region, so the peak
# hour fires at a plausible local-time equivalent without requiring pytz/zoneinfo.
REGION_UTC_OFFSETS: dict[str, int] = {
    "us-east":    -5,   # EST (UTC-5)
    "us-west":    -8,   # PST (UTC-8)
    "eu-west":     0,   # GMT (UTC+0)
    "eu-central":  1,   # CET (UTC+1)
    "apac-au":    10,   # AEST (UTC+10)
    "apac-jp":     9,   # JST  (UTC+9, no DST)
}

# ---------------------------------------------------------------------------
# Intraday multipliers (indexed by local hour 0–23)
# ---------------------------------------------------------------------------

# Shape based on typical e-commerce patterns:
#   • Deep trough 02–05 (overnight, most shoppers asleep)
#   • Gentle morning ramp from 06 (phone check on wake-up, commute browsing)
#   • Lunch-hour bump at 12
#   • Afternoon dip 14–15
#   • Evening peak 18–21 (home from work, couch shopping)
#   • Late-night tail 22–23
#
# Values are relative to BASE_ORDERS_PER_REGION_PER_HOUR.
# The unweighted mean across 24 hours is ≈ 0.71, so average throughput is
# roughly 142 orders/hour/region on a normal weekday in a mid-year month.
INTRADAY_MULTIPLIERS: list[float] = [
    #  00    01    02    03    04    05
    0.10, 0.07, 0.05, 0.04, 0.04, 0.06,
    #  06    07    08    09    10    11
    0.15, 0.30, 0.55, 0.70, 0.80, 0.90,
    #  12    13    14    15    16    17
    1.05, 1.00, 0.85, 0.85, 0.95, 1.15,
    #  18    19    20    21    22    23
    1.45, 1.65, 1.55, 1.35, 0.95, 0.55,
]

# ---------------------------------------------------------------------------
# Day-of-week multipliers (0 = Monday … 6 = Sunday)
# ---------------------------------------------------------------------------

# Weekends are higher across all regions; Friday uplift reflects both
# payday-adjacent behaviour and pre-weekend impulse buying.
DOW_MULTIPLIERS: dict[int, float] = {
    0: 0.85,   # Monday   — post-weekend slump
    1: 0.90,   # Tuesday
    2: 0.95,   # Wednesday
    3: 0.95,   # Thursday
    4: 1.05,   # Friday   — payday / pre-weekend
    5: 1.20,   # Saturday — highest consumer activity
    6: 1.10,   # Sunday   — high but slightly lower than Saturday
}

# ---------------------------------------------------------------------------
# Monthly seasonality multipliers
# ---------------------------------------------------------------------------

# November and December are peak due to Black Friday, Cyber Monday, and gift
# buying.  January is the deepest trough (post-holiday spend hangover).
# Summer (Jun–Jul) dips slightly for the electronics/tech category mix
# GalaxyCommerce carries.
MONTHLY_MULTIPLIERS: dict[int, float] = {
    1:  0.80,   # January   — post-holiday slump
    2:  0.85,   # February  — gradual recovery
    3:  0.95,   # March
    4:  0.95,   # April
    5:  1.00,   # May       — steady baseline
    6:  0.90,   # June      — pre-summer electronics lull
    7:  0.85,   # July      — summer trough
    8:  0.90,   # August    — back-to-school uptick
    9:  1.00,   # September — return to baseline
    10: 1.05,   # October   — early holiday browsing
    11: 1.35,   # November  — Black Friday / Cyber Monday month
    12: 1.40,   # December  — peak holiday shopping
}

# ---------------------------------------------------------------------------
# Per-hour noise band
# ---------------------------------------------------------------------------

# Uniform noise applied independently to every region/hour so that consecutive
# hours and consecutive weeks of the same hour don't produce identical counts.
# The noise is seeded (via compute_order_volume's seed argument) so the same
# hour always generates the same noisy count on retry.
VOLUME_NOISE_PCT: int = 15   # ±15%

# ---------------------------------------------------------------------------
# Holiday calendar
# ---------------------------------------------------------------------------

def _nth_weekday(year: int, month: int, weekday: int, n: int) -> date:
    """
    Return the nth occurrence of ``weekday`` in the given month/year.

    Parameters
    ----------
    weekday : int
        0 = Monday … 6 = Sunday (Python calendar convention).
    n : int
        1-based occurrence index (1 = first, 4 = fourth).
    """
    first = date(year, month, 1)
    delta = (weekday - first.weekday()) % 7
    return first + timedelta(days=delta + (n - 1) * 7)


def _last_weekday(year: int, month: int, weekday: int) -> date:
    """Return the last occurrence of ``weekday`` in the given month/year."""
    last_day = date(year, month, calendar.monthrange(year, month)[1])
    delta = (last_day.weekday() - weekday) % 7
    return last_day - timedelta(days=delta)


def regional_holidays(year: int) -> dict[str, dict[date, float]]:
    """
    Build the holiday volume-multiplier calendar for all regions for ``year``.

    Multipliers > 1.0 indicate a demand spike (e.g. Black Friday = 4×).
    Multipliers < 1.0 indicate suppressed demand (e.g. Christmas Day = 0.3×
    because most fulfilment centres operate at reduced capacity and consumers
    are offline with family).

    Floating holidays (Black Friday, Cyber Monday, Memorial Day, Labor Day,
    Golden Week) are computed from calendar rules so they track the real date
    year to year.  Fixed-date approximations are used for events whose exact
    dates vary commercially (Click Frenzy, Rakuten Super Sale).

    Parameters
    ----------
    year : int
        Calendar year to compute holidays for.

    Returns
    -------
    dict[str, dict[date, float]]
        { region_name: { calendar_date: volume_multiplier } }
    """
    # ------------------------------------------------------------------
    # Floating dates
    # ------------------------------------------------------------------
    # US Thanksgiving: 4th Thursday of November
    thanksgiving  = _nth_weekday(year, 11, 3, 4)
    black_friday  = thanksgiving + timedelta(days=1)
    cyber_monday  = thanksgiving + timedelta(days=4)   # Mon after Thanksgiving

    # US long weekends
    memorial_day  = _last_weekday(year, 5, 0)          # Last Monday of May
    labor_day     = _nth_weekday(year, 9, 0, 1)        # 1st Monday of September

    # Japan Golden Week: 29 Apr – 5 May (fixed window)
    golden_week_start = date(year, 4, 29)
    golden_week_days  = [golden_week_start + timedelta(days=i) for i in range(7)]

    # ------------------------------------------------------------------
    # Fixed-date holidays
    # ------------------------------------------------------------------
    xmas_eve    = date(year, 12, 24)
    xmas_day    = date(year, 12, 25)
    boxing_day  = date(year, 12, 26)
    nye         = date(year, 12, 31)
    new_year    = date(year, 1, 1)

    # Asia-Pacific events (approximate commercial dates)
    singles_day       = date(year, 11, 11)   # JP/global: biggest online sale
    click_frenzy_au   = date(year, 11, 13)   # AU: mid-Nov, date varies year to year
    rakuten_super_jp  = date(year, 6, 15)    # JP: approximate Super Sale start

    # ------------------------------------------------------------------
    # Per-region calendars
    # ------------------------------------------------------------------

    # US regions share an identical holiday calendar.
    us: dict[date, float] = {
        black_friday:  4.0,   # biggest online shopping day of the year
        cyber_monday:  3.0,   # online-only complement to Black Friday
        thanksgiving:  1.5,   # some "pre-Black Friday" browsing before dinner
        memorial_day:  1.3,   # long weekend = browse time
        labor_day:     1.2,
        xmas_eve:      2.0,   # last-minute gift purchases
        xmas_day:      0.3,   # most offline with family
        nye:           1.5,   # "New Year, New Gadget" impulse buying
        new_year:      0.6,   # slow start to January
    }

    # Western Europe (UK, Ireland, Benelux, Nordics).
    # Black Friday has been adopted strongly; Boxing Day is a major sale day in UK/IE.
    eu_west: dict[date, float] = {
        black_friday:  2.5,
        cyber_monday:  2.0,
        xmas_eve:      2.0,
        xmas_day:      0.3,
        boxing_day:    2.5,   # major UK & Irish retail sale day
        nye:           1.3,
        new_year:      0.5,
    }

    # Central Europe (Germany, France, Netherlands, etc.).
    # Black Friday adoption is strong; no Boxing Day tradition.
    eu_central: dict[date, float] = {
        black_friday:  2.2,
        cyber_monday:  1.8,
        xmas_eve:      2.2,   # gift buying peaks on 24 Dec in DE/FR culture
        xmas_day:      0.3,
        nye:           1.3,
        new_year:      0.5,
    }

    # Australia.
    # Boxing Day is the single biggest retail event of the year in AU.
    # Click Frenzy is a home-grown online sale event in mid-November.
    apac_au: dict[date, float] = {
        black_friday:     2.0,
        cyber_monday:     1.8,
        click_frenzy_au:  2.5,
        xmas_eve:         1.8,
        xmas_day:         0.2,   # public holiday, most retail closed
        boxing_day:       3.5,   # largest single shopping day in AU
        nye:              1.2,
        new_year:         0.5,
    }

    # Japan.
    # Singles Day (11/11) is the dominant spike.  Golden Week is a national
    # holiday period — consumers travel rather than shop online, so volume drops.
    # Christmas is not a public holiday but gift-giving (particularly couples)
    # drives a modest uptick.  Oshōgatsu (New Year) is a major offline holiday;
    # online shopping almost stops.
    apac_jp: dict[date, float] = {
        singles_day:      3.0,
        black_friday:     1.5,   # growing in JP but smaller than West
        rakuten_super_jp: 2.0,   # approximate Rakuten Super Sale
        xmas_eve:         1.5,   # couples' gift-giving night
        xmas_day:         0.8,   # not a public holiday; light but present
        nye:              0.4,   # families preparing for Oshōgatsu, offline
        new_year:         0.3,   # Oshōgatsu — shops and fulfilment mostly closed
        **{d: 0.5 + 0.1 * i for i, d in enumerate(golden_week_days)},
        # Golden Week: ramp from 0.5 (Apr 29) → ~0.8 (May 5) as the holiday
        # winds down and people start browsing again before returning to work.
    }

    return {
        "us-east":    us,
        "us-west":    us,
        "eu-west":    eu_west,
        "eu-central": eu_central,
        "apac-au":    apac_au,
        "apac-jp":    apac_jp,
    }
