"""Bar-close firing rules, ported from `alert_engine.dart` / `price_alert.dart`.

Pure functions only: no I/O, no clock, no database. The Dart tests are the
specification; `tests/test_rules.py` mirrors the load-bearing cases so a change
on either side that breaks parity shows up here.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

TICK = 0.01
PERCENT_EPSILON = 1e-9
MIN_LOOKBACK = 2
MAX_LOOKBACK = 100

# A quote is live while its own clock says <= 90s, exactly like
# `kGoldFreshWindow` in `gold_price.dart`.
QUOTE_FRESH_MS = 90_000

CROSSING = frozenset({"crosses", "crossesUp", "crossesDown", "entersRange", "exitsRange"})
RANGE = frozenset({"entersRange", "exitsRange", "insideRange", "outsideRange"})
PERCENT = "movesBy"

INTERVAL_MS = {
    "1m": 60_000,
    "5m": 300_000,
    "15m": 900_000,
    "30m": 1_800_000,
    "1h": 3_600_000,
    "4h": 14_400_000,
    "1d": 86_400_000,
}


def parse_ms(value) -> int | None:
    """ISO-8601 (the app's wire form) to epoch milliseconds, or None."""
    if not isinstance(value, str) or not value:
        return None
    text = value.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(text)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000)


def as_bool(value, default: bool = False) -> bool:
    return value if isinstance(value, bool) else default


def as_float(value) -> float | None:
    return float(value) if isinstance(value, (int, float)) and not isinstance(value, bool) else None


def as_int(value) -> int | None:
    return int(value) if isinstance(value, (int, float)) and not isinstance(value, bool) else None


def is_range(operator: str) -> bool:
    return operator in RANGE


def is_crossing(operator: str) -> bool:
    return operator in CROSSING


def lower(alert: dict) -> float:
    second = as_float(alert.get("price2"))
    first = as_float(alert.get("price")) or 0.0
    return second if second is not None and second < first else first


def upper(alert: dict) -> float:
    second = as_float(alert.get("price2"))
    first = as_float(alert.get("price")) or 0.0
    return second if second is not None and second > first else first


def is_valid(alert: dict) -> bool:
    """`PriceAlert.isValid`, as far as the relay can see a record."""
    level = as_float(alert.get("price"))
    if not alert.get("id") or level is None or level <= 0:
        return False
    operator = alert.get("operator")
    if not isinstance(operator, str):
        return False
    if is_range(operator):
        second = as_float(alert.get("price2"))
        return second is not None and second > 0 and second != level
    if operator == PERCENT:
        bars = as_int(alert.get("lookback"))
        return (
            bars is not None
            and MIN_LOOKBACK <= bars <= MAX_LOOKBACK
            and alert.get("cadence") == "barClose"
        )
    return True


def percent_move(closes: list[float], bars: int) -> float | None:
    """`percentMoveOverBars`: signed percent from `bars` back to the newest close."""
    if bars < 1 or len(closes) < bars + 1:
        return None
    last = closes[-1]
    base = closes[len(closes) - 1 - bars]
    if base <= 0:
        return None
    return (last - base) / base * 100.0


def matches(alert: dict, price: float, previous: float | None) -> bool:
    """`_matches`: the one operator test shared by both cadences."""
    operator = alert.get("operator")
    level = as_float(alert.get("price")) or 0.0
    low, high = lower(alert), upper(alert)

    if operator == "above":
        return price >= level + TICK
    if operator == "below":
        return price <= level - TICK
    if operator == "crosses":
        if previous is None:
            return False
        return (previous < level and price >= level) or (
            previous > level and price <= level
        )
    if operator == "crossesUp":
        return previous is not None and previous < level and price >= level
    if operator == "crossesDown":
        return previous is not None and previous > level and price <= level
    if operator == "insideRange":
        return low <= price <= high
    if operator == "outsideRange":
        return price < low or price > high
    if operator == "entersRange":
        return (
            previous is not None
            and (previous < low or previous > high)
            and low <= price <= high
        )
    if operator == "exitsRange":
        return (
            previous is not None
            and low <= previous <= high
            and (price < low or price > high)
        )
    # `movesBy` is decided from the series before this point (see should_fire).
    return False


def should_fire(
    alert: dict,
    closed: list[dict],
    interval: str,
    now_ms: int,
    last_fired_ms: int | None,
) -> bool:
    """`shouldFireOnClose`, judging the newest bar in `closed`.

    `closed` is ascending, forming bar already excluded. A fire is recorded at
    the bar's own close instant, so "one candle cannot ring twice" is a
    comparison of instants, exactly like the app.
    """
    if not as_bool(alert.get("armed")):
        return False
    if alert.get("cadence") != "barClose":
        return False
    expires_at = parse_ms(alert.get("expiresAt"))
    if expires_at is not None and now_ms >= expires_at:
        return False
    if not closed:
        return False

    bar = closed[-1]
    closes_at = bar["open_ms"] + INTERVAL_MS.get(interval, 900_000)

    watching_since = parse_ms(alert.get("watchingSince")) or parse_ms(alert.get("createdAt")) or 0
    if closes_at <= watching_since:
        return False

    already = max(
        [x for x in (parse_ms(alert.get("triggeredAt")), last_fired_ms) if x is not None],
        default=None,
    )
    if already is not None and already >= closes_at:
        return False

    if alert.get("operator") == PERCENT:
        bars = as_int(alert.get("lookback")) or 0
        moved = percent_move([b["close"] for b in closed], bars)
        if moved is None:
            return False
        return abs(moved) + PERCENT_EPSILON >= (as_float(alert.get("price")) or 0.0)

    previous = closed[-2]["close"] if len(closed) >= 2 else None
    return matches(alert, bar["close"], previous)


# ------------------------------------------------------- gold market calendar
#
# Ported from `market_hours.dart` / `display_zone.dart`. The week is defined in
# New York, not in the viewer's zone, and the DST rule is the same one the app
# uses for its display clock: EDT from the 2nd Sunday of March 07:00 UTC to the
# 1st Sunday of November 06:00 UTC. A live quote may only fire while a session
# is actually running, because a weekend feed that republishes Friday's close
# would otherwise look fresh and wake someone over a price that cannot move.

NEW_YORK_STANDARD_MINUTES = -5 * 60
NEW_YORK_DAYLIGHT_MINUTES = -4 * 60
GOLD_WEEKEND_CLOSE_MINUTE = 17 * 60
GOLD_WEEK_OPEN_MINUTE = 18 * 60


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _nth_sunday_utc(year: int, month: int, n: int, hour: int) -> datetime:
    """The UTC instant of the nth Sunday of `month` at `hour`:00 UTC."""
    first = datetime(year, month, 1, tzinfo=timezone.utc)
    to_sunday = (6 - first.weekday()) % 7  # weekday(): Monday=0 ... Sunday=6
    return datetime(year, month, 1 + to_sunday + (n - 1) * 7, hour, tzinfo=timezone.utc)


def new_york_offset_minutes_at(utc: datetime) -> int:
    """New York's offset east of UTC for `utc` — DST-aware."""
    u = _as_utc(utc)
    if u.year < 2007:
        return NEW_YORK_STANDARD_MINUTES
    start = _nth_sunday_utc(u.year, 3, 2, 7)  # 2nd Sunday of March, 02:00 EST
    end = _nth_sunday_utc(u.year, 11, 1, 6)  # 1st Sunday of November, 02:00 EDT
    return NEW_YORK_DAYLIGHT_MINUTES if start <= u < end else NEW_YORK_STANDARD_MINUTES


def gold_market_phase_at(utc: datetime) -> str:
    """`goldMarketPhaseAt`: 'open', 'dailyBreak' or 'weekendClosed'."""
    u = _as_utc(utc)
    new_york = u + timedelta(minutes=new_york_offset_minutes_at(u))
    minute = new_york.hour * 60 + new_york.minute
    weekday = new_york.weekday()  # Monday=0 ... Sunday=6
    if weekday == 5:  # Saturday
        return "weekendClosed"
    if weekday == 6:  # Sunday
        return "open" if minute >= GOLD_WEEK_OPEN_MINUTE else "weekendClosed"
    if weekday == 4:  # Friday
        return "open" if minute < GOLD_WEEKEND_CLOSE_MINUTE else "weekendClosed"
    if GOLD_WEEKEND_CLOSE_MINUTE <= minute < GOLD_WEEK_OPEN_MINUTE:
        return "dailyBreak"
    return "open"


def gold_market_is_open(utc: datetime) -> bool:
    """True only while a gold session is actually running."""
    return gold_market_phase_at(utc) == "open"


def should_fire_live(
    alert: dict, price: float | None, previous: float | None, now_ms: int
) -> bool:
    """`shouldFire` for a live quote, minus the quote object itself.

    The caller owns freshness (a quote older than [QUOTE_FRESH_MS] must not be
    used); this function owns the market calendar, the arm/expiry state and the
    operator test, so a closed market or a paused alert stays silent even when a
    feed hands us a plausible number. A crossing with no previous sample is
    silent: the first pass only establishes the baseline, exactly like the app.
    """
    if not as_bool(alert.get("armed")):
        return False
    expires_at = parse_ms(alert.get("expiresAt"))
    if expires_at is not None and now_ms >= expires_at:
        return False
    # A percent move is a property of a series, not of one quote.
    if alert.get("operator") == PERCENT:
        return False
    if price is None:
        return False
    if not gold_market_is_open(datetime.fromtimestamp(now_ms / 1000, tz=timezone.utc)):
        return False
    return matches(alert, price, previous)
