"""Bar-close firing rules, ported from `alert_engine.dart` / `price_alert.dart`.

Pure functions only: no I/O, no clock, no database. The Dart tests are the
specification; `tests/test_rules.py` mirrors the load-bearing cases so a change
on either side that breaks parity shows up here.
"""

from __future__ import annotations

from datetime import datetime, timezone

TICK = 0.01
PERCENT_EPSILON = 1e-9
MIN_LOOKBACK = 2
MAX_LOOKBACK = 100

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
