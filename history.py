"""OANDA v20 history: the one second source, and only for *older* bars.

Why this exists (D-20261108-001, TQ-052): the live feed stores a fixed window
per cadence (15m holds ~48h) and ignores `limit` above it, so a chart that wants
more history has to ask someone else. The someone else is OANDA's spot
`XAU_USD`, fetched here rather than in the app so the token never ships in an
APK.

Three rules keep this honest:

* **History only.** This module is never asked for the newest bar and never
  feeds a quote or an alert. The live feed still owns the recent series.
* **Closed bars only.** OANDA marks a forming candle `complete: false`; it is
  dropped, exactly like the live feed's `isOpen` bars.
* **Bars are shaped like the live feed's.** The app parses both with one
  parser, so `openTime`/OHLC/`tickVolume`/`isOpen` mean the same thing here as
  they do in an `.../ohlc` payload. OANDA's `volume` is a count of prices, not
  consolidated volume, so it lands in `tickVolume` and `volume` stays 0.

The source is optional. Without `OANDA_TOKEN` in the environment every entry
point refuses; the app then shows the history it already had.
"""

from __future__ import annotations

import json
import os
import re
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from threading import Lock

SOURCE = "oanda"

HOSTS = {
    "practice": "https://api-fxpractice.oanda.com",
    "live": "https://api-fxtrade.oanda.com",
}

# The app's symbol -> OANDA's instrument name. Only gold: this app is gold-only,
# and guessing another instrument's name would produce a wrong series rather
# than a missing one.
INSTRUMENTS = {"XAUUSD": "XAU_USD"}

# The app's cadence -> OANDA granularity, with the bar length in ms so a window
# can be expressed as from/to (OANDA forbids `count` together with both).
GRANULARITY = {
    "1m": ("M1", 60_000),
    "5m": ("M5", 300_000),
    "15m": ("M15", 900_000),
    "1h": ("H1", 3_600_000),
    "4h": ("H4", 14_400_000),
    "1d": ("D", 86_400_000),
}

# One page is bounded on purpose: 1000 bars is ~10 days of 15m and ~120 KB of
# JSON, which is a chart open, not a stream.
MAX_COUNT = 2000
DEFAULT_COUNT = 500

# Closed history is immutable, so a short cache costs nothing and stops a
# pan-triggered refetch from spending OANDA's rate budget.
CACHE_TTL = 300.0
CACHE_MAX = 32

_TIMEOUT = 20.0
_USER_AGENT = "fxx-relay/1.0 (+history)"
_FRACTION = re.compile(r"\.(\d+)")

_CACHE: dict[tuple, tuple[float, list[dict]]] = {}
_LOCK = Lock()


class HistoryError(RuntimeError):
    """The upstream source failed; the caller should answer 502."""


def configured() -> bool:
    return bool(str(os.environ.get("OANDA_TOKEN") or "").strip())


def environment() -> str:
    value = str(os.environ.get("OANDA_ENV") or "practice").strip().lower()
    return value if value in HOSTS else "practice"


def instrument_for(symbol: str) -> str | None:
    return INSTRUMENTS.get(symbol.upper())


def _iso(ms: int) -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(ms / 1000))


def _ms(value) -> int | None:
    """OANDA's RFC3339 (nanosecond fractions) to epoch ms."""
    if not isinstance(value, str) or not value:
        return None
    text = value.strip()
    # `datetime.fromisoformat` takes at most microseconds; OANDA sends nine
    # fractional digits, so the fraction is cut before parsing.
    match = _FRACTION.search(text)
    if match and len(match.group(1)) > 6:
        text = text[: match.start(1)] + match.group(1)[:6] + text[match.end(1) :]
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return int(parsed.timestamp() * 1000)


def _get_json(url: str, headers: dict[str, str]) -> dict:
    """One request. Patched in tests, so no test reaches OANDA."""
    request = urllib.request.Request(url, headers=headers)
    with urllib.request.urlopen(request, timeout=_TIMEOUT) as response:
        payload = json.load(response)
    if not isinstance(payload, dict):
        raise HistoryError("unexpected payload")
    return payload


def _number(value) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if number > 0 else None


def fetch(
    symbol: str,
    interval: str,
    before_ms: int | None = None,
    count: int = DEFAULT_COUNT,
) -> list[dict]:
    """Closed bars older than `before_ms`, oldest first, in the app's shape.

    `before_ms` is exclusive: the app passes the open time of its oldest live
    bar, so the two series meet without overlapping. Raises ValueError for a
    caller mistake (unknown symbol/cadence) and HistoryError for an upstream
    failure; it never returns a partial series.
    """
    instrument = instrument_for(symbol)
    if instrument is None:
        raise ValueError(f"unsupported symbol: {symbol}")
    granularity, bar_ms = GRANULARITY.get(interval, (None, 0))
    if granularity is None:
        raise ValueError(f"unsupported interval: {interval}")

    count = max(1, min(int(count), MAX_COUNT))
    before = int(before_ms if before_ms is not None else time.time() * 1000)
    start = before - count * bar_ms

    cache_key = (instrument, granularity, start, before, count)
    now = time.monotonic()
    with _LOCK:
        hit = _CACHE.get(cache_key)
        if hit is not None and hit[0] > now:
            return [dict(bar) for bar in hit[1]]

    token = str(os.environ.get("OANDA_TOKEN") or "").strip()
    if not token:
        raise HistoryError("history is not configured")

    query = urllib.parse.urlencode(
        {
            "granularity": granularity,
            "price": "M",
            "from": _iso(start),
            "to": _iso(before),
        }
    )
    url = f"{HOSTS[environment()]}/v3/instruments/{instrument}/candles?{query}"
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept-Datetime-Format": "RFC3339",
        "User-Agent": _USER_AGENT,
    }

    try:
        payload = _get_json(url, headers)
    except urllib.error.HTTPError as error:
        raise HistoryError(f"oanda {error.code}") from error
    except (urllib.error.URLError, TimeoutError, OSError, ValueError) as error:
        raise HistoryError(f"oanda: {error}") from error

    candles = payload.get("candles")
    if not isinstance(candles, list):
        raise HistoryError("oanda: unexpected payload")

    by_time: dict[int, dict] = {}
    for candle in candles:
        if not isinstance(candle, dict) or candle.get("complete") is not True:
            continue
        open_ms = _ms(candle.get("time"))
        mid = candle.get("mid")
        if open_ms is None or not isinstance(mid, dict):
            continue
        # Exclusive on both ends: nothing at or past the seam, nothing before
        # the window that was asked for.
        if open_ms >= before or open_ms < start:
            continue
        open_ = _number(mid.get("o"))
        high = _number(mid.get("h"))
        low = _number(mid.get("l"))
        close = _number(mid.get("c"))
        if None in (open_, high, low, close) or high < low:
            continue
        tick = candle.get("volume")
        by_time[open_ms] = {
            "openTime": _iso(open_ms),
            "open": open_,
            "high": high,
            "low": low,
            "close": close,
            "volume": 0,
            "tickVolume": tick if isinstance(tick, (int, float)) else None,
            "isOpen": False,
        }

    bars = [by_time[key] for key in sorted(by_time)]
    with _LOCK:
        if len(_CACHE) >= CACHE_MAX:
            for stale in [k for k, v in _CACHE.items() if v[0] <= now]:
                _CACHE.pop(stale, None)
            if len(_CACHE) >= CACHE_MAX:
                _CACHE.clear()
        _CACHE[cache_key] = (now + CACHE_TTL, bars)
    return [dict(bar) for bar in bars]
