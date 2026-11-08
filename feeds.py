"""Market data: closed candles from the same feed the app uses."""

from __future__ import annotations

import json
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone

from rules import parse_ms

BASE = "https://biquote.io/api"
USER_AGENT = "fxx-relay/1.0 (+android-termux)"


class FeedError(RuntimeError):
    pass


def fetch_closed_bars(
    symbol: str, interval: str, limit: int = 120, timeout: float = 20.0
) -> list[dict]:
    """Closed bars, ascending by open time.

    The provider marks the forming candle with `isOpen`; it is dropped here so no
    caller can ever judge an unsettled candle. The newest bar may carry an
    openTime slightly ahead of our clock, which is why the cursor - never
    `now()` - decides what is new.
    """
    url = f"{BASE}/{symbol}/ohlc?interval={interval}&limit={limit}"
    request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            payload = json.load(response)
    except (urllib.error.URLError, TimeoutError, ValueError) as error:
        raise FeedError(f"{interval}: {error}") from error

    bars = payload.get("bars") if isinstance(payload, dict) else None
    if not isinstance(bars, list):
        raise FeedError(f"{interval}: unexpected payload")

    out: list[dict] = []
    for bar in bars:
        if not isinstance(bar, dict) or bar.get("isOpen"):
            continue
        open_ms = parse_ms(bar.get("openTime"))
        close = bar.get("close")
        if open_ms is None or not isinstance(close, (int, float)):
            continue
        out.append(
            {
                "open_ms": open_ms,
                "open": float(bar.get("open") or close),
                "high": float(bar.get("high") or close),
                "low": float(bar.get("low") or close),
                "close": float(close),
            }
        )
    out.sort(key=lambda b: b["open_ms"])
    return out


def _first_positive(payload: dict, keys) -> float | None:
    for key in keys:
        value = payload.get(key)
        if isinstance(value, (int, float)) and not isinstance(value, bool) and value > 0:
            return float(value)
    return None


def _declares_closed(payload: dict) -> bool:
    for key in ("marketState", "market_state"):
        value = payload.get(key)
        if isinstance(value, str) and value.lower().startswith("clos"):
            return True
    return False


def _dotted_time_ms(value) -> int | None:
    """biquote's `time` is "YYYY.MM.DD HH:MM:SS" (UTC), not ISO-8601."""
    if not isinstance(value, str):
        return None
    try:
        return int(
            datetime.strptime(value.strip(), "%Y.%m.%d %H:%M:%S")
            .replace(tzinfo=timezone.utc)
            .timestamp()
            * 1000
        )
    except ValueError:
        return None


def fetch_quote(symbol: str = "XAUUSD", timeout: float = 15.0) -> dict | None:
    """One live quote, parsed the way the app parses biquote.

    Returns `{"price", "at_ms"}`. `at_ms` is the provider's own clock when the
    payload carries one, else None — the caller then uses its fetch instant,
    exactly like `GoldQuote.providerTime` defaults to `now`.

    None means "the payload cannot be used" (the feed says it is stale, or
    carries no positive mid); a network or JSON failure raises [FeedError], so a
    dead feed still surfaces on `/health` instead of looking like a quiet market.
    """
    request = urllib.request.Request(
        f"{BASE}/{symbol}", headers={"User-Agent": USER_AGENT}
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            payload = json.load(response)
    except (urllib.error.URLError, TimeoutError, ValueError) as error:
        raise FeedError(f"quote: {error}") from error

    if not isinstance(payload, dict):
        raise FeedError("quote: unexpected payload")

    # `stale` means a dead feed mid-session; a payload that declares the market
    # shut is the session's last close, which the calendar already refuses to
    # treat as live, so it is kept rather than dropped here.
    market_closed = _declares_closed(payload)
    if not market_closed:
        for key in ("stale", "isStale", "is_stale"):
            if payload.get(key) is True:
                return None

    price = _first_positive(payload, ("mid", "price", "midPrice", "mid_price", "last"))
    if price is None:
        return None

    at_ms = None
    age = payload.get("quoteAgeSeconds")
    if (
        isinstance(age, (int, float))
        and not isinstance(age, bool)
        and 0 <= age < 3600
    ):
        at_ms = int(time.time() * 1000 - float(age) * 1000)
    else:
        for key in ("lastQuoteAt", "last_quote_at", "timestamp", "updatedAt"):
            at_ms = parse_ms(payload.get(key))
            if at_ms is not None:
                break
        if at_ms is None:
            at_ms = _dotted_time_ms(payload.get("time"))

    return {"price": price, "at_ms": at_ms}
