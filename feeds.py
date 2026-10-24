"""Market data: closed candles from the same feed the app uses."""

from __future__ import annotations

import json
import urllib.error
import urllib.request

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


def fetch_quote(symbol: str = "XAUUSD", timeout: float = 15.0) -> dict | None:
    """Live quote, used only by `/health` so the status page can show a price."""
    request = urllib.request.Request(
        f"{BASE}/{symbol}", headers={"User-Agent": USER_AGENT}
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            payload = json.load(response)
    except Exception:
        return None
    if not isinstance(payload, dict):
        return None
    price = payload.get("price") or payload.get("close")
    if not isinstance(price, (int, float)):
        return None
    return {"price": float(price), "at": payload.get("time") or payload.get("timestamp")}
