"""The evaluator tick: judge every tenant's armed alerts and fire when they ring.

Two cadences, one loop. Bar-close alerts read finished candles once per interval
for the whole relay (the market is shared). Live alerts read one quote per tick
and are judged against the previous sample, so a crossing that happens while the
app is off screen still produces a fire the phone can post. A fire is written
into the tenant that owns the alert, so it only ever wakes that tenant's phone.
"""

from __future__ import annotations

import time
from datetime import datetime, timezone

import feeds
import rules
import wire

# The app also evaluates live alerts while it is on screen and pushes its fire
# within a few seconds. This window is shorter than one tick, so it absorbs that
# race without swallowing a genuinely later crossing.
LIVE_DEDUPE_MS = 15_000


def _now_ms() -> int:
    return int(time.time() * 1000)


def _utc(ms: int) -> datetime:
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc)


def _watching_ms(alert: dict) -> int:
    return (
        rules.parse_ms(alert.get("watchingSince"))
        or rules.parse_ms(alert.get("createdAt"))
        or 0
    )


def _iso(ms: int) -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(ms / 1000))


def _iso_ms(ms: int) -> str:
    """ISO-8601 at millisecond precision.

    The app derives a fire's identity as `f<alertId>@<firedAt_ms>`, so a live
    fire must keep its milliseconds: `_iso` would round the instant to the
    second and the app would compute a different id for the same record.
    """
    return time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(ms / 1000)) + (
        f".{ms % 1000:03d}Z"
    )


def tick(app) -> None:
    cfg = app.cfg
    now = _now_ms()

    live: list[tuple[str, dict]] = []
    groups: dict[str, list[tuple[str, dict]]] = {}
    for tenant in app.store.tenants_with_alerts():
        for alert in app.store.alerts(tenant):
            alert_id = alert.get("id")
            if not isinstance(alert_id, str) or not alert_id:
                continue
            watching = _watching_ms(alert)
            app.store.remember_alert(tenant, alert_id, bool(alert.get("armed")), watching)
            if not alert.get("armed"):
                continue
            if not rules.is_valid(alert):
                continue
            if alert.get("cadence") == "barClose":
                interval = alert.get("interval") or "15m"
                groups.setdefault(interval, []).append((tenant, alert))
            else:
                live.append((tenant, alert))

    if live:
        _tick_live(app, live, now)

    for interval, items in groups.items():
        try:
            bars = feeds.fetch_closed_bars(
                cfg["symbol"], interval, int(cfg.get("bars_limit", 120))
            )
        except feeds.FeedError as error:
            app.note_error(str(error))
            continue
        if not bars:
            continue

        cursor = app.store.cursor(interval)
        for bar in [b for b in bars if b["open_ms"] > cursor]:
            window = [b for b in bars if b["open_ms"] <= bar["open_ms"]]
            closes_at = bar["open_ms"] + rules.INTERVAL_MS.get(interval, 900_000)
            for tenant, alert in items:
                alert_id = alert["id"]
                state = app.store.alert_state(tenant, alert_id)
                # A threshold or posture alert rings once per condition; only a
                # crossing is allowed to ring again on the next bar. Re-arming or
                # editing the condition clears the memory in remember_alert().
                if not rules.is_crossing(alert.get("operator")):
                    if state["last_fired_ms"] is not None:
                        continue
                if not rules.should_fire(
                    alert, window, interval, now, state["last_fired_ms"]
                ):
                    continue

                fire_id = f"f{alert_id}@{closes_at}"
                if not app.store.exists(tenant, "fires", fire_id):
                    # The Arabic line is computed once, here, and travels with
                    # the record: the Android service has no rule engine and no
                    # localisation, so a notification must be ready to display.
                    message = wire.describe(alert, bar["close"])
                    app.store.put(
                        tenant,
                        "fires",
                        {
                            "id": fire_id,
                            "_u": now,
                            "_d": False,
                            "alertId": alert_id,
                            "barOpenMs": bar["open_ms"],
                            "price": bar["close"],
                            "firedAt": _iso(closes_at),
                            "source": "relay",
                            "title": "fxx",
                            "message": message,
                        },
                        "relay",
                    )
                    _push(app, tenant, alert, bar["close"], fire_id)
                app.store.mark_fired(tenant, alert_id, closes_at)

        app.store.set_cursor(interval, bars[-1]["open_ms"])

    app.last_tick_ms = _now_ms()


def _tick_live(app, items: list[tuple[str, dict]], now: int) -> None:
    """One quote, every live alert.

    The market calendar is consulted before the feed is even asked, so a weekend
    or the daily settlement hour costs no request and can never fire. A quote
    whose own clock is older than [rules.QUOTE_FRESH_MS] is skipped: a stale
    number must never wake anyone.
    """
    if not rules.gold_market_is_open(_utc(now)):
        return
    try:
        quote = feeds.fetch_quote(app.cfg["symbol"])
    except feeds.FeedError as error:
        app.note_error(str(error))
        return
    if quote is None:
        return
    quote_at = quote.get("at_ms")
    if quote_at is not None and now - quote_at > rules.QUOTE_FRESH_MS:
        return

    price = quote["price"]
    for tenant, alert in items:
        alert_id = alert["id"]
        state = app.store.alert_state(tenant, alert_id)
        if rules.should_fire_live(alert, price, state.get("last_price"), now):
            # A threshold/posture alert rings once per condition: the app disarms
            # it, and until that disarm syncs this memory stands in for it. A
            # crossing stays armed and is allowed to ring again.
            already = state["last_fired_ms"] is not None
            if rules.is_crossing(alert.get("operator")) or not already:
                if not _recent_fire(app, tenant, alert_id, now):
                    _write_live_fire(app, tenant, alert, price, now)
                    app.store.mark_fired_live(tenant, alert_id, now)
        # Every live alert sees this price, including one that just fired: a
        # crossing stays armed and would otherwise re-fire against the same
        # sample on the next tick.
        app.store.set_last_price(tenant, alert_id, price)


def _write_live_fire(app, tenant: str, alert: dict, price: float, now: int) -> str:
    """Write the fire the phone will post, with the whole condition on it.

    `level`, `level2`, `lookback` and `operator` are part of the record because
    the app rebuilds the log row from them (`AlertFire.decode`); a fire that
    carries only a price cannot be adopted into the trader's fire log.
    """
    alert_id = alert["id"]
    fire_id = f"f{alert_id}@{now}"
    if app.store.exists(tenant, "fires", fire_id):
        return fire_id
    record = {
        "id": fire_id,
        "_u": now,
        "_d": False,
        "alertId": alert_id,
        "level": alert.get("price"),
        "price": price,
        "operator": alert.get("operator"),
        "firedAt": _iso_ms(now),
        "source": "relay",
        "title": "fxx",
        "message": wire.describe(alert, price),
    }
    if alert.get("price2") is not None:
        record["level2"] = alert.get("price2")
    if alert.get("lookback") is not None:
        record["lookback"] = alert.get("lookback")
    app.store.put(tenant, "fires", record, "relay")
    _push(app, tenant, alert, price, fire_id)
    return fire_id


def _recent_fire(app, tenant: str, alert_id: str, now: int) -> bool:
    """Whether this alert already produced a fire moments ago.

    The app evaluates live alerts in the foreground and pushes the fire within a
    few seconds; without this the relay would write the same crossing a second
    time and the log would show it twice. The window is shorter than one tick,
    so a genuinely later crossing still rings.
    """
    for record in app.store.fires_since(tenant, now - LIVE_DEDUPE_MS, 20):
        if record.get("alertId") == alert_id and not record.get("_d"):
            return True
    return False


def _push(app, tenant: str, alert: dict, price: float, fire_id: str) -> None:
    """Optional second channel.

    ntfy was how a device rediscovered a changing tunnel address; with a fixed
    hostname baked into the app it is dead weight, and publishing every tenant's
    fires to a public topic would leak them. Off unless an operator asks for it.
    """
    cfg = app.cfg
    if not cfg.get("ntfy_enabled"):
        return
    tunnel = app.tunnel_url()
    click = f"{tunnel}/a/{fire_id}" if tunnel else None
    wire.publish(
        cfg["ntfy_base"],
        f"{cfg['ntfy_topic']}-{tenant}",
        f"fxx - {cfg['symbol']}",
        wire.describe(alert, price),
        click=click,
        priority=5,
        tags=["rotating_light"],
    )
