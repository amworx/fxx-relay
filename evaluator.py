"""The evaluator tick: read closed bars, judge every tenant's armed alerts, fire once per bar.

Bars are fetched once per interval for the whole relay - the market is shared -
and then applied to each tenant's alerts. A fire is written into the tenant that
owns the alert, so it only ever wakes that tenant's phone.
"""

from __future__ import annotations

import time

import feeds
import rules
import wire


def _now_ms() -> int:
    return int(time.time() * 1000)


def _watching_ms(alert: dict) -> int:
    return (
        rules.parse_ms(alert.get("watchingSince"))
        or rules.parse_ms(alert.get("createdAt"))
        or 0
    )


def _iso(ms: int) -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(ms / 1000))


def tick(app) -> None:
    cfg = app.cfg
    now = _now_ms()

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
            if alert.get("cadence") != "barClose":
                continue  # live cadence needs a quote per tick and stays app-only
            if not rules.is_valid(alert):
                continue
            interval = alert.get("interval") or "15m"
            groups.setdefault(interval, []).append((tenant, alert))

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

                fire_id = f"{alert_id}@{closes_at}"
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
