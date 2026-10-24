"""Outbound push through ntfy.

JSON publish (`POST /` with a body) rather than headers: the message is Arabic,
and a header would need RFC 2047 encoding for every title. The JSON form takes
UTF-8 as-is, and `click` carries the `fxx://` deep link.
"""

from __future__ import annotations

import json
import urllib.error
import urllib.request

USER_AGENT = "fxx-relay/1.0 (+android-termux)"

LABELS = {
    "above": "تجاوز المستوى من أعلى",
    "below": "تجاوز المستوى من أدنى",
    "crosses": "اخترق المستوى",
    "crossesUp": "اخترق المستوى صعوداً",
    "crossesDown": "اخترق المستوى هبوطاً",
    "insideRange": "دخل النطاق",
    "outsideRange": "خرج عن النطاق",
    "entersRange": "دخل النطاق",
    "exitsRange": "خرج من النطاق",
    "movesBy": "تحرّك بنسبة",
}


def format_percent(value: float) -> str:
    """Up to 4 decimals, trailing zeros trimmed - `formatAlertPercent` in Dart."""
    text = f"{value:.4f}".rstrip("0").rstrip(".")
    return text or "0"


def describe(alert: dict, price: float) -> str:
    operator = alert.get("operator")
    label = LABELS.get(operator, operator or "تنبيه")
    if operator == "movesBy":
        return f"{label} {format_percent(float(alert.get('price') or 0))}% خلال {alert.get('lookback')} شمعات — {price:.2f}"
    return f"{label} {float(alert.get('price') or 0):.2f} — {price:.2f}"


def publish(
    base: str,
    topic: str,
    title: str,
    message: str,
    click: str | None = None,
    priority: int = 4,
    tags: list[str] | None = None,
    timeout: float = 15.0,
) -> bool:
    payload: dict = {
        "topic": topic,
        "title": title,
        "message": message,
        "priority": priority,
        "tags": tags or ["chart_with_upwards_trend"],
    }
    if click:
        payload["click"] = click
    request = urllib.request.Request(
        base.rstrip("/") + "/",
        data=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
        headers={"Content-Type": "application/json", "User-Agent": USER_AGENT},
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            return 200 <= response.status < 300
    except (urllib.error.URLError, TimeoutError, ValueError):
        return False
