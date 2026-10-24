"""End-to-end protocol smoke test, run on the relay itself."""

import json
import os
import time
import urllib.request

CONFIG = json.load(open(os.path.expanduser("~/fxx-relay/config.json"), encoding="utf-8"))
BASE = f"http://127.0.0.1:{CONFIG['port']}"
KEY = CONFIG["key"]


def call(path, payload=None):
    body = json.dumps(payload).encode("utf-8") if payload is not None else None
    request = urllib.request.Request(
        BASE + path,
        data=body,
        headers={"Authorization": "Bearer " + KEY, "Content-Type": "application/json"},
        method="POST" if body else "GET",
    )
    with urllib.request.urlopen(request, timeout=10) as response:
        return json.load(response)


health = call("/health")
print("health:", json.dumps(health, ensure_ascii=False))

now = int(time.time() * 1000)
alert = {
    "id": "smoke-1",
    "_u": now,
    "_d": False,
    "price": 4000.0,
    "operator": "above",
    "armed": True,
    "cadence": "barClose",
    "interval": "15m",
    "createdAt": "2026-10-20T00:00:00Z",
    "watchingSince": "2026-10-20T00:00:00Z",
}

first = call("/sync", {"device": "smoke", "since": 0, "push": {"alerts": [alert]}})
print("push changed:", first["changed"], "pulled:", len(first["pull"]["alerts"]))

second = call("/sync", {"device": "smoke", "since": first["now"], "push": {}})
print("incremental pull (expect 0):", len(second["pull"]["alerts"]))

stale = call(
    "/sync",
    {"device": "smoke", "since": 0, "push": {"alerts": [dict(alert, _u=now - 1000, price=1.0)]}},
)
print("stale write ignored (expect price 4000.0):", stale["pull"]["alerts"][0]["price"])

call(
    "/sync",
    {"device": "smoke", "since": 0, "push": {"alerts": [dict(alert, _u=now + 1000, _d=True)]}},
)
print("cleanup done")

unauthorised = None
try:
    request = urllib.request.Request(BASE + "/health", headers={"Authorization": "Bearer nope"})
    urllib.request.urlopen(request, timeout=5)
except Exception as error:
    unauthorised = getattr(error, "code", error)
print("wrong key rejected:", unauthorised)
