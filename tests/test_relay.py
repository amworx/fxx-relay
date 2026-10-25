"""Parity tests for the ported rules and the sync store.

Run from `backend/`:  py -m unittest discover -s tests -v
"""

from __future__ import annotations

import os
import sqlite3
import sys
import tempfile
import time
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import http.client  # noqa: E402
import json  # noqa: E402
import threading  # noqa: E402
import urllib.error  # noqa: E402
import urllib.request  # noqa: E402

import rules  # noqa: E402
from api import App, serve  # noqa: E402
from store import Store  # noqa: E402

STEP = rules.INTERVAL_MS["15m"]
BASE_TIME = 1_700_000_000_000
NOW = BASE_TIME + 100 * STEP


def bars(closes: list[float], start: int = BASE_TIME) -> list[dict]:
    return [
        {"open_ms": start + index * STEP, "close": close}
        for index, close in enumerate(closes)
    ]


def iso(ms: int) -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(ms / 1000))


def alert(**overrides) -> dict:
    record = {
        "id": "a1",
        "price": 4000.0,
        "operator": "above",
        "armed": True,
        "cadence": "barClose",
        "interval": "15m",
        "createdAt": iso(BASE_TIME - 10 * STEP),
        "watchingSince": iso(BASE_TIME - 10 * STEP),
        "triggeredAt": None,
    }
    record.update(overrides)
    return record


class ThresholdTests(unittest.TestCase):
    def test_above_needs_a_full_tick(self) -> None:
        self.assertTrue(
            rules.should_fire(alert(), bars([4000.01]), "15m", NOW, None)
        )
        self.assertFalse(
            rules.should_fire(alert(), bars([4000.005]), "15m", NOW, None)
        )

    def test_below_needs_a_full_tick(self) -> None:
        record = alert(operator="below", price=4000.0)
        self.assertTrue(rules.should_fire(record, bars([3999.99]), "15m", NOW, None))
        self.assertFalse(rules.should_fire(record, bars([4000.0]), "15m", NOW, None))

    def test_disarmed_and_expired_are_silent(self) -> None:
        self.assertFalse(
            rules.should_fire(alert(armed=False), bars([4100.0]), "15m", NOW, None)
        )
        expired = alert(expiresAt=iso(NOW - 1000))
        self.assertFalse(rules.should_fire(expired, bars([4100.0]), "15m", NOW, None))


class CrossingTests(unittest.TestCase):
    def test_crosses_on_equality_from_either_side(self) -> None:
        record = alert(operator="crosses")
        self.assertTrue(rules.should_fire(record, bars([3999.0, 4000.0]), "15m", NOW, None))
        self.assertTrue(rules.should_fire(record, bars([4001.0, 4000.0]), "15m", NOW, None))

    def test_first_bar_is_only_a_baseline(self) -> None:
        record = alert(operator="crosses")
        self.assertFalse(rules.should_fire(record, bars([4100.0]), "15m", NOW, None))

    def test_direction_is_respected(self) -> None:
        up = alert(operator="crossesUp")
        down = alert(operator="crossesDown")
        self.assertTrue(rules.should_fire(up, bars([3999.0, 4000.0]), "15m", NOW, None))
        self.assertFalse(rules.should_fire(up, bars([4001.0, 4000.0]), "15m", NOW, None))
        self.assertTrue(rules.should_fire(down, bars([4001.0, 4000.0]), "15m", NOW, None))


class RangeTests(unittest.TestCase):
    def test_bounds_are_inclusive_for_inside(self) -> None:
        record = alert(operator="insideRange", price=4000.0, price2=4010.0)
        self.assertTrue(rules.should_fire(record, bars([4000.0]), "15m", NOW, None))
        self.assertTrue(rules.should_fire(record, bars([4010.0]), "15m", NOW, None))
        self.assertFalse(rules.should_fire(record, bars([4010.01]), "15m", NOW, None))

    def test_outside_is_the_complement(self) -> None:
        record = alert(operator="outsideRange", price=4000.0, price2=4010.0)
        self.assertTrue(rules.should_fire(record, bars([3999.99]), "15m", NOW, None))
        self.assertFalse(rules.should_fire(record, bars([4005.0]), "15m", NOW, None))

    def test_enters_needs_a_transition(self) -> None:
        record = alert(operator="entersRange", price=4000.0, price2=4010.0)
        self.assertTrue(
            rules.should_fire(record, bars([3995.0, 4005.0]), "15m", NOW, None)
        )
        self.assertFalse(
            rules.should_fire(record, bars([4005.0, 4006.0]), "15m", NOW, None)
        )


class PercentTests(unittest.TestCase):
    def percent_alert(self, **overrides) -> dict:
        record = alert(operator="movesBy", price=2.0, lookback=2, cadence="barClose")
        record.update(overrides)
        return record

    def test_exact_move_fires(self) -> None:
        self.assertTrue(
            rules.should_fire(self.percent_alert(), bars([100.0, 101.0, 102.0]), "15m", NOW, None)
        )

    def test_float_noise_is_tolerated(self) -> None:
        closes = [100.0, 100.5, 101.99999999999999]
        self.assertTrue(rules.should_fire(self.percent_alert(), bars(closes), "15m", NOW, None))

    def test_short_series_is_silent(self) -> None:
        self.assertFalse(
            rules.should_fire(self.percent_alert(), bars([100.0, 102.0]), "15m", NOW, None)
        )

    def test_symmetric_in_both_directions(self) -> None:
        self.assertTrue(
            rules.should_fire(self.percent_alert(), bars([100.0, 99.0, 98.0]), "15m", NOW, None)
        )

    def test_lookback_is_validated(self) -> None:
        self.assertFalse(rules.is_valid(self.percent_alert(lookback=1)))
        self.assertFalse(rules.is_valid(self.percent_alert(lookback=101)))
        self.assertTrue(rules.is_valid(self.percent_alert()))


class GateTests(unittest.TestCase):
    def test_bar_closed_before_watching_since_is_not_ours(self) -> None:
        record = alert(watchingSince=iso(BASE_TIME + STEP))
        self.assertFalse(rules.should_fire(record, bars([4100.0]), "15m", NOW, None))

    def test_same_bar_cannot_ring_twice(self) -> None:
        closes_at = BASE_TIME + STEP
        record = alert(triggeredAt=iso(closes_at))
        self.assertFalse(rules.should_fire(record, bars([4100.0]), "15m", NOW, None))
        self.assertFalse(
            rules.should_fire(alert(), bars([4100.0]), "15m", NOW, closes_at)
        )

    def test_next_bar_is_new_again(self) -> None:
        self.assertTrue(
            rules.should_fire(alert(), bars([4100.0, 4101.0]), "15m", NOW, BASE_TIME + STEP)
        )


class StoreTests(unittest.TestCase):
    def setUp(self) -> None:
        self.dir = tempfile.mkdtemp(prefix="fxx-relay-")
        self.store = Store(os.path.join(self.dir, "t.db"))

    def tearDown(self) -> None:
        self.store.close()

    def test_push_pull_roundtrip(self) -> None:
        changed = self.store.apply_push(
            "main", {"alerts": [{"id": "a1", "_u": 1000, "_d": False, "price": 4000}]}, "dev-a"
        )
        self.assertEqual(changed, 1)
        pulled = self.store.pull("main", 0)["alerts"]
        self.assertEqual(pulled[0]["id"], "a1")
        self.assertEqual(pulled[0]["price"], 4000)
        self.assertFalse(pulled[0]["_d"])
        self.assertEqual(self.store.pull("main", 1000)["alerts"], [])

    def test_last_write_wins_and_ties_break_by_device(self) -> None:
        self.store.apply_push("main", {"alerts": [{"id": "a1", "_u": 2000, "price": 2}]}, "dev-b")
        self.store.apply_push("main", {"alerts": [{"id": "a1", "_u": 1000, "price": 1}]}, "dev-c")
        self.assertEqual(self.store.pull("main", 0)["alerts"][0]["price"], 2)
        self.store.apply_push("main", {"alerts": [{"id": "a1", "_u": 2000, "price": 3}]}, "dev-c")
        self.assertEqual(self.store.pull("main", 0)["alerts"][0]["price"], 3)
        self.store.apply_push("main", {"alerts": [{"id": "a1", "_u": 2000, "price": 4}]}, "dev-b")
        self.assertEqual(self.store.pull("main", 0)["alerts"][0]["price"], 3)

    def test_tombstone_is_a_record(self) -> None:
        self.store.apply_push("main", {"journal": [{"id": "j1", "_u": 10, "note": "x"}]}, "dev-a")
        self.store.apply_push("main", {"journal": [{"id": "j1", "_u": 20, "_d": True}]}, "dev-a")
        record = self.store.pull("main", 0)["journal"][0]
        self.assertTrue(record["_d"])
        self.assertEqual(record["id"], "j1")

    def test_fire_ids_are_physically_unique(self) -> None:
        fire = {"id": "a1@1700", "_u": 5, "price": 4000}
        self.store.put("main", "fires", fire, "relay")
        self.assertTrue(self.store.exists("main", "fires", "a1@1700"))
        self.assertFalse(self.store.exists("main", "fires", "a1@1701"))

    def test_cursor_never_rewinds(self) -> None:
        self.store.set_cursor("15m", 500)
        self.store.set_cursor("15m", 400)
        self.assertEqual(self.store.cursor("15m"), 500)

    def test_rearm_clears_the_fire_memory(self) -> None:
        self.store.remember_alert("main", "a1", True, 100)
        self.store.mark_fired("main", "a1", 900)
        self.assertEqual(self.store.alert_state("main", "a1")["last_fired_ms"], 900)
        self.store.remember_alert("main", "a1", False, 100)
        self.store.remember_alert("main", "a1", True, 100)
        self.assertIsNone(self.store.alert_state("main", "a1")["last_fired_ms"])

    def test_new_condition_clears_the_fire_memory(self) -> None:
        self.store.remember_alert("main", "a1", True, 100)
        self.store.mark_fired("main", "a1", 900)
        self.store.remember_alert("main", "a1", True, 500)
        self.assertIsNone(self.store.alert_state("main", "a1")["last_fired_ms"])


class EventsTests(unittest.TestCase):
    """The long-poll the phone holds open instead of a push subscription."""

    KEY = "TESTKEY"

    def setUp(self) -> None:
        self.dir = tempfile.mkdtemp(prefix="fxx-events-")
        self.store = Store(os.path.join(self.dir, "t.db"))
        self.app = App(self.store, {"key": self.KEY}, self.dir)
        self.server = serve(self.app, "127.0.0.1", 0)
        self.base = f"http://127.0.0.1:{self.server.server_address[1]}"

    def tearDown(self) -> None:
        self.server.shutdown()
        self.server.server_close()
        self.store.close()

    def get(self, query: str, key: str | None = KEY):
        request = urllib.request.Request(self.base + "/events?" + query)
        if key:
            request.add_header("Authorization", f"Bearer {key}")
        with urllib.request.urlopen(request, timeout=40) as response:
            return json.loads(response.read().decode("utf-8"))

    def test_fire_is_returned_at_once(self) -> None:
        self.store.put("main", "fires", {"id": "a1@1700", "price": 4000.0, "_u": 1234}, "relay")
        started = time.time()
        payload = self.get("since=0&wait=10")
        self.assertLess(time.time() - started, 2.0)
        self.assertEqual([f["id"] for f in payload["fires"]], ["a1@1700"])
        self.assertEqual(payload["next"], 1234)

    def test_waiting_call_wakes_when_a_fire_lands(self) -> None:
        def land() -> None:
            time.sleep(0.8)
            self.store.put("main", "fires", {"id": "a2@1800", "_u": 5000}, "relay")

        threading.Thread(target=land, daemon=True).start()
        started = time.time()
        payload = self.get("since=1&wait=15")
        elapsed = time.time() - started
        self.assertGreater(elapsed, 0.5)
        self.assertLess(elapsed, 4.0)
        self.assertEqual([f["id"] for f in payload["fires"]], ["a2@1800"])

    def test_timeout_is_empty_and_never_moves_the_cursor_forward(self) -> None:
        before = int(time.time() * 1000)
        payload = self.get("since=900000&wait=1")
        self.assertEqual(payload["fires"], [])
        self.assertGreaterEqual(payload["next"], 900000)
        self.assertLessEqual(payload["next"], before + 2000)

    def test_missing_key_is_rejected(self) -> None:
        with self.assertRaises(urllib.error.HTTPError) as caught:
            self.get("since=0&wait=1", key=None)
        self.assertEqual(caught.exception.code, 401)


class KeepAliveTests(unittest.TestCase):
    """A refused POST must not poison the connection it arrived on.

    cloudflared pools its connections to the relay, so a body left unread is
    glued to the next request's request line and the server answers 501
    Unsupported method - which is exactly what a live device hit.
    """

    KEY = "TESTKEY"

    def setUp(self) -> None:
        self.dir = tempfile.mkdtemp(prefix="fxx-keepalive-")
        self.store = Store(os.path.join(self.dir, "t.db"))
        self.app = App(self.store, {"key": self.KEY}, self.dir)
        self.server = serve(self.app, "127.0.0.1", 0)
        self.host = "127.0.0.1"
        self.port = self.server.server_address[1]

    def tearDown(self) -> None:
        self.server.shutdown()
        self.server.server_close()
        self.store.close()

    def connection(self) -> http.client.HTTPConnection:
        return http.client.HTTPConnection(self.host, self.port, timeout=40)

    def test_rejected_post_body_does_not_corrupt_the_next_request(self) -> None:
        body = json.dumps({"device": "d1", "since": 0, "push": {"fires": []}})
        first = self.connection()
        first.request(
            "POST",
            "/sync",
            body=body,
            headers={"Authorization": "Bearer WRONG", "Content-Length": str(len(body))},
        )
        refused = first.getresponse()
        self.assertEqual(refused.status, 401)
        refused.read()
        # Same connection, as a pooled origin connection would be reused.
        first.request("GET", "/health", headers={"Authorization": f"Bearer {self.KEY}"})
        second = first.getresponse()
        self.assertEqual(second.status, 200)
        json.loads(second.read().decode("utf-8"))
        first.close()

    def test_rejected_post_on_wrong_path_is_drained_too(self) -> None:
        body = json.dumps({"device": "d1"})
        first = self.connection()
        first.request(
            "POST",
            "/nope",
            body=body,
            headers={"Authorization": f"Bearer {self.KEY}", "Content-Length": str(len(body))},
        )
        refused = first.getresponse()
        self.assertEqual(refused.status, 404)
        refused.read()
        first.request("GET", "/health", headers={"Authorization": f"Bearer {self.KEY}"})
        second = first.getresponse()
        self.assertEqual(second.status, 200)
        second.read()
        first.close()


class TenancyTests(unittest.TestCase):
    """One key per user: no tenant may read, overwrite or hear another's data."""

    LEGACY = "LEGACYKEY"

    def setUp(self) -> None:
        self.dir = tempfile.mkdtemp(prefix="fxx-tenants-")
        self.store = Store(os.path.join(self.dir, "t.db"))
        self.app = App(self.store, {"key": self.LEGACY}, self.dir)
        self.server = serve(self.app, "127.0.0.1", 0)
        self.base = f"http://127.0.0.1:{self.server.server_address[1]}"

    def tearDown(self) -> None:
        self.server.shutdown()
        self.server.server_close()
        self.store.close()

    def _post(self, path: str, payload: dict | None = None, key: str | None = None):
        data = json.dumps(payload or {}).encode("utf-8")
        request = urllib.request.Request(self.base + path, data=data, method="POST")
        request.add_header("Content-Type", "application/json")
        if key:
            request.add_header("Authorization", f"Bearer {key}")
        try:
            with urllib.request.urlopen(request, timeout=15) as response:
                return response.status, json.loads(response.read().decode("utf-8"))
        except urllib.error.HTTPError as error:
            return error.code, json.loads(error.read().decode("utf-8") or "{}")

    def _get(self, path: str, key: str | None = None):
        request = urllib.request.Request(self.base + path)
        if key:
            request.add_header("Authorization", f"Bearer {key}")
        try:
            with urllib.request.urlopen(request, timeout=15) as response:
                return response.status, json.loads(response.read().decode("utf-8"))
        except urllib.error.HTTPError as error:
            return error.code, json.loads(error.read().decode("utf-8") or "{}")

    def register(self) -> tuple[str, str]:
        status, body = self._post("/register")
        self.assertEqual(status, 201, body)
        return body["tenant"], body["key"]

    def pull(self, key: str, since: int = 0) -> dict:
        status, body = self._post(
            "/sync", {"device": "probe", "since": since, "push": {}}, key
        )
        self.assertEqual(status, 200, body)
        return body["pull"]

    def test_register_mints_a_tenant_and_its_key(self) -> None:
        tenant, key = self.register()
        self.assertTrue(tenant.startswith("t_"))
        self.assertEqual(len(key), 48)
        self.assertEqual(self.store.tenant_for_key(key), tenant)

    def test_an_unknown_key_is_refused(self) -> None:
        status, _ = self._get("/health", "nope")
        self.assertEqual(status, 401)

    def test_registered_tenants_are_isolated(self) -> None:
        tenant_a, key_a = self.register()
        tenant_b, key_b = self.register()
        self.assertNotEqual(tenant_a, tenant_b)

        self._post(
            "/sync",
            {"device": "a", "since": 0, "push": {"alerts": [{"id": "x", "_u": 100, "price": 1}]}},
            key_a,
        )
        self.assertEqual([r["id"] for r in self.pull(key_a)["alerts"]], ["x"])
        self.assertEqual(self.pull(key_b)["alerts"], [])

        # A later write from another tenant must not land in A's row.
        self._post(
            "/sync",
            {"device": "b", "since": 0, "push": {"alerts": [{"id": "x", "_u": 999, "price": 7}]}},
            key_b,
        )
        self.assertEqual(self.pull(key_a)["alerts"][0]["price"], 1)
        self.assertEqual(self.pull(key_b)["alerts"][0]["price"], 7)

        self.assertEqual(self._get("/health", key_a)[1]["alerts"], 1)
        self.assertEqual(self._get("/health", key_b)[1]["alerts"], 1)

    def test_fires_only_wake_their_own_tenant(self) -> None:
        _, key_a = self.register()
        _, key_b = self.register()
        self.store.put("main", "fires", {"id": "main@1", "_u": 1, "price": 1}, "relay")
        self.assertEqual(self._get("/events?since=0&wait=1", key_a)[1]["fires"], [])
        self.assertEqual(self._get("/events?since=0&wait=1", key_b)[1]["fires"], [])
        self.assertEqual(
            [f["id"] for f in self._get("/events?since=0&wait=1", self.LEGACY)[1]["fires"]],
            ["main@1"],
        )

    def test_the_legacy_key_still_reaches_tenant_main(self) -> None:
        status, body = self._get("/health", self.LEGACY)
        self.assertEqual(status, 200)
        self.assertEqual(body["tenant"], "main")

    def test_signup_is_rate_limited_per_address(self) -> None:
        for _ in range(10):
            self.assertEqual(self._post("/register")[0], 201)
        self.assertEqual(self._post("/register")[0], 429)

    def test_link_claims_the_anonymous_tenant(self) -> None:
        self.app.cfg["google_client_id"] = "client.apps.googleusercontent.com"
        self.app.verify_google = lambda token: (
            {"sub": "g1", "email": "u@e.com"} if token == "good" else None
        )
        tenant, key = self.register()
        status, body = self._post("/link", {"id_token": "good"}, key)
        self.assertEqual(status, 200, body)
        self.assertEqual(body["tenant"], tenant)
        self.assertEqual(body["merged"], 0)
        self.assertEqual(self.store.tenant_for_key(body["key"]), tenant)
        # The key that was used to link must keep working: signing in on a
        # second phone must not sign the first one out.
        self.assertEqual(self.store.tenant_for_key(key), tenant)
        self.assertEqual(self.store.tenant(tenant)["google_sub"], "g1")

    def test_link_merges_a_second_anonymous_tenant_into_the_account(self) -> None:
        self.app.cfg["google_client_id"] = "client.apps.googleusercontent.com"
        self.app.verify_google = lambda token: (
            {"sub": "g1", "email": "u@e.com"} if token == "good" else None
        )
        first, key_first = self.register()
        self._post("/link", {"id_token": "good"}, key_first)

        second, key_second = self.register()
        self._post(
            "/sync",
            {"device": "d2", "since": 0, "push": {"journal": [{"id": "j2", "_u": 500, "note": "n"}]}},
            key_second,
        )
        status, body = self._post("/link", {"id_token": "good"}, key_second)
        self.assertEqual(status, 200, body)
        self.assertEqual(body["tenant"], first)
        self.assertEqual(body["merged"], 1)
        self.assertEqual([r["id"] for r in self.pull(body["key"])["journal"]], ["j2"])

    def test_link_is_refused_when_unconfigured(self) -> None:
        _, key = self.register()
        status, _ = self._post("/link", {"id_token": "good"}, key)
        self.assertEqual(status, 503)

    def test_link_rejects_a_bad_token(self) -> None:
        self.app.cfg["google_client_id"] = "client.apps.googleusercontent.com"
        self.app.verify_google = lambda token: None
        _, key = self.register()
        status, _ = self._post("/link", {"id_token": "bad"}, key)
        self.assertEqual(status, 401)

    def test_link_refuses_to_move_a_device_to_another_account(self) -> None:
        self.app.cfg["google_client_id"] = "client.apps.googleusercontent.com"
        people = {
            "first": {"sub": "g1", "email": "a@e.com"},
            "second": {"sub": "g2", "email": "b@e.com"},
        }
        self.app.verify_google = lambda token: people.get(token)
        tenant, key = self.register()
        self.assertEqual(self._post("/link", {"id_token": "first"}, key)[0], 200)

        status, body = self._post("/link", {"id_token": "second"}, key)
        self.assertEqual(status, 409, body)
        self.assertEqual(self.store.tenant(tenant)["google_sub"], "g1")
        # Same account again is a no-op re-link, not a conflict.
        self.assertEqual(self._post("/link", {"id_token": "first"}, key)[0], 200)


class MigrationTests(unittest.TestCase):
    def test_an_old_database_becomes_tenant_main(self) -> None:
        path = os.path.join(tempfile.mkdtemp(prefix="fxx-mig-"), "old.db")
        legacy = sqlite3.connect(path)
        legacy.executescript(
            "CREATE TABLE kv_sync (bucket TEXT NOT NULL, id TEXT NOT NULL,"
            " updated_at INTEGER NOT NULL, device TEXT NOT NULL,"
            " deleted INTEGER NOT NULL, body TEXT NOT NULL, PRIMARY KEY (bucket, id));"
        )
        legacy.execute(
            "INSERT INTO kv_sync VALUES ('alerts','a1',1000,'d1',0,'{\"price\":4000}')"
        )
        legacy.execute(
            "INSERT INTO kv_sync VALUES ('fires','a1@1700',1200,'relay',0,'{\"price\":4000}')"
        )
        legacy.commit()
        legacy.close()

        store = Store(path)
        try:
            rows = store.pull("main", 0)["alerts"]
            self.assertEqual([r["id"] for r in rows], ["a1"])
            self.assertEqual(rows[0]["_u"], 1000)
            self.assertEqual(rows[0]["price"], 4000)
            self.assertEqual([f["id"] for f in store.fires_since("main", 0)], ["a1@1700"])
            self.assertEqual(store.pull("main", 0)["journal"], [])
        finally:
            store.close()

    def test_an_old_alerts_table_is_rebuilt_even_if_kv_sync_already_migrated(self) -> None:
        # The state a cut that rebuilt only kv_sync leaves behind: the alert
        # state table still has no tenant column, and nothing else migrates it.
        path = os.path.join(tempfile.mkdtemp(prefix="fxx-mig-"), "half.db")
        legacy = sqlite3.connect(path)
        legacy.executescript(
            "CREATE TABLE kv_sync (tenant TEXT NOT NULL, bucket TEXT NOT NULL,"
            " id TEXT NOT NULL, updated_at INTEGER NOT NULL, device TEXT NOT NULL,"
            " deleted INTEGER NOT NULL, body TEXT NOT NULL,"
            " PRIMARY KEY (tenant, bucket, id));"
            "CREATE TABLE server_alerts (id TEXT NOT NULL PRIMARY KEY,"
            " armed INTEGER NOT NULL, watching_since INTEGER NOT NULL,"
            " last_fired_ms INTEGER, last_bar_ms INTEGER);"
        )
        legacy.execute("INSERT INTO server_alerts VALUES ('a1',1,900,950,960)")
        legacy.commit()
        legacy.close()

        store = Store(path)
        try:
            state = store.alert_state("main", "a1")
            self.assertEqual(state["armed"], 1)
            self.assertEqual(state["watching_since"], 900)
            self.assertEqual(state["last_fired_ms"], 950)
            # A write must land now that the column exists.
            store.mark_fired("main", "a1", 1000)
            self.assertEqual(store.alert_state("main", "a1")["last_fired_ms"], 1000)
            self.assertIsNone(store.alert_state("other", "a1")["armed"])
        finally:
            store.close()


if __name__ == "__main__":
    unittest.main()


class ConfigFromEnvironmentTests(unittest.TestCase):
    """A container host recreates config.json on every deploy.

    Anything that only lived in that file (Google linking, the symbol) would be
    silently absent there, which is how `/link` came to answer "not configured"
    on a relay whose dashboard showed GOOGLE_CLIENT_ID set.
    """

    def setUp(self) -> None:
        import main

        self.main = main
        self._saved_path = main.CONFIG_PATH
        self._saved_env = {
            name: os.environ.get(name) for name, _ in main.ENV_SETTINGS
        }
        handle, self._path = tempfile.mkstemp(suffix=".json")
        os.close(handle)
        os.unlink(self._path)
        main.CONFIG_PATH = self._path
        for name, _ in main.ENV_SETTINGS:
            os.environ.pop(name, None)

    def tearDown(self) -> None:
        self.main.CONFIG_PATH = self._saved_path
        for name, value in self._saved_env.items():
            if value is None:
                os.environ.pop(name, None)
            else:
                os.environ[name] = value
        if os.path.exists(self._path):
            os.unlink(self._path)

    def test_google_client_id_comes_from_the_environment(self) -> None:
        os.environ["GOOGLE_CLIENT_ID"] = "1234-abc.apps.googleusercontent.com"

        config = self.main.load_config()

        self.assertEqual(
            config["google_client_id"], "1234-abc.apps.googleusercontent.com"
        )

    def test_a_missing_variable_leaves_linking_off(self) -> None:
        config = self.main.load_config()

        self.assertNotIn("google_client_id", config)
