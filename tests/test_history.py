"""Tests for the second-source history endpoint (D-20261108-001, TQ-052).

Nothing here reaches OANDA: `history._get_json` is patched, so the tests pin
the contract - URL, mapping, filtering, refusals - without a token or a network.

Run from `backend/`:  py -m unittest discover -s tests -v
"""

from __future__ import annotations

import json
import os
import sys
import tempfile
import unittest
import urllib.error
import urllib.request
from unittest import mock

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import history  # noqa: E402
from api import App, serve  # noqa: E402
from store import Store  # noqa: E402

# 2026-11-05T12:00:00Z and 12:15:00Z.
BAR_1200 = 1_793_880_000_000
BAR_1215 = BAR_1200 + 900_000


def candle(time_ms: int, *, complete: bool = True, close: float = 4001.0) -> dict:
    stamp = history._iso(time_ms).replace("Z", ".000000000Z")
    return {
        "complete": complete,
        "volume": 42,
        "time": stamp,
        "mid": {
            "o": "4000.0",
            "h": f"{close + 1:.1f}",
            "l": f"{close - 1:.1f}",
            "c": f"{close:.1f}",
        },
    }


class FetchTests(unittest.TestCase):
    def setUp(self) -> None:
        history._CACHE.clear()
        self.env = mock.patch.dict(
            os.environ, {"OANDA_TOKEN": "tok", "OANDA_ENV": "practice"}
        )
        self.env.start()
        self.addCleanup(self.env.stop)

    def _patch(self, payload: dict):
        patched = mock.patch.object(history, "_get_json", return_value=payload)
        return patched.start(), patched.stop

    def test_maps_candles_to_the_live_feed_shape(self):
        payload = {"candles": [candle(BAR_1200)]}
        get, stop = self._patch(payload)
        self.addCleanup(stop)

        bars = history.fetch("XAUUSD", "15m", before_ms=BAR_1215, count=2)

        self.assertEqual(len(bars), 1)
        bar = bars[0]
        self.assertEqual(bar["openTime"], "2026-11-05T12:00:00Z")
        self.assertEqual(bar["close"], 4001.0)
        self.assertEqual(bar["volume"], 0)
        # OANDA's volume is a price count, so it is a tick proxy.
        self.assertEqual(bar["tickVolume"], 42)
        self.assertFalse(bar["isOpen"])

        url = get.call_args[0][0]
        self.assertIn("/v3/instruments/XAU_USD/candles?", url)
        self.assertIn("granularity=M15", url)
        self.assertIn("price=M", url)
        self.assertIn("from=2026-11-05T11%3A45%3A00Z", url)
        self.assertIn("to=2026-11-05T12%3A15%3A00Z", url)
        headers = get.call_args[0][1]
        self.assertEqual(headers["Authorization"], "Bearer tok")

    def test_drops_forming_and_out_of_window_candles(self):
        payload = {
            "candles": [
                candle(BAR_1200 - 1_800_000, close=3999.0),  # before `from`
                candle(BAR_1200),
                candle(BAR_1215, close=4005.0),  # at `before`: exclusive
                candle(BAR_1200 + 1_800_000, complete=False),  # still forming
            ]
        }
        get, stop = self._patch(payload)
        self.addCleanup(stop)

        bars = history.fetch("XAUUSD", "15m", before_ms=BAR_1215, count=2)

        self.assertEqual([bar["openTime"] for bar in bars], ["2026-11-05T12:00:00Z"])

    def test_orders_ascending_even_when_the_source_does_not(self):
        payload = {"candles": [candle(BAR_1200, close=4001.0), candle(BAR_1200 - 900_000, close=3999.0)]}
        get, stop = self._patch(payload)
        self.addCleanup(stop)

        bars = history.fetch("XAUUSD", "15m", before_ms=BAR_1215, count=2)

        self.assertEqual(
            [bar["openTime"] for bar in bars],
            ["2026-11-05T11:45:00Z", "2026-11-05T12:00:00Z"],
        )

    def test_closed_history_is_cached(self):
        get, stop = self._patch({"candles": [candle(BAR_1200)]})
        self.addCleanup(stop)

        history.fetch("XAUUSD", "15m", before_ms=BAR_1215, count=2)
        history.fetch("XAUUSD", "15m", before_ms=BAR_1215, count=2)

        self.assertEqual(get.call_count, 1)

    def test_upstream_failure_is_a_history_error(self):
        patched = mock.patch.object(
            history, "_get_json", side_effect=urllib.error.URLError("down")
        )
        patched.start()
        self.addCleanup(patched.stop)

        with self.assertRaises(history.HistoryError):
            history.fetch("XAUUSD", "15m", before_ms=BAR_1215, count=2)

    def test_unknown_symbol_or_cadence_is_a_caller_error(self):
        with self.assertRaises(ValueError):
            history.fetch("EURUSD", "15m", before_ms=BAR_1215, count=2)
        with self.assertRaises(ValueError):
            history.fetch("XAUUSD", "2h", before_ms=BAR_1215, count=2)

    def test_unconfigured_source_refuses(self):
        with mock.patch.dict(os.environ, {"OANDA_TOKEN": ""}):
            self.assertFalse(history.configured())
            with self.assertRaises(history.HistoryError):
                history.fetch("XAUUSD", "15m", before_ms=BAR_1215, count=2)


class RouteTests(unittest.TestCase):
    def setUp(self) -> None:
        history._CACHE.clear()
        self.dir = tempfile.mkdtemp(prefix="fxx-history-")
        self.store = Store(os.path.join(self.dir, "t.db"))
        self.app = App(self.store, {"key": "LEGACYKEY"}, self.dir)
        self.server = serve(self.app, "127.0.0.1", 0)
        self.base = f"http://127.0.0.1:{self.server.server_address[1]}"
        _, self.key = self.store.create_tenant()

    def tearDown(self) -> None:
        self.server.shutdown()
        self.server.server_close()
        self.store.close()

    def _get(self, path: str, key: str | None = None):
        request = urllib.request.Request(self.base + path)
        if key:
            request.add_header("Authorization", f"Bearer {key}")
        try:
            with urllib.request.urlopen(request, timeout=15) as response:
                return response.status, json.loads(response.read().decode("utf-8"))
        except urllib.error.HTTPError as error:
            return error.code, json.loads(error.read().decode("utf-8") or "{}")

    def test_requires_a_tenant_key(self):
        status, _ = self._get("/history?symbol=XAUUSD&interval=15m")
        self.assertEqual(status, 401)

    def test_answers_503_when_the_source_is_not_configured(self):
        with mock.patch.dict(os.environ, {"OANDA_TOKEN": ""}):
            status, body = self._get("/history?symbol=XAUUSD&interval=15m", self.key)
        self.assertEqual(status, 503, body)
        self.assertEqual(body["error"], "history is not configured")

    def test_serves_the_page_and_names_the_source(self):
        patched = mock.patch.object(
            history, "_get_json", return_value={"candles": [candle(BAR_1200)]}
        )
        patched.start()
        self.addCleanup(patched.stop)

        with mock.patch.dict(os.environ, {"OANDA_TOKEN": "tok"}):
            status, body = self._get(
                f"/history?symbol=XAUUSD&interval=15m&before={BAR_1215}&count=2",
                self.key,
            )

        self.assertEqual(status, 200, body)
        self.assertEqual(body["source"], "oanda")
        self.assertEqual(body["interval"], "15m")
        self.assertEqual(len(body["bars"]), 1)
        self.assertEqual(body["bars"][0]["openTime"], "2026-11-05T12:00:00Z")

    def test_refuses_an_unknown_cadence_with_400(self):
        with mock.patch.dict(os.environ, {"OANDA_TOKEN": "tok"}):
            status, body = self._get("/history?symbol=XAUUSD&interval=2h", self.key)
        self.assertEqual(status, 400, body)

    def test_health_names_the_source_only_when_configured(self):
        with mock.patch.dict(os.environ, {"OANDA_TOKEN": ""}):
            _, off = self._get("/health", self.key)
        self.assertIsNone(off["history"])

        with mock.patch.dict(os.environ, {"OANDA_TOKEN": "tok"}):
            _, on = self._get("/health", self.key)
        self.assertEqual(on["history"], "oanda")


if __name__ == "__main__":
    unittest.main()
