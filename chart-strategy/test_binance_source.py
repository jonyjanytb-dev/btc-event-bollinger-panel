"""Offline regression tests for the single Binance Futures signal source."""
import json
import sqlite3
import tempfile
import unittest
from decimal import Decimal
from pathlib import Path

import requests

from chart_strategy import BinanceFuturesClient, RULES, capture, evaluate, open_db


END = 1_800_000
RECEIVED = END + 3_000
SOURCE = "BINANCE_FUTURES_KLINES"


def market(interval, period, *, high=102, low=95):
    bars = []
    for i in range(RULES["minimum_indicator_bars"]):
        close = 100 + (i % 5) - 2
        opened = END - (RULES["minimum_indicator_bars"] - i) * period
        bars.append([opened, str(close), str(close + 1), str(close - 1), str(close), "1"])
    bars[-1] = [END - period, "100", str(high), str(low), "100", "1"]
    return {"status": "ok", "source": SOURCE, "symbol": "BTCUSDT",
            "interval": interval, "truncated": False, "candles": bars}


def pair(**kwargs):
    return {"five_min": market("5m", 300_000, **kwargs.get("five", {})),
            "one_min": market("1m", 60_000, **kwargs.get("one", {}))}


def response_rows(data, period):
    return [[bar[0], *bar[1:], bar[0] + period - 1, "0", 0, "0", "0", "0"]
            for bar in data["candles"]]


class FakeResponse:
    def __init__(self, value):
        self.value = value

    def raise_for_status(self):
        return None

    def json(self):
        return self.value


class FakeSession:
    def __init__(self, values):
        self.values = list(values)
        self.calls = []
        self.trust_env = True

    def get(self, url, **kwargs):
        self.calls.append((url, kwargs))
        value = self.values.pop(0)
        if isinstance(value, Exception):
            raise value
        return FakeResponse(value)

    def close(self):
        pass


class BinanceSourceTests(unittest.TestCase):
    def test_closed_futures_bars_preserve_dual_lower_signal(self):
        result = evaluate(pair(), RECEIVED)
        self.assertEqual((result["direction"], result["reason"]),
                         ("HIGHER", "BOTH_LOWER_TOUCHED"))
        self.assertEqual(result["entry_ms"], END + 60_000)
        self.assertEqual(result["source"], SOURCE)

    def test_only_one_timeframe_touch_does_not_trade(self):
        result = evaluate(pair(five={"low": 99}), RECEIVED)
        self.assertEqual(result["direction"], "NO_SIGNAL")

    def test_stale_5m_upper_does_not_create_false_short(self):
        data = pair(five={"high": 102, "low": 99},
                    one={"high": 111, "low": 99})
        result = evaluate(data, RECEIVED)
        self.assertEqual((result["direction"], result["reason"]),
                         ("NO_SIGNAL", "CONDITIONS_NOT_MET"))

    def test_other_source_or_symbol_is_rejected(self):
        for field, value in (("source", "OPENMARKET_CHART"),
                             ("symbol", "ETHUSDT"), ("interval", "1h")):
            with self.subTest(field=field):
                data = pair()
                data["five_min"][field] = value
                self.assertEqual(evaluate(data, RECEIVED)["direction"], "UNAVAILABLE")

    def test_gap_or_short_history_is_rejected(self):
        data = pair()
        data["five_min"]["candles"][-10][0] += 300_000
        self.assertEqual(evaluate(data, RECEIVED)["direction"], "UNAVAILABLE")
        data = pair()
        data["five_min"]["candles"] = data["five_min"]["candles"][-20:]
        self.assertEqual(evaluate(data, RECEIVED)["reason"], "INDICATOR_HISTORY_UNAVAILABLE")

    def test_stale_or_unclosed_candle_is_rejected(self):
        self.assertEqual(evaluate(pair(), END + 500)["direction"], "UNAVAILABLE")
        self.assertEqual(evaluate(pair(), END + 56_000)["direction"], "UNAVAILABLE")
        data = pair()
        data["one_min"]["candles"][-1][0] = END
        self.assertEqual(evaluate(data, RECEIVED)["direction"], "UNAVAILABLE")

    def test_snapshot_uses_only_binance_futures_with_closed_end_time(self):
        data = pair()
        session = FakeSession([response_rows(data["five_min"], 300_000),
                               response_rows(data["one_min"], 60_000)])
        client = BinanceFuturesClient(session=session, clock=lambda: RECEIVED)
        snapshot, received = client.snapshot()
        self.assertEqual(received, RECEIVED)
        self.assertEqual(evaluate(snapshot, received)["direction"], "HIGHER")
        self.assertEqual([call[1]["params"]["interval"] for call in session.calls],
                         ["5m", "1m"])
        self.assertTrue(all(call[1]["params"]["endTime"] == END - 1
                            for call in session.calls))
        self.assertTrue(all(call[1]["params"]["symbol"] == "BTCUSDT"
                            for call in session.calls))
        self.assertTrue(all("binance.com" in call[0] for call in session.calls))
        self.assertFalse(session.trust_env)

    def test_timeout_retries_without_creating_a_signal(self):
        data = pair()
        session = FakeSession([requests.Timeout("transient"),
                               response_rows(data["five_min"], 300_000),
                               response_rows(data["one_min"], 60_000)])
        pauses = []
        client = BinanceFuturesClient(session=session, clock=lambda: RECEIVED,
                                      sleep=pauses.append)
        snapshot, received = client.snapshot()
        self.assertEqual(evaluate(snapshot, received)["direction"], "HIGHER")
        self.assertEqual(len(session.calls), 3)
        self.assertEqual(len(pauses), 1)

    def test_bad_rest_payload_is_rejected(self):
        client = BinanceFuturesClient(session=FakeSession([{"code": -1}]),
                                      clock=lambda: RECEIVED, sleep=lambda _: None)
        with self.assertRaises(ValueError):
            client.snapshot()

    def test_capture_preserves_raw_binance_source_without_order(self):
        class Client:
            def snapshot(self):
                return pair(), RECEIVED

        with tempfile.TemporaryDirectory() as directory:
            db = open_db(Path(directory) / "test.sqlite")
            try:
                self.assertEqual(capture(Client(), db)["direction"], "HIGHER")
                workspace, payload, execution = db.execute(
                    "SELECT workspace,payload,execution FROM observations").fetchone()
                self.assertEqual(workspace, SOURCE)
                self.assertEqual(json.loads(payload)["five_min"]["source"], SOURCE)
                self.assertEqual(execution, "NO TRADE")
            finally:
                db.close()


if __name__ == "__main__":
    unittest.main()
