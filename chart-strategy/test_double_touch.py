"""Synthetic dual timeframe Bollinger rules, independent of network and orders."""
import json
import tempfile
import unittest
from pathlib import Path

from chart_strategy import RULES, capture, evaluate, open_db


END = 1_800_000
RECEIVED = END + 3_000


def market(interval, period, low=95, high=102):
    bars = []
    for i in range(RULES["minimum_indicator_bars"]):
        close = 100 + (i % 5) - 2
        opened = END - (RULES["minimum_indicator_bars"] - i) * period
        bars.append([opened, str(close), str(close + 1), str(close - 1),
                     str(close), "1"])
    bars[-1] = [END - period, "100", str(high), str(low), "100", "1"]
    return {"status": "ok", "source": RULES["source_id"], "symbol": "BTCUSDT",
            "interval": interval, "truncated": False, "candles": bars}


def pair(**kwargs):
    return {"five_min": market("5m", 300_000, **kwargs.get("five", {})),
            "one_min": market("1m", 60_000, **kwargs.get("one", {}))}


class DoubleTouchTests(unittest.TestCase):
    def test_both_lower_touch_generates_higher_30m(self):
        decision = evaluate(pair(), RECEIVED)
        self.assertEqual((decision["direction"], decision["reason"]),
                         ("HIGHER", "BOTH_LOWER_TOUCHED"))
        self.assertEqual(decision["bar_close_ms"], END)
        self.assertEqual(decision["expiry_ms"] - decision["entry_ms"], 1_800_000)

    def test_both_upper_touch_generates_lower_30m(self):
        decision = evaluate(pair(five={"high": 111, "low": 99},
                                 one={"high": 111, "low": 99}), RECEIVED)
        self.assertEqual((decision["direction"], decision["reason"]),
                         ("LOWER", "BOTH_UPPER_TOUCHED"))

    def test_only_one_period_touched_is_not_a_signal(self):
        for kwargs in ({"five": {"low": 99}}, {"one": {"low": 99}}):
            with self.subTest(kwargs=kwargs):
                self.assertEqual(evaluate(pair(**kwargs), RECEIVED)["direction"],
                                 "NO_SIGNAL")

    def test_two_sides_touched_is_ambiguous(self):
        decision = evaluate(pair(five={"high": 111}, one={"high": 111}), RECEIVED)
        self.assertEqual((decision["direction"], decision["reason"]),
                         ("NO_SIGNAL", "AMBIGUOUS_TOUCH"))

    def test_misaligned_last_minute_is_rejected(self):
        data = pair()
        data["one_min"]["candles"][-1][0] -= 60_000
        self.assertEqual(evaluate(data, RECEIVED)["direction"], "UNAVAILABLE")

    def test_unclosed_bar_and_stale_data_rejected(self):
        self.assertEqual(evaluate(pair(), END + 500)["direction"], "UNAVAILABLE")
        self.assertEqual(evaluate(pair(), END + 56_000)["direction"], "UNAVAILABLE")

    def test_both_candles_must_be_valid(self):
        data = pair()
        data["five_min"]["candles"][-1][2] = "NaN"
        self.assertEqual(evaluate(data, RECEIVED)["direction"], "UNAVAILABLE")
        data = pair()
        data["one_min"]["candles"][-1][5] = "-1"
        self.assertEqual(evaluate(data, RECEIVED)["direction"], "UNAVAILABLE")

    def test_capture_preserves_source_and_never_places_an_order(self):
        class Client:
            def snapshot(self):
                return pair(), RECEIVED

        with tempfile.TemporaryDirectory() as directory:
            db = open_db(Path(directory) / "test.sqlite")
            try:
                self.assertEqual(capture(Client(), db)["direction"], "HIGHER")
                workspace, payload, execution = db.execute(
                    "SELECT workspace,payload,execution FROM observations").fetchone()
                self.assertEqual(workspace, RULES["source_id"])
                self.assertEqual(set(json.loads(payload)), {"five_min", "one_min"})
                self.assertEqual(execution, "NO TRADE")
            finally:
                db.close()


if __name__ == "__main__":
    unittest.main()
