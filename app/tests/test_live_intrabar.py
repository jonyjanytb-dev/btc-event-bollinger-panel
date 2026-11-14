"""Intrabar live eligibility and strategy migration guards; no bridge calls."""
import json
import tempfile
import unittest
from pathlib import Path

from shadow.live_control import LiveControl, LiveExecutor, eligible_signal


V2 = "bb_dual_touch_30m_intrabar_v2"
V3 = "bb_multi_touch_intrabar_v3"
V1 = "bb_dual_touch_30m_v1"


def intrabar_signal(now=1_800_000_130_000, direction="HIGHER", ident=101):
    five = now // 300_000 * 300_000
    one = now // 60_000 * 60_000
    expiry = now + 1_800_000
    return {"id": ident, "received_ms": now, "entry_ms": now,
            "expiry_ms": expiry, "execution": "NO TRADE",
            "decision": json.dumps({"direction": direction,
                                    "reason": "BOTH_LOWER_TOUCHED_INTRABAR" if direction == "HIGHER" else "BOTH_UPPER_TOUCHED_INTRABAR",
                                    "bar_open_ms": five, "bar_close_ms": five + 300_000,
                                    "one_open_ms": one, "entry_ms": now,
                                    "expiry_ms": expiry, "strategy_version": V2,
                                    "source": "BINANCE_FUTURES_KLINES"})}


class IntrabarLiveTests(unittest.TestCase):
    def test_multi_period_signal_requires_current_exact_periods(self):
        row = intrabar_signal()
        decision = json.loads(row["decision"])
        now = row["entry_ms"]
        periods = ["1m", "5m", "15m"]
        decision.update(strategy_version=V3, periods=periods,
                        period_open_ms={p: now // ms * ms for p, ms in
                                        (("1m", 60000), ("5m", 300000), ("15m", 900000))},
                        bar_open_ms=now // 900000 * 900000,
                        bar_close_ms=(now // 900000 + 1) * 900000)
        row["decision"] = json.dumps(decision)
        self.assertEqual(eligible_signal(row, now + 1000, strategy=V3), "LONG")
        decision["period_open_ms"]["15m"] -= 900000
        row["decision"] = json.dumps(decision)
        self.assertIsNone(eligible_signal(row, now + 1000, strategy=V3))

    def test_fresh_intrabar_candidate_can_be_sent_without_minute_boundary(self):
        row = intrabar_signal()
        self.assertEqual(eligible_signal(row, row["entry_ms"] + 1_000), "LONG")
        short = intrabar_signal(direction="LOWER")
        self.assertEqual(eligible_signal(short, short["entry_ms"] + 1_000), "SHORT")

    def test_late_or_future_or_modified_intrabar_candidate_is_rejected(self):
        row = intrabar_signal()
        self.assertIsNone(eligible_signal(row, row["entry_ms"] - 1))
        self.assertIsNone(eligible_signal(row, row["entry_ms"] + 5_001))
        bad = dict(row)
        decision = json.loads(bad["decision"])
        decision["one_open_ms"] -= 60_000
        bad["decision"] = json.dumps(decision)
        self.assertIsNone(eligible_signal(bad, bad["entry_ms"] + 1_000))

    def test_previous_strategy_arm_does_not_execute_new_strategy(self):
        row = intrabar_signal()
        with tempfile.TemporaryDirectory() as directory:
            with LiveControl(Path(directory) / "live.sqlite") as control:
                control.enable(strategy=V1, horizon=30, amount="5", max_amount="30",
                               current_observation_id=100, now_ms=row["received_ms"] - 1)
                self.assertIsNone(control.reserve(row, row["entry_ms"] + 1_000))
                self.assertEqual(control.attempts(), [])

    def test_new_strategy_arm_reserves_signal_once(self):
        row = intrabar_signal()
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "live.sqlite"
            with LiveControl(path) as control:
                control.enable(strategy=V2, horizon=30, amount="5", max_amount="30",
                               current_observation_id=100, now_ms=row["received_ms"] - 1)
                attempt = control.reserve(row, row["entry_ms"] + 1_000)
                self.assertEqual(attempt["direction"], "LONG")
            with LiveControl(path) as control:
                self.assertIsNone(control.reserve(row, row["entry_ms"] + 2_000))
                self.assertEqual(len(control.attempts()), 1)

    def test_fresh_signal_submits_once_without_waiting_for_next_minute(self):
        row = intrabar_signal()

        class Bridge:
            submits = 0
            def status(self): return {"history": []}
            def quote(self, minutes, direction):
                return {"symbol": "BTCUSDT", "minutes": minutes,
                        "direction": direction, "enum": "THIRTY_MINUTE", "ratio": "0.85"}
            def preview(self, minutes, direction, amount):
                return {"id": "a" * 32, "amount": amount}
            def submit(self, ident):
                self.submits += 1
                return {"id": ident, "status": "ACCEPTED_UNVERIFIED"}

        with tempfile.TemporaryDirectory() as directory:
            with LiveControl(Path(directory) / "live.sqlite") as control:
                control.enable(strategy=V2, horizon=30, amount="5", max_amount="30",
                               current_observation_id=100, now_ms=row["received_ms"] - 1)
                bridge = Bridge()
                result = LiveExecutor(control, bridge,
                                      clock=lambda: row["entry_ms"] + 2_000).process(
                                          row, row["entry_ms"] + 1_000)
                self.assertEqual(result["state"], "API_ACCEPTED_UNVERIFIED")
                self.assertEqual(bridge.submits, 1)
                self.assertIsNone(LiveExecutor(control, bridge).process(
                    row, row["entry_ms"] + 2_000))
                self.assertEqual(bridge.submits, 1)


if __name__ == "__main__":
    unittest.main()
