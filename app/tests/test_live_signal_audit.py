"""Durable evidence for live signals that do not produce order attempts."""
import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

from shadow.live_control import LiveControl
from shadow.live_worker import run_tick


STRATEGY = "bb_multi_touch_intrabar_v3"


def signal(ident=1212, received_ms=1_794_432_812_555):
    periods = ("1m", "5m", "10m")
    period_ms = {"1m": 60_000, "5m": 300_000, "10m": 600_000}
    opens = {p: received_ms // period_ms[p] * period_ms[p] for p in periods}
    return {
        "id": ident, "received_ms": received_ms, "entry_ms": received_ms,
        "expiry_ms": received_ms + 1_800_000, "execution": "NO TRADE",
        "decision": json.dumps({
            "direction": "LOWER", "reason": "BOTH_UPPER_TOUCHED_INTRABAR",
            "bar_open_ms": opens["10m"], "bar_close_ms": opens["10m"] + 600_000,
            "one_open_ms": opens["1m"], "periods": list(periods),
            "period_open_ms": opens, "entry_ms": received_ms,
            "expiry_ms": received_ms + 1_800_000,
            "strategy_version": STRATEGY, "source": "BINANCE_FUTURES_KLINES",
        }),
    }


class LiveSignalAuditTests(unittest.TestCase):
    def test_fresh_enabled_signal_still_submits_through_fake_bridge_once(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "live.sqlite"
            chart = Path(directory) / "chart.sqlite"
            row = signal()
            with sqlite3.connect(chart) as db:
                db.execute("CREATE TABLE observations(id INTEGER PRIMARY KEY,received_ms INTEGER,"
                           "entry_ms INTEGER,expiry_ms INTEGER,decision TEXT,execution TEXT)")
                db.execute("INSERT INTO observations VALUES(?,?,?,?,?,?)",
                           (row["id"], row["received_ms"], row["entry_ms"],
                            row["expiry_ms"], row["decision"], row["execution"]))

            class FakeBridge:
                submits = 0
                def status(self): return {"history": []}
                def quote(self, minutes, direction):
                    return {"symbol": "BTCUSDT", "minutes": minutes, "direction": direction,
                            "enum": "ONE_HOUR", "ratio": "0.85"}
                def preview(self, minutes, direction, amount):
                    return {"id": "a" * 32, "amount": amount}
                def submit(self, ident):
                    self.submits += 1
                    return {"id": ident, "status": "ACCEPTED_UNVERIFIED"}

            with LiveControl(path) as control:
                control.enable(strategy=STRATEGY, horizon=60, amount="5",
                               max_amount="200", current_observation_id=row["id"] - 1,
                               now_ms=row["received_ms"] - 1)
                bridge = FakeBridge()
                result = run_tick(control, chart, bridge, row["received_ms"] + 1_000,
                                  clock=lambda: row["received_ms"] + 2_000)
                self.assertEqual([r["state"] for r in result], ["API_ACCEPTED_UNVERIFIED"])
                self.assertEqual(bridge.submits, 1)
                self.assertEqual([e["code"] for e in control.signal_events()
                                  if e["observation_id"] == row["id"]], ["RESERVED"])

    def test_signal_observed_while_disabled_is_audited_without_order_or_replay(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "live.sqlite"
            chart = Path(directory) / "chart.sqlite"
            row = signal()
            with sqlite3.connect(chart) as db:
                db.execute("CREATE TABLE observations(id INTEGER PRIMARY KEY,received_ms INTEGER,"
                           "entry_ms INTEGER,expiry_ms INTEGER,decision TEXT,execution TEXT)")
                db.execute("INSERT INTO observations VALUES(?,?,?,?,?,?)",
                           (row["id"], row["received_ms"], row["entry_ms"],
                            row["expiry_ms"], row["decision"], row["execution"]))

            class NeverCalledBridge:
                def __getattr__(self, name):
                    raise AssertionError("disabled signal must never call bridge")

            with LiveControl(path) as control:
                control.enable(strategy=STRATEGY, horizon=60, amount="5",
                               max_amount="200", current_observation_id=row["id"] - 1,
                               now_ms=row["received_ms"] - 2_000)
                control.disable(row["received_ms"] - 1_000)
                run_tick(control, chart, NeverCalledBridge(), row["received_ms"] + 1_000)
                run_tick(control, chart, NeverCalledBridge(), row["received_ms"] + 2_000)
                events = [e for e in control.signal_events() if e["observation_id"] == row["id"]]
                self.assertEqual(len(events), 1)
                self.assertEqual(events[0]["code"], "DISABLED_SIGNAL")
                self.assertEqual(control.attempts(), [])
                control.enable(strategy=STRATEGY, horizon=60, amount="5",
                               max_amount="200", current_observation_id=row["id"],
                               now_ms=row["received_ms"] + 3_000)
                run_tick(control, chart, NeverCalledBridge(), row["received_ms"] + 4_000)
                self.assertEqual(control.attempts(), [])

    def test_arming_watermark_and_no_replay_survive_restart(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "live.sqlite"
            row = signal()
            with LiveControl(path) as control:
                control.enable(strategy=STRATEGY, horizon=60, amount="5",
                               max_amount="200", current_observation_id=row["id"],
                               now_ms=row["received_ms"] + 2_000)
                self.assertIsNone(control.reserve(row, row["received_ms"] + 3_000))
            with LiveControl(path) as control:
                events = control.signal_events()
                self.assertEqual([(e["code"], e["observation_id"], e["cursor"])
                                  for e in events], [("ENABLED", None, row["id"])])
                self.assertEqual(control.attempts(), [])

    def test_expired_signal_is_audited_once_without_attempt(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "live.sqlite"
            row = signal()
            with LiveControl(path) as control:
                control.enable(strategy=STRATEGY, horizon=60, amount="5",
                               max_amount="200", current_observation_id=row["id"] - 1,
                               now_ms=row["received_ms"] - 1)
                self.assertIsNone(control.reserve(row, row["entry_ms"] + 5_001))
                self.assertIsNone(control.reserve(row, row["entry_ms"] + 6_000))
                self.assertEqual(control.attempts(), [])
                events = [e for e in control.signal_events() if e["observation_id"] == row["id"]]
                self.assertEqual(len(events), 1)
                self.assertEqual(events[0]["code"], "SIGNAL_EXPIRED")
                self.assertEqual(events[0]["processing_lag_ms"], 5_001)

    def test_strategy_mismatch_and_direction_filter_are_audited(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "live.sqlite"
            first, second = signal(ident=1212), signal(ident=1213, received_ms=1_794_432_813_555)
            with LiveControl(path) as control:
                control.enable(strategy="bb_dual_touch_30m_intrabar_v2", horizon=30,
                               amount="5", max_amount="200", current_observation_id=1211,
                               now_ms=first["entry_ms"] - 1)
                self.assertIsNone(control.reserve(first, first["entry_ms"] + 1_000))
                control.disable(first["entry_ms"] + 1_001)
                control.enable(strategy=STRATEGY, horizon=30, amount="5", max_amount="200",
                               current_observation_id=1212, now_ms=first["entry_ms"] + 1_002,
                               direction_mode="LONG_ONLY")
                self.assertIsNone(control.reserve(second, second["entry_ms"] + 1_000))
                events = [e for e in control.signal_events()
                          if e["observation_id"] in (1212, 1213)]
                self.assertEqual([(e["observation_id"], e["code"]) for e in events],
                                 [(1212, "STRATEGY_MISMATCH"), (1213, "DIRECTION_FILTERED")])

    def test_existing_v6_database_gets_audit_table_without_changing_config(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "live.sqlite"
            with LiveControl(path) as control:
                control.enable(strategy=STRATEGY, horizon=60, amount="5",
                               max_amount="200", current_observation_id=1211, now_ms=123)
                before = control.state().copy()
            with sqlite3.connect(path) as db:
                db.execute("DROP TABLE live_signal_events")
                self.assertEqual(db.execute("PRAGMA user_version").fetchone()[0], 6)
            with LiveControl(path) as control:
                self.assertEqual(control.state(), before)
                self.assertEqual(control.signal_events(), [])


if __name__ == "__main__":
    unittest.main()
