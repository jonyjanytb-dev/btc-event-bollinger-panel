import json
import sqlite3
from pathlib import Path
import tempfile
import unittest

from shadow.live_control import LiveControl, LiveControlError, LiveExecutor, eligible_signal
from shadow.live_bridge import BridgeUnavailable


def signal(ident=101, now=1_800_000_010_000):
    entry = ((now // 60_000) + 1) * 60_000
    return {"id": ident, "received_ms": now - 3000, "entry_ms": entry,
            "expiry_ms": entry + 30 * 60_000, "execution": "NO TRADE",
            "decision": json.dumps({"direction": "HIGHER", "reason": "BOTH_LOWER_TOUCHED",
                                    "entry_ms": entry, "expiry_ms": entry + 30 * 60_000,
                                    "bar_close_ms": now - 4000,
                                    "strategy_version": "bb_dual_touch_30m_v1"})}


class LiveControlTests(unittest.TestCase):
    def test_only_fresh_matching_closed_bar_signal_can_prepare(self):
        now = 1_800_000_010_000
        entry = signal(now=now)["entry_ms"]
        self.assertIsNone(eligible_signal(signal(now=now), now))
        self.assertEqual(eligible_signal(signal(now=now), entry + 500), "LONG")
        stale = signal(now=now)
        self.assertIsNone(eligible_signal(stale, stale["entry_ms"] + 3000))
        wrong = signal(now=now)
        wrong["expiry_ms"] += 30 * 60_000
        self.assertIsNone(eligible_signal(wrong, entry + 500))
        late_data = signal(now=now)
        late_data["received_ms"] = late_data["entry_ms"] + 1
        self.assertIsNone(eligible_signal(late_data, entry + 500))

    def test_reservation_survives_restart_and_blocks_duplicate(self):
        now = 1_800_000_010_000
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "live.sqlite"
            with LiveControl(path) as control:
                control.enable(strategy="bb_dual_touch_30m_v1", horizon=30,
                               amount="5", max_amount="30", current_observation_id=100, now_ms=now)
                self.assertIsNone(control.reserve(signal(100, now), now))
                self.assertIsNone(control.reserve(signal(101, now), now))
                reservation = control.reserve(signal(101, now), signal(now=now)["entry_ms"] + 500)
                self.assertEqual(reservation["state"], "RESERVED")
            with LiveControl(path) as control:
                self.assertIsNone(control.reserve(signal(101, now), signal(now=now)["entry_ms"] + 500))
                self.assertEqual(control.attempts()[0]["observation_id"], 101)

    def test_unknown_outcome_freezes_new_submissions(self):
        now = 1_800_000_010_000
        with tempfile.TemporaryDirectory() as directory:
            with LiveControl(Path(directory) / "live.sqlite") as control:
                control.enable(strategy="bb_dual_touch_30m_v1", horizon=30,
                               amount="5", max_amount="30", current_observation_id=100, now_ms=now)
                first = control.reserve(signal(101, now), signal(now=now)["entry_ms"] + 500)
                control.mark_unknown(first["id"], now + 1)
                with self.assertRaises(LiveControlError):
                    control.reserve(signal(102, now), now + 2)

    def test_restart_marks_incomplete_attempt_unknown_and_disables(self):
        now = 1_800_000_010_000
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "live.sqlite"
            with LiveControl(path) as control:
                control.enable(strategy="bb_dual_touch_30m_v1", horizon=30,
                               amount="5", max_amount="30", current_observation_id=100, now_ms=now)
                control.reserve(signal(101, now), signal(now=now)["entry_ms"] + 500)
            with LiveControl(path) as control:
                self.assertEqual(control.recover_pending(now + 1), 1)
                self.assertEqual(control.attempts()[0]["state"], "UNKNOWN")
                self.assertEqual(control.state()["enabled"], 0)
                self.assertEqual(control.recover_pending(now + 2), 0)

    def test_executor_submits_once_then_stays_armed_until_expiry(self):
        now = 1_800_000_010_000

        class Bridge:
            calls = []

            def status(self):
                self.calls.append("status")
                return {"history": []}

            def quote(self, minutes, direction):
                self.calls.append("quote")
                return {"symbol": "BTCUSDT", "minutes": minutes, "direction": direction,
                        "enum": "THIRTY_MINUTE", "ratio": "0.85",
                        "kind": "configured_parameter", "source": "local_period_parameters"}

            def preview(self, minutes, direction, amount):
                self.calls.append("preview")
                return {"id": "a" * 32, "amount": amount,
                        "quote": self.quote(minutes, direction)}

            def submit(self, preview_id):
                self.calls.append("submit")
                return {"id": preview_id, "status": "ACCEPTED_UNVERIFIED"}

        with tempfile.TemporaryDirectory() as directory:
            with LiveControl(Path(directory) / "live.sqlite") as control:
                control.enable(strategy="bb_dual_touch_30m_v1", horizon=30, amount="5",
                               max_amount="30", current_observation_id=100, now_ms=now)
                bridge = Bridge()
                entry = signal(now=now)["entry_ms"]
                result = LiveExecutor(control, bridge, clock=lambda: entry + 600).process(signal(101, now), entry + 500)
                self.assertEqual(result["state"], "API_ACCEPTED_UNVERIFIED")
                self.assertEqual(result["bridge_attempt_id"], "a" * 32)
                self.assertEqual(bridge.calls.count("submit"), 1)
                self.assertEqual(control.state()["enabled"], 1)
                self.assertIsNone(LiveExecutor(control, bridge).process(signal(101, now), entry + 500))
                self.assertEqual(bridge.calls.count("submit"), 1)
                with self.assertRaises(LiveControlError):
                    control.release_accepted(result["id"], entry + 30 * 60000,
                                             "a" * 32, buffer_ms=60000)
                control.release_accepted(result["id"], entry + 31 * 60000,
                                         "a" * 32, buffer_ms=60000)
                self.assertEqual(control.attempt(result["id"])["state"], "TIME_EXPIRED_UNVERIFIED")
                self.assertEqual(control.state()["enabled"], 1)

    def test_executor_unknown_submit_never_retries_after_restart(self):
        now = 1_800_000_010_000

        class Bridge:
            submits = 0

            def status(self):
                return {"history": []}

            def quote(self, minutes, direction):
                return {"symbol": "BTCUSDT", "minutes": minutes, "direction": direction,
                        "enum": "THIRTY_MINUTE", "ratio": "0.85", "kind": "configured_parameter"}

            def preview(self, minutes, direction, amount):
                return {"id": "b" * 32, "amount": amount}

            def submit(self, preview_id):
                self.submits += 1
                raise BridgeUnavailable("BRIDGE_UNAVAILABLE")

        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "live.sqlite"
            bridge = Bridge()
            with LiveControl(path) as control:
                control.enable(strategy="bb_dual_touch_30m_v1", horizon=30, amount="5",
                               max_amount="30", current_observation_id=100, now_ms=now)
                entry = signal(now=now)["entry_ms"]
                result = LiveExecutor(control, bridge, clock=lambda: entry + 600).process(signal(101, now), entry + 500)
                self.assertEqual(result["state"], "UNKNOWN")
                self.assertEqual(result["bridge_attempt_id"], "b" * 32)
            with LiveControl(path) as control:
                self.assertIsNone(LiveExecutor(control, bridge).process(signal(101, now), now))
                self.assertEqual(bridge.submits, 1)

    def test_mismatched_bridge_receipt_stops_automation(self):
        now = 1_800_000_010_000
        entry = signal(now=now)["entry_ms"]
        class Bridge:
            def status(self): return {"history": []}
            def quote(self, minutes, direction):
                return {"symbol": "BTCUSDT", "minutes": 30, "direction": direction,
                        "enum": "THIRTY_MINUTE", "ratio": "0.85"}
            def preview(self, minutes, direction, amount): return {"id": "a" * 32, "amount": amount}
            def submit(self, preview_id): return {"id": "b" * 32, "status": "ACCEPTED_UNVERIFIED"}
        with tempfile.TemporaryDirectory() as directory:
            with LiveControl(Path(directory) / "live.sqlite") as control:
                control.enable(strategy="bb_dual_touch_30m_v1", horizon=30, amount="5",
                               max_amount="30", current_observation_id=100, now_ms=now)
                result = LiveExecutor(control, Bridge(), clock=lambda: entry + 600).process(signal(101, now), entry + 500)
                self.assertEqual(result["state"], "UNKNOWN")
                self.assertEqual(control.state()["enabled"], 0)

    def test_existing_v1_control_ledger_migrates_without_changing_enable(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "live.sqlite"
            with sqlite3.connect(path) as db:
                db.execute("PRAGMA application_id=1162894410")
                db.execute("PRAGMA user_version=1")
                db.execute("CREATE TABLE config(id INTEGER PRIMARY KEY,enabled INTEGER,strategy TEXT,horizon INTEGER,amount TEXT,cursor INTEGER,updated_ms INTEGER)")
                db.execute("CREATE TABLE attempts(id TEXT PRIMARY KEY,observation_id INTEGER,direction TEXT,amount TEXT,state TEXT,created_ms INTEGER,updated_ms INTEGER,bridge_attempt_id TEXT)")
                db.execute("INSERT INTO config VALUES(1,1,'bb_dual_touch_30m_v1',30,'5',744,1000)")
            with LiveControl(path) as control:
                self.assertEqual(control.state()["enabled"], 1)
                self.assertEqual(control.state()["cursor"], 744)
                self.assertIn("expiry_ms", [r[1] for r in control.db.execute("PRAGMA table_info(attempts)")])


if __name__ == "__main__":
    unittest.main()
