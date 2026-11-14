import json
import sqlite3
from pathlib import Path
import tempfile
import unittest

from shadow.live_api import LiveAPIError, change_live_state, change_live_state_from_site, change_signal_periods_from_site, read_live_state
from shadow.live_control import LiveControl


class LiveAPITests(unittest.TestCase):
    def test_site_can_arm_custom_proxy_ladder_without_browser_secret(self):
        class Bridge:
            def status(self):
                return {"history": [], "parameter_mode": "configured", "periods": {"30": True}}
        body = {"action": "enable", "strategy": "bb_dual_touch_30m_intrabar_v2",
                "horizon": 30, "amount": "6.5", "ladder_stakes": ["6.5", "9", "18"]}
        result = change_live_state_from_site(self.config, body, now_ms=1000,
                                             bridge_factory=lambda *_: Bridge())
        self.assertEqual(result["ladder"]["stakes"], ["6.5", "9", "18"])
        self.assertEqual(result["ladder"]["next_amount"], "6.5")
        self.assertTrue(result["ladder"]["enabled"])
        self.assertNotIn("a" * 64, json.dumps(result))

    def test_period_selection_persists_and_cannot_change_while_enabled(self):
        result = change_signal_periods_from_site(self.config, ["1h", "5m", "1m"])
        self.assertEqual(result["signal_periods"], ["1m", "5m", "1h"])
        self.assertEqual(read_live_state(self.config)["signal_periods"], ["1m", "5m", "1h"])
        with LiveControl(Path(self.tmp.name) / "live.sqlite") as control:
            control.enable(strategy="bb_multi_touch_intrabar_v3", horizon=30, amount="5",
                           max_amount="30", current_observation_id=1, now_ms=1)
        with self.assertRaisesRegex(LiveAPIError, "DISABLE_BEFORE_CHANGING_PERIODS"):
            change_signal_periods_from_site(self.config, ["1m", "5m"])

    def test_stale_multi_period_enable_is_rejected_before_bridge_use(self):
        change_signal_periods_from_site(self.config, ["1m", "15m"])
        body = {"action": "enable", "strategy": "bb_multi_touch_intrabar_v3",
                "signal_periods": ["1m", "5m"], "horizon": 30,
                "amount": "5", "authorization": "a" * 64}
        with self.assertRaisesRegex(LiveAPIError, "SIGNAL_PERIODS_CHANGED"):
            change_live_state(self.config, body, now_ms=1000,
                              bridge_factory=lambda *_: self.fail("bridge called"))

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        root = Path(self.tmp.name)
        self.chart = root / "chart.sqlite"
        with sqlite3.connect(self.chart) as db:
            db.execute("CREATE TABLE observations(id INTEGER PRIMARY KEY)")
            db.executemany("INSERT INTO observations VALUES(?)", [(1,), (2,)])
        self.key = root / "bridge.key"
        self.key.write_text("a" * 64)
        self.key.chmod(0o600)
        self.config = root / "live.json"
        self.config.write_text(json.dumps({"control_db": str(root / "live.sqlite"),
                                           "chart_db": str(self.chart),
                                           "bridge_key_file": str(self.key),
                                           "max_amount": "30",
                                           "public_origin": "https://panel.example"}))
        self.config.chmod(0o600)

    def test_enable_requires_second_secret_and_starts_after_latest_observation(self):
        with self.assertRaises(LiveAPIError):
            change_live_state(self.config, {"action": "enable", "strategy": "bb_dual_touch_30m_v1",
                                            "horizon": 30, "amount": "5", "authorization": "wrong"},
                              now_ms=1000)
        class Bridge:
            def status(self):
                return {"history": [], "parameter_mode": "configured", "periods": {"30": True}}
        result = change_live_state(self.config,
                                   {"action": "enable", "strategy": "bb_dual_touch_30m_v1",
                                    "horizon": 30, "amount": "5", "authorization": "a" * 64},
                                   now_ms=1000, bridge_factory=lambda *_: Bridge())
        self.assertEqual(result["enabled"], True)
        with LiveControl(Path(self.tmp.name) / "live.sqlite") as control:
            self.assertEqual(control.state()["cursor"], 2)
        self.assertNotIn("a" * 64, json.dumps(read_live_state(self.config)))

    def test_logged_in_site_can_enable_without_browser_secret(self):
        class Bridge:
            def __init__(self, url, key):
                self.key = key
            def status(self):
                self.was_called = True
                return {"history": [], "parameter_mode": "configured", "periods": {"30": True}}
        seen = []
        def factory(url, key):
            seen.append(key)
            return Bridge(url, key)
        body = {"action": "enable", "strategy": "bb_dual_touch_30m_v1",
                "horizon": 30, "amount": "5"}
        with self.assertRaisesRegex(LiveAPIError, "INVALID_LIVE_REQUEST"):
            change_live_state_from_site(self.config,
                                        body | {"authorization": "browser-supplied"},
                                        now_ms=1000, bridge_factory=factory)
        result = change_live_state_from_site(self.config, body, now_ms=1000,
                                             bridge_factory=factory)
        self.assertTrue(result["enabled"])
        self.assertEqual(seen, ["a" * 64])
        self.assertNotIn("a" * 64, json.dumps(result))

    def test_only_30m_and_amount_cap_and_no_unchecked_bridge_attempt(self):
        class PendingBridge:
            def status(self):
                return {"history": [{"acknowledged_ms": None}],
                        "parameter_mode": "configured", "periods": {"30": True}}
        for horizon, amount in ((60, "5"), (30, "31")):
            with self.assertRaises(LiveAPIError):
                change_live_state(self.config,
                                  {"action": "enable", "strategy": "bb_dual_touch_30m_v1",
                                   "horizon": horizon, "amount": amount,
                                   "authorization": "a" * 64},
                                  now_ms=1000, bridge_factory=lambda *_: PendingBridge())
        with self.assertRaises(LiveAPIError):
            change_live_state(self.config,
                              {"action": "enable", "strategy": "bb_dual_touch_30m_v1",
                               "horizon": 30, "amount": "5", "authorization": "a" * 64},
                              now_ms=1000, bridge_factory=lambda *_: PendingBridge())
        self.assertFalse(read_live_state(self.config)["enabled"])

    def test_all_periods_are_stored_in_isolated_control(self):
        class Bridge:
            def status(self):
                return {"history": [], "parameter_mode": "configured", "periods": {"10": True, "30": True, "60": True}}
        for horizon in (10, 30, 60):
            with self.subTest(horizon=horizon):
                body = {"action": "enable", "strategy": "bb_dual_touch_30m_intrabar_v2", "horizon": horizon, "amount": "5"}
                state = change_live_state_from_site(self.config, body, now_ms=1000, bridge_factory=lambda *_: Bridge())
                self.assertEqual(state["horizon"], horizon)
                change_live_state(self.config, {"action": "disable"}, now_ms=1001)

    def test_site_reverse_choice_is_persisted(self):
        class Bridge:
            def status(self):
                return {"history": [], "parameter_mode": "configured", "periods": {"10": True}}
        body = {"action": "enable", "strategy": "bb_dual_touch_30m_intrabar_v2",
                "horizon": 10, "amount": "5", "reverse": True}
        result = change_live_state_from_site(self.config, body, now_ms=1000,
                                             bridge_factory=lambda *_: Bridge())
        self.assertTrue(result["reverse"])
        with LiveControl(Path(self.tmp.name) / "live.sqlite") as control:
            self.assertEqual(control.state()["reverse"], 1)

    def test_site_cooldown_choice_is_persisted(self):
        class Bridge:
            def status(self):
                return {"history": [], "parameter_mode": "configured", "periods": {"30": True}}
        body = {"action": "enable", "strategy": "bb_dual_touch_30m_intrabar_v2",
                "horizon": 30, "amount": "5", "reverse": True, "cooldown_five_minutes": True}
        result = change_live_state_from_site(self.config, body, now_ms=1000,
                                             bridge_factory=lambda *_: Bridge())
        self.assertTrue(result["cooldown_five_minutes"])
        self.assertTrue(result["reverse"])
        with LiveControl(Path(self.tmp.name) / "live.sqlite") as control:
            self.assertEqual(control.state()["cooldown_five_minutes"], 1)

    def test_site_direction_mode_is_persisted(self):
        class Bridge:
            def status(self):
                return {"history": [], "parameter_mode": "configured", "periods": {"30": True}}
        body = {"action": "enable", "strategy": "bb_dual_touch_30m_intrabar_v2",
                "horizon": 30, "amount": "5", "direction_mode": "SHORT_ONLY",
                "cooldown_five_minutes": True}
        result = change_live_state_from_site(self.config, body, now_ms=1000,
                                             bridge_factory=lambda *_: Bridge())
        self.assertEqual(result["direction_mode"], "SHORT_ONLY")
        self.assertTrue(result["cooldown_five_minutes"])

    def test_disable_does_not_need_second_secret(self):
        change_live_state(self.config, {"action": "disable"}, now_ms=1000)
        self.assertFalse(read_live_state(self.config)["enabled"])

    def test_paused_accepted_request_allows_new_settings_without_erasing_old_attempt(self):
        from test_live_intrabar import intrabar_signal, V2
        signal = intrabar_signal()
        with LiveControl(Path(self.tmp.name) / "live.sqlite") as control:
            control.enable(strategy=V2, horizon=60, amount="5", max_amount="30",
                           current_observation_id=0, now_ms=signal["entry_ms"] - 1,
                           cooldown_five_minutes=True)
            attempt = control.reserve(signal, signal["entry_ms"] + 1000)
            control.mark_submitting(attempt["id"], signal["entry_ms"] + 1000, "c" * 32)
            control.finish(attempt["id"], signal["entry_ms"] + 1000,
                           "API_ACCEPTED_UNVERIFIED", "c" * 32)
        change_live_state(self.config, {"action": "disable"}, now_ms=signal["entry_ms"] + 2000)

        class Bridge:
            def status(self):
                return {"history": [{"id": "c" * 32, "status": "ACCEPTED_UNVERIFIED",
                                     "acknowledged_ms": None, "auto_released_ms": None}],
                        "parameter_mode": "configured", "periods": {"10": True}}
        result = change_live_state_from_site(
            self.config, {"action": "enable", "strategy": V2, "horizon": 10,
                          "amount": "10", "direction_mode": "SHORT_ONLY"},
            now_ms=signal["entry_ms"] + 3000, bridge_factory=lambda *_: Bridge())
        self.assertTrue(result["enabled"])
        self.assertEqual(result["horizon"], 10)
        self.assertEqual(result["attempts"][0]["release_buffer_ms"], 300000)
        self.assertEqual(result["attempts"][0]["expiry_ms"], attempt["expiry_ms"])

    def test_review_requires_user_check_and_bridge_ack(self):
        with LiveControl(Path(self.tmp.name) / "live.sqlite") as control:
            control.enable(strategy="bb_dual_touch_30m_v1", horizon=30, amount="5",
                           max_amount="30", current_observation_id=2, now_ms=1000)
            from test_live_control import signal
            entry = signal(3)["entry_ms"]
            attempt = control.reserve(signal(3), entry + 500)
            control.mark_submitting(attempt["id"], entry + 500, "c" * 32)
            control.finish(attempt["id"], entry + 600, "API_ACCEPTED_UNVERIFIED", "c" * 32)
        class Bridge:
            calls = []
            def status(self):
                return {"history": [{"id": "c" * 32, "status": "ACCEPTED_UNVERIFIED",
                                     "acknowledged_ms": None}]}
            def acknowledge(self, attempt_id):
                self.calls.append(attempt_id)
                return {"acknowledged": True}
        body = {"action": "review", "attempt_id": attempt["id"],
                "checked_in_binance": True}
        with self.assertRaises(LiveAPIError):
            change_live_state_from_site(self.config, body | {"checked_in_binance": False},
                                        now_ms=entry + 1000, bridge_factory=lambda *_: Bridge())
        result = change_live_state_from_site(self.config, body, now_ms=entry + 1000,
                                             bridge_factory=lambda *_: Bridge())
        self.assertEqual(Bridge.calls, ["c" * 32])
        self.assertEqual(result["attempts"][0]["state"], "USER_REVIEWED_UNVERIFIED")


if __name__ == "__main__":
    unittest.main()
