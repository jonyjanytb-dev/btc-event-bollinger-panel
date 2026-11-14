import tempfile
import sqlite3
import unittest
from pathlib import Path

from shadow.live_control import LiveControl, LiveControlError
from shadow.live_worker import run_tick
from test_live_intrabar import intrabar_signal, V2


class LadderLiveTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name) / "live.sqlite"
        self.base = 1_800_000_130_000

    def arm(self, control, stakes=["5", "7", "15"], at=None):
        control.enable(strategy=V2, horizon=30, amount=stakes[0], max_amount="200",
                       current_observation_id=100, now_ms=at or self.base - 1,
                       ladder_stakes=stakes)

    def reserve(self, control, ident, at):
        row = intrabar_signal(now=at, ident=ident,
                              direction="HIGHER" if ident % 2 else "LOWER")
        return control.reserve(row, at + 1_000)

    def result(self, control, attempt, outcome, resolved):
        control.mark_submitting(attempt["id"], attempt["created_ms"]+1, "a"*32)
        control.finish(attempt["id"], attempt["created_ms"]+2,
                       "API_ACCEPTED_UNVERIFIED", "a"*32)
        control.release_accepted(attempt["id"], attempt["expiry_ms"]+60_000,
                                 "a"*32, buffer_ms=60_000)
        control.db.execute("INSERT INTO live_proxy_results VALUES(?,?,?,?,?,?,?,?)",
                           (attempt["id"], "VERIFIED" if outcome in ("WIN", "LOSS") else outcome,
                            "100", "101", outcome if outcome in ("WIN", "LOSS") else None,
                            attempt["created_ms"], attempt["expiry_ms"], resolved))

    def test_custom_amounts_follow_losses_across_directions_and_reset(self):
        with LiveControl(self.path) as c:
            self.arm(c, ["6", "9", "18"])
            for step, ident, amount in ((0,101,"6"),(1,102,"9"),(2,103,"18"),(0,104,"6")):
                at = self.base + (ident-101)*3_600_000
                a = self.reserve(c, ident, at)
                self.assertEqual(a["amount"], amount)
                self.assertEqual(c.ladder_state()["step"], step)
                self.result(c, a, "LOSS", at+1_800_000)
                self.assertTrue(c.advance_ladder(at+1_800_001))
                self.assertFalse(c.advance_ladder(at+1_800_002))
            self.assertEqual(c.ladder_state()["step"], 1)

    def test_win_resets_and_pending_or_missing_blocks_next_signal(self):
        with LiveControl(self.path) as c:
            self.arm(c)
            first = self.reserve(c, 101, self.base)
            c.mark_submitting(first["id"], first["created_ms"]+1, "a"*32)
            c.finish(first["id"], first["created_ms"]+2,
                     "API_ACCEPTED_UNVERIFIED", "a"*32)
            c.release_accepted(first["id"], first["expiry_ms"]+60_000,
                               "a"*32, buffer_ms=60_000)
            second = self.reserve(c, 102, self.base+3_600_000)
            self.assertIsNone(second)
            self.assertEqual(c.ladder_state()["step"], 0)
            c.db.execute("INSERT INTO live_proxy_results VALUES(?,?,?,?,?,?,?,?)",
                         (first["id"], "VERIFIED", "100", "101", "WIN",
                          first["created_ms"], first["expiry_ms"], self.base+1_800_000))
            self.assertTrue(c.advance_ladder(self.base+1_800_001))
            self.assertEqual(c.ladder_state()["step"], 0)

    def test_restart_retains_stage_and_applies_result_once(self):
        with LiveControl(self.path) as c:
            self.arm(c)
            first = self.reserve(c, 101, self.base)
            self.result(c, first, "LOSS", self.base+1_800_000)
        with LiveControl(self.path) as c:
            self.assertTrue(c.advance_ladder(self.base+1_800_001))
            self.assertEqual(c.ladder_state()["step"], 1)
        with LiveControl(self.path) as c:
            self.assertFalse(c.advance_ladder(self.base+1_800_002))
            self.assertEqual(c.ladder_state()["step"], 1)

    def test_pause_and_rearm_preserve_unsettled_attempt_and_stage(self):
        with LiveControl(self.path) as c:
            self.arm(c)
            first = self.reserve(c, 101, self.base)
            self.result(c, first, "LOSS", self.base+1_800_000)
            self.assertTrue(c.advance_ladder(self.base+1_800_001))
            second = self.reserve(c, 102, self.base+3_600_000)
            self.assertEqual(second["amount"], "7")
            c.mark_submitting(second["id"], second["created_ms"]+1, "a"*32)
            c.finish(second["id"], second["created_ms"]+2,
                     "API_ACCEPTED_UNVERIFIED", "a"*32)
            c.disable(self.base+3_600_001)
            self.arm(c, at=self.base+3_600_002)
            self.assertEqual(c.ladder_state()["step"], 1)
            self.assertTrue(c.ladder_state()["waiting_for_result"])
            c.release_accepted(second["id"], second["expiry_ms"]+60_000,
                               "a"*32, buffer_ms=60_000)
            c.db.execute("INSERT INTO live_proxy_results VALUES(?,?,?,?,?,?,?,?)",
                         (second["id"], "VERIFIED", "100", "101", "WIN",
                          second["created_ms"], second["expiry_ms"], self.base+5_400_000))
            self.assertTrue(c.advance_ladder(self.base+5_400_001))
            self.assertEqual(c.ladder_state()["step"], 0)

    def test_equal_refund_resets_and_invalid_stakes_rejected(self):
        with LiveControl(self.path) as c:
            with self.assertRaises(LiveControlError):
                self.arm(c, ["5", "7", "201"])
            self.arm(c)
            first = self.reserve(c, 101, self.base)
            self.result(c, first, "EQUAL", self.base+1_800_000)
            self.assertTrue(c.advance_ladder(self.base+1_800_001))
            self.assertEqual(c.ladder_state()["step"], 0)

    def test_existing_database_without_ladder_keeps_fixed_amount(self):
        with LiveControl(self.path) as c:
            c.enable(strategy=V2, horizon=30, amount="12", max_amount="200",
                     current_observation_id=100, now_ms=self.base-1)
            self.assertFalse(c.ladder_state()["enabled"])
            self.assertEqual(self.reserve(c,101,self.base)["amount"], "12")

    def test_worker_releases_then_advances_stage_from_proxy_result(self):
        chart = Path(self.temp.name) / "chart.sqlite"
        with sqlite3.connect(chart) as db:
            db.execute("CREATE TABLE observations(id INTEGER PRIMARY KEY,received_ms INTEGER,"
                       "entry_ms INTEGER,expiry_ms INTEGER,decision TEXT,execution TEXT)")
        class Bridge:
            def release_expired(self, ident):
                return {"id": ident, "status": "ACCEPTED_UNVERIFIED", "auto_released_ms": 1}
        with LiveControl(self.path) as c:
            self.arm(c)
            a = self.reserve(c, 101, self.base)
            c.mark_submitting(a["id"], a["created_ms"]+1, "a"*32)
            c.finish(a["id"], a["created_ms"]+2, "API_ACCEPTED_UNVERIFIED", "a"*32)
            c.db.execute("INSERT INTO live_proxy_results VALUES(?,?,?,?,?,?,?,?)",
                         (a["id"], "VERIFIED", "100", "99", "LOSS",
                          a["created_ms"], a["expiry_ms"], a["expiry_ms"]+5_000))
            run_tick(c, chart, Bridge(), a["expiry_ms"]+60_000)
            self.assertEqual(c.ladder_state()["step"], 1)
            self.assertFalse(c.ladder_state()["waiting_for_result"])


if __name__ == "__main__":
    unittest.main()
