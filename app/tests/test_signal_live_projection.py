"""The record and live views share signal IDs and never call a proxy an exchange fill."""
import json
from pathlib import Path
import sqlite3
import tempfile
import unittest

from shadow.web import build_chart_observations, enrich_live_attempts


class SignalLiveProjectionTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        root = Path(self.temp.name)
        self.chart = root / "chart.sqlite"
        self.ticks = root / "shadow.sqlite"
        with sqlite3.connect(self.chart) as db:
            db.execute("CREATE TABLE observations(id INTEGER PRIMARY KEY,received_ms INTEGER,decision TEXT,payload TEXT,entry_ms INTEGER,expiry_ms INTEGER)")
            db.execute("CREATE TABLE proxy_results(observation_id INTEGER PRIMARY KEY,status TEXT,entry_price TEXT,exit_price TEXT,direction_result TEXT)")
            for ident, direction in ((7, "LOWER"), (8, "HIGHER")):
                db.execute("INSERT INTO observations VALUES(?,?,?,?,?,?)",
                           (ident, 2000, json.dumps({"direction": direction,
                                                    "reason": "BOTH_UPPER_TOUCHED" if ident == 7 else "BOTH_LOWER_TOUCHED"}),
                            json.dumps({"five_min": {"truncated": ident == 7},
                                        "one_min": {"truncated": ident == 7}}), 3000, 1803000))
            db.execute("INSERT INTO proxy_results VALUES(7,'VERIFIED','100.49','90.49','LOWER')")
            db.execute("INSERT INTO proxy_results VALUES(8,'PENDING','101',NULL,NULL)")
        with sqlite3.connect(self.ticks) as db:
            db.execute("CREATE TABLE ticks(event_ms INTEGER,received_ms INTEGER,index_price TEXT,mark_price TEXT)")
            db.executemany("INSERT INTO ticks VALUES(?,?,?,?)", [
                (3100, 3150, "100.49", "100"),
                (3900, 3950, "102.50", "102"),
                (4001, 4002, "999", "999"),
            ])

    def tearDown(self):
        self.temp.cleanup()

    def test_same_signal_id_links_both_views_and_keeps_proxy_label(self):
        state = {"attempts": [{"observation_id": 7, "created_ms": 4000,
                                "direction": "SHORT", "amount": "5",
                                "state": "TIME_EXPIRED_UNVERIFIED", "bridge_attempt_id": "b" * 32}]}
        enrich_live_attempts(state, self.chart, self.ticks, 5000)
        attempt = state["attempts"][0]
        self.assertEqual(attempt["request_index_price_display"], "103")
        self.assertEqual(attempt["request_index_event_ms"], 3900)
        self.assertEqual(attempt["proxy_result_label"], "赢")
        self.assertEqual(attempt["proxy_exit_price_display"], "90")
        self.assertEqual(attempt["state"], "TIME_EXPIRED_UNVERIFIED")
        self.assertEqual(attempt["indicator_history_status"], "TRUNCATED")
        records = build_chart_observations(self.chart, attempts_by_observation={7: attempt})
        self.assertEqual([r["observation_id"] for r in records], [8, 7])
        self.assertEqual(records[1]["request_index_price_display"], "103")
        self.assertEqual(records[1]["live_request_state"], "TIME_EXPIRED_UNVERIFIED")
        self.assertEqual(records[1]["indicator_history_status"], "TRUNCATED")
        self.assertIsNone(records[0]["live_request_state"])

    def test_missing_or_future_tick_does_not_invent_request_price(self):
        state = {"attempts": [
            {"observation_id": 7, "created_ms": 7000, "state": "API_ACCEPTED_UNVERIFIED", "bridge_attempt_id": "b" * 32},
            {"observation_id": 8, "created_ms": 4000, "state": "SKIPPED", "bridge_attempt_id": None},
        ]}
        enrich_live_attempts(state, self.chart, self.ticks, 2000)
        self.assertIsNone(state["attempts"][0]["request_index_price_display"])
        self.assertIsNone(state["attempts"][1]["request_index_price_display"])

    def test_corrupt_signal_does_not_hide_live_request(self):
        with sqlite3.connect(self.chart) as db:
            db.execute("UPDATE observations SET decision='invalid' WHERE id=7")
        state = {"attempts": [{"observation_id": 7, "created_ms": 4000,
                                "state": "API_ACCEPTED_UNVERIFIED", "bridge_attempt_id": "b" * 32}]}
        enrich_live_attempts(state, self.chart, self.ticks, 5000)
        self.assertEqual(state["attempts"][0]["proxy_result_label"], "信号数据异常")
        self.assertEqual(state["attempts"][0]["request_index_price_display"], "103")


if __name__ == "__main__":
    unittest.main()
