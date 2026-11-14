import http.client
import json
from pathlib import Path
import sqlite3
import tempfile
import threading
import unittest
from unittest.mock import patch

from shadow.web import HTML, Handler, LocalServer
from shadow.live_control import LiveControl


class LiveHTTPTests(unittest.TestCase):
    def test_live_control_requires_same_origin_but_no_browser_secret(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            chart = root / "chart.sqlite"
            with sqlite3.connect(chart) as db:
                db.execute("CREATE TABLE observations(id INTEGER PRIMARY KEY)")
            key = root / "key"
            key.write_text("a" * 64)
            key.chmod(0o600)
            config = root / "live.json"
            config.write_text(json.dumps({"control_db": str(root / "live.sqlite"),
                                          "chart_db": str(chart), "bridge_key_file": str(key),
                                          "max_amount": "30", "public_origin": "https://panel.example"}))
            config.chmod(0o600)
            with LiveControl(root / "live.sqlite"):
                pass
            with sqlite3.connect(root / "bridge.sqlite") as db:
                db.execute("CREATE TABLE attempts(id TEXT PRIMARY KEY,status TEXT,payload TEXT)")
            (root / "live-statistics.json").write_text(json.dumps({
                "start_ms": 1000, "bridge_ledger": str(root / "bridge.sqlite")}))
            server = LocalServer(("127.0.0.1", 0), Handler)
            server.live_config = config
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            try:
                connection = http.client.HTTPConnection("127.0.0.1", server.server_port, timeout=3)
                connection.request("GET", "/api/live")
                response = connection.getresponse()
                self.assertEqual(response.status, 200)
                self.assertEqual(json.loads(response.read())["enabled"], False)
                connection.request("GET", "/api/live-statistics")
                response = connection.getresponse()
                self.assertEqual(response.status, 200)
                self.assertEqual(json.loads(response.read())["requests"], 0)
                self.assertIn('id="live-pnl-chart"', HTML)
                for name in ("live-stake-mode", "live-stake-1", "live-stake-2", "live-stake-3"):
                    self.assertIn('id="' + name + '"', HTML)
                self.assertNotIn('id="live-authorization"', HTML)
                request = json.dumps({"action": "enable", "strategy": "bb_dual_touch_30m_v1",
                                      "horizon": 30, "amount": "5"})
                headers = {"Host": "panel.example", "Origin": "https://evil.example",
                           "Content-Type": "application/json"}
                connection.request("POST", "/api/live", request, headers)
                response = connection.getresponse()
                self.assertEqual(response.status, 403)
                response.read()
                headers["Origin"] = "https://panel.example"
                with patch("shadow.web.change_live_state_from_site",
                           return_value={"enabled": True}) as action:
                    connection.request("POST", "/api/live", request, headers)
                    response = connection.getresponse()
                    self.assertEqual(response.status, 200)
                    self.assertTrue(json.loads(response.read())["enabled"])
                    self.assertEqual(action.call_args.args[1]["amount"], "5")
                    self.assertNotIn("authorization", action.call_args.args[1])
                connection.close()
            finally:
                server.shutdown()
                server.server_close()
                thread.join(timeout=2)


if __name__ == "__main__":
    unittest.main()
