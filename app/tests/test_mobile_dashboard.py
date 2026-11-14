import http.client
import threading
import unittest

from shadow.web import Handler, LocalServer


class MobileDashboardDeliveryTests(unittest.TestCase):
    def test_pages_are_separate_and_live_controls_default_unavailable(self):
        server = LocalServer(("127.0.0.1", 0), Handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            connection = http.client.HTTPConnection("127.0.0.1", server.server_port, timeout=3)
            for path in ("/", "/records", "/statistics", "/live"):
                connection.request("GET", path)
                response = connection.getresponse()
                html = response.read().decode()
                self.assertEqual(response.status, 200, path)
                self.assertIn('href="/records"', html)
                self.assertIn('href="/statistics"', html)
                self.assertIn('href="/live"', html)
                self.assertIn('id="live-strategy"', html)
                self.assertIn('id="live-stake-mode"', html)
                self.assertIn('仅按指数代理结果推进', html)
                self.assertIn('id="actual-request-count"', html)
                self.assertIn('id="live-review"', html)
                self.assertNotIn('真实交易未启用', html)
                self.assertIn('id="live-footer-status"', html)
                self.assertIn('请求时指数价', html)
                self.assertIn('id="chart-strategy-history"', html)
                self.assertIn('["API_ACCEPTED_UNVERIFIED","REJECTED_UNVERIFIED","UNKNOWN"].includes(r.state)', html)
            connection.request("POST", "/api/live", "{}", {"Content-Type": "application/json"})
            response = connection.getresponse()
            self.assertEqual(response.status, 404)
            response.read()
            connection.close()
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=2)

    def test_mobile_styles_are_served_with_dashboard(self):
        server = LocalServer(("127.0.0.1", 0), Handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            connection = http.client.HTTPConnection("127.0.0.1", server.server_port, timeout=3)
            connection.request("GET", "/")
            page = connection.getresponse()
            html = page.read()
            self.assertEqual(page.status, 200)
            self.assertTrue(b'/static/dashboard-mobile.css' in html)
            connection.request("GET", "/static/dashboard-mobile.css")
            stylesheet = connection.getresponse()
            body = stylesheet.read()
            self.assertEqual(stylesheet.status, 200)
            self.assertEqual(stylesheet.getheader("Content-Type"), "text/css; charset=utf-8")
            self.assertTrue(body)
            connection.close()
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=2)


if __name__ == "__main__":
    unittest.main()
