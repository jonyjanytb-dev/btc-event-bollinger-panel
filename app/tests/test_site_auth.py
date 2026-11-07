import http.client
import threading
import unittest

from shadow.site_auth import AuthConfig, AuthHTTPServer, make_config


class SiteAuthTests(unittest.TestCase):
    def setUp(self):
        config = AuthConfig.from_dict(
            make_config("demo", "correct-horse", public_origin="https://panel.example", iterations=1000, max_attempts=2)
        )
        self.server = AuthHTTPServer(("127.0.0.1", 0), config)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=2)

    def request(self, method, path, body=None, headers=None):
        connection = http.client.HTTPConnection("127.0.0.1", self.server.server_port, timeout=3)
        base = {"Host": "panel.example"}
        base.update(headers or {})
        connection.request(method, path, body=body, headers=base)
        response = connection.getresponse()
        result = response.status, dict(response.getheaders()), response.read()
        connection.close()
        return result

    def login(self, password="correct-horse", remember=True):
        body = "username=demo&password=" + password
        if remember:
            body += "&remember=1"
        return self.request(
            "POST", "/login", body,
            {"Origin": "https://panel.example", "Content-Type": "application/x-www-form-urlencoded"},
        )

    def test_login_remembers_device_and_protects_dashboard(self):
        status, headers, _ = self.request("GET", "/verify")
        self.assertEqual((status, headers.get("Location")), (302, "/login"))
        status, headers, _ = self.login()
        self.assertEqual((status, headers.get("Location")), (303, "/"))
        cookie = headers["Set-Cookie"]
        self.assertIn("Max-Age=2592000", cookie)
        self.assertIn("HttpOnly", cookie)
        self.assertIn("Secure", cookie)
        self.assertIn("SameSite=Lax", cookie)
        self.assertEqual(self.request("GET", "/verify", headers={"Cookie": cookie.split(";", 1)[0]})[0], 204)
        self.assertEqual(self.request("GET", "/verify", headers={"Cookie": "shadow_device=invalid"})[0], 302)

    def test_session_only_without_remember(self):
        status, headers, _ = self.login(remember=False)
        self.assertEqual(status, 303)
        self.assertNotIn("Max-Age=", headers["Set-Cookie"])

    def test_bad_password_rate_limited_without_leak(self):
        first = self.login("incorrect")
        second = self.login("incorrect")
        third = self.login("incorrect")
        self.assertEqual([first[0], second[0], third[0]], [401, 401, 429])
        self.assertNotIn(b"correct-horse", first[2])
        self.assertEqual(third[1]["Retry-After"], "900")

    def test_non_ascii_username_is_rejected_cleanly(self):
        status, _, _ = self.request(
            "POST", "/login", "username=%E6%B5%8B%E8%AF%95&password=correct-horse",
            {"Origin": "https://panel.example", "Content-Type": "application/x-www-form-urlencoded"},
        )
        self.assertEqual(status, 401)

    def test_login_rejects_cross_site_post_and_logout_clears_cookie(self):
        status, _, _ = self.request(
            "POST", "/login", "username=demo&password=correct-horse",
            {"Origin": "https://other.example", "Content-Type": "application/x-www-form-urlencoded"},
        )
        self.assertEqual(status, 403)
        _, headers, _ = self.login()
        cookie = headers["Set-Cookie"].split(";", 1)[0]
        status, headers, _ = self.request(
            "POST", "/logout", "",
            {"Origin": "https://panel.example", "Cookie": cookie},
        )
        self.assertEqual((status, headers.get("Location")), (303, "/login"))
        self.assertIn("Max-Age=0", headers["Set-Cookie"])

    def test_mobile_login_page_and_unknown_path(self):
        status, headers, body = self.request("GET", "/login")
        self.assertEqual(status, 200)
        self.assertEqual(headers.get("Referrer-Policy"), "same-origin")
        self.assertIn(b'width=device-width', body)
        self.assertIn(b'name="remember"', body)
        self.assertEqual(self.request("GET", "/not-a-route")[0], 404)


if __name__ == "__main__":
    unittest.main()
