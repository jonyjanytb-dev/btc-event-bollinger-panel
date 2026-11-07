"""Loopback-only login gateway for the SHADOW dashboard and trade controls.

Caddy calls /verify before forwarding dashboard requests. Login cookies are
signed and expire after 30 days when the device is remembered. No trading
credentials or order endpoints are handled here; the private bridge key stays
inside the backend.
"""
from __future__ import annotations

import argparse
import base64
import hashlib
import hmac
import html
import json
import os
import secrets
import threading
import time
from collections import defaultdict, deque
from dataclasses import dataclass
from http import cookies
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

COOKIE_NAME = "shadow_device"
MAX_BODY = 2048


def _encode(value: bytes) -> str:
    return base64.urlsafe_b64encode(value).rstrip(b"=").decode("ascii")


def _decode(value: str) -> bytes:
    return base64.urlsafe_b64decode(value + "=" * (-len(value) % 4))


def make_config(username: str, password: str, *, public_origin: str,
                remember_days: int = 30, iterations: int = 600_000,
                max_attempts: int = 5) -> dict:
    if (not username or not password or not public_origin.startswith("https://")
            or not 1 <= remember_days <= 90 or iterations < 1000):
        raise ValueError("invalid site credentials")
    salt = secrets.token_bytes(32)
    digest = hashlib.pbkdf2_hmac("sha256", password.encode(), salt, iterations)
    return {
        "username": username,
        "password_salt": _encode(salt),
        "password_hash": _encode(digest),
        "iterations": iterations,
        "session_secret": _encode(secrets.token_bytes(32)),
        "public_origin": public_origin,
        "remember_days": remember_days,
        "max_attempts": max_attempts,
        "lockout_seconds": 900,
    }


@dataclass(frozen=True)
class AuthConfig:
    username: str
    password_salt: bytes
    password_hash: bytes
    iterations: int
    session_secret: bytes
    public_origin: str
    remember_days: int
    max_attempts: int
    lockout_seconds: int

    @classmethod
    def from_dict(cls, value: dict) -> "AuthConfig":
        config = cls(
            username=str(value["username"]),
            password_salt=_decode(value["password_salt"]),
            password_hash=_decode(value["password_hash"]),
            iterations=int(value["iterations"]),
            session_secret=_decode(value["session_secret"]),
            public_origin=str(value["public_origin"]),
            remember_days=int(value["remember_days"]),
            max_attempts=int(value["max_attempts"]),
            lockout_seconds=int(value["lockout_seconds"]),
        )
        if (len(config.password_salt) < 16 or len(config.password_hash) != 32
                or len(config.session_secret) < 32 or config.iterations < 1000
                or not config.public_origin.startswith("https://")
                or not 1 <= config.remember_days <= 90
                or not 1 <= config.max_attempts <= 20
                or not 60 <= config.lockout_seconds <= 3600):
            raise ValueError("invalid site auth configuration")
        return config


def load_config(path: Path) -> AuthConfig:
    if path.is_symlink() or not path.is_file():
        raise ValueError("auth config must be a regular file")
    status = path.stat()
    if status.st_uid != os.getuid() or status.st_mode & 0o077:
        raise ValueError("auth config must be owned by service user and mode 0600")
    return AuthConfig.from_dict(json.loads(path.read_text()))


def _token(config: AuthConfig, expires: int) -> str:
    payload = _encode(f"1:{expires}:{secrets.token_hex(16)}".encode())
    signature = _encode(hmac.new(config.session_secret, payload.encode(), hashlib.sha256).digest())
    return payload + "." + signature


def _valid_token(config: AuthConfig, token: str) -> bool:
    if len(token) > 512 or "." not in token:
        return False
    try:
        payload, signature = token.split(".", 1)
        expected = hmac.new(config.session_secret, payload.encode(), hashlib.sha256).digest()
        if not hmac.compare_digest(_decode(signature), expected):
            return False
        version, expiry, nonce = _decode(payload).decode().split(":")
        return version == "1" and len(nonce) == 32 and int(time.time()) < int(expiry)
    except (ValueError, UnicodeDecodeError, base64.binascii.Error):
        return False


class AttemptLimiter:
    def __init__(self, config: AuthConfig):
        self.config = config
        self._lock = threading.Lock()
        self._failures: dict[str, deque[float]] = defaultdict(deque)

    def _trim(self, key: str, now: float) -> deque[float]:
        failures = self._failures[key]
        while failures and now - failures[0] >= self.config.lockout_seconds:
            failures.popleft()
        return failures

    def blocked(self, key: str) -> bool:
        with self._lock:
            now = time.monotonic()
            return (len(self._trim(key, now)) >= self.config.max_attempts
                    or len(self._trim("*", now)) >= 100)

    def failed(self, key: str) -> None:
        with self._lock:
            now = time.monotonic()
            self._trim(key, now).append(now)
            self._trim("*", now).append(now)

    def succeeded(self, key: str) -> None:
        with self._lock:
            self._failures.pop(key, None)


class AuthHTTPServer(ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = True

    def __init__(self, address, config: AuthConfig):
        if address[0] not in ("127.0.0.1", "::1"):
            raise ValueError("site auth must bind to loopback")
        super().__init__(address, AuthHandler)
        self.config = config
        self.limiter = AttemptLimiter(config)


class AuthHandler(BaseHTTPRequestHandler):
    server_version = "BTCShadowAuth/1.0"

    def log_message(self, fmt, *args):
        # Never log passwords, cookies, request bodies or query strings.
        return

    def _send(self, status: int, body: bytes = b"", *, content_type: str = "text/html; charset=utf-8",
              location: str | None = None, cookie: str | None = None, retry_after: int | None = None):
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.send_header("X-Content-Type-Options", "nosniff")
        self.send_header("X-Frame-Options", "DENY")
        # A same-origin form POST needs a concrete Origin; no-referrer yields
        # Origin: null in Chromium and would reject legitimate mobile logins.
        self.send_header("Referrer-Policy", "same-origin")
        self.send_header("Content-Security-Policy", "default-src 'none'; style-src 'self'; form-action 'self'; base-uri 'none'; frame-ancestors 'none'")
        if location:
            self.send_header("Location", location)
        if cookie:
            self.send_header("Set-Cookie", cookie)
        if retry_after is not None:
            self.send_header("Retry-After", str(retry_after))
        self.end_headers()
        if body:
            self.wfile.write(body)

    def _cookie_token(self) -> str:
        raw = self.headers.get("Cookie", "")
        if len(raw) > 4096:
            return ""
        try:
            jar = cookies.SimpleCookie()
            jar.load(raw)
            return jar[COOKIE_NAME].value if COOKIE_NAME in jar else ""
        except cookies.CookieError:
            return ""

    def _authenticated(self) -> bool:
        return _valid_token(self.server.config, self._cookie_token())

    def _login_html(self, error: bool = False) -> bytes:
        username = html.escape(self.server.config.username, quote=True)
        warning = '<p class="error" role="alert">用户名或密码错误，请重试。</p>' if error else ""
        return f'''<!doctype html><html lang="zh-CN"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1,viewport-fit=cover"><title>BTC SHADOW · 登录</title><link rel="stylesheet" href="/auth.css"></head><body><main class="login-shell"><section class="login-card"><div class="mark">S</div><p class="eyebrow">BTC EVENT · SHADOW</p><h1>观察台登录</h1><p class="intro">观察与模拟默认运行；登录后可在交易页手动开启或停用真实自动交易。</p>{warning}<form method="post" action="/login"><label for="username">用户名</label><input id="username" name="username" autocomplete="username" value="{username}" required><label for="password">密码</label><input id="password" name="password" type="password" autocomplete="current-password" required><label class="remember"><input name="remember" type="checkbox" value="1" checked><span>记住此设备 {self.server.config.remember_days} 天</span></label><button type="submit">进入观察台</button></form><p class="note">公共设备请取消勾选。</p></section></main></body></html>'''.encode()

    def _client_key(self) -> str:
        # Caddy overwrites X-Real-IP with {remote_host}; the gateway is loopback-only.
        value = self.headers.get("X-Real-IP", self.client_address[0])
        return value[:64] if value else "local"

    def _same_origin(self) -> bool:
        origin = self.headers.get("Origin", "")
        host = self.headers.get("Host", "")
        expected = urlsplit(self.server.config.public_origin)
        return origin == self.server.config.public_origin and host == expected.netloc

    def do_GET(self):
        path = urlsplit(self.path).path
        if path == "/verify":
            self._send(204 if self._authenticated() else 302,
                       location=None if self._authenticated() else "/login")
        elif path == "/login":
            if self._authenticated():
                self._send(302, location="/")
            else:
                self._send(200, self._login_html())
        elif path == "/auth.css":
            body = (Path(__file__).parent / "static" / "site-auth.css").read_bytes()
            self._send(200, body, content_type="text/css; charset=utf-8")
        else:
            self._send(404, b"Not found", content_type="text/plain; charset=utf-8")

    def do_POST(self):
        path = urlsplit(self.path).path
        if path not in ("/login", "/logout"):
            self._send(404, b"Not found", content_type="text/plain; charset=utf-8")
            return
        if not self._same_origin():
            self._send(403, b"Forbidden", content_type="text/plain; charset=utf-8")
            return
        if path == "/logout":
            self._send(303, location="/login", cookie=f"{COOKIE_NAME}=; Path=/; Max-Age=0; HttpOnly; Secure; SameSite=Lax")
            return
        if self.headers.get("Content-Type", "").split(";")[0] != "application/x-www-form-urlencoded":
            self._send(415, b"Unsupported media type", content_type="text/plain; charset=utf-8")
            return
        try:
            length = int(self.headers.get("Content-Length", "0"))
            if not 0 < length <= MAX_BODY:
                raise ValueError("invalid length")
            form = parse_qs(self.rfile.read(length).decode("utf-8"), keep_blank_values=True)
            username = form.get("username", [""])[0]
            password = form.get("password", [""])[0]
            remember = form.get("remember", [""])[0] == "1"
        except (ValueError, UnicodeDecodeError):
            self._send(400, b"Invalid request", content_type="text/plain; charset=utf-8")
            return
        key = self._client_key()
        if self.server.limiter.blocked(key):
            self._send(429, b"Too many attempts", content_type="text/plain; charset=utf-8",
                       retry_after=self.server.config.lockout_seconds)
            return
        config = self.server.config
        actual = hashlib.pbkdf2_hmac("sha256", password.encode(), config.password_salt, config.iterations)
        if not (hmac.compare_digest(username.encode(), config.username.encode())
                and hmac.compare_digest(actual, config.password_hash)):
            self.server.limiter.failed(key)
            self._send(401, self._login_html(error=True))
            return
        self.server.limiter.succeeded(key)
        seconds = config.remember_days * 86400 if remember else 12 * 3600
        token = _token(config, int(time.time()) + seconds)
        extra = f"; Max-Age={seconds}" if remember else ""
        self._send(303, location="/", cookie=f"{COOKIE_NAME}={token}; Path=/; HttpOnly; Secure; SameSite=Lax{extra}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True, type=Path)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8766)
    args = parser.parse_args()
    config = load_config(args.config)
    server = AuthHTTPServer((args.host, args.port), config)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
