"""Loopback transport for the existing manual event-contract bridge.

This adapter never reads Binance credentials and never retries a submit.
The calling service must separately authorize every requested operation.
"""
from __future__ import annotations

import json
from urllib.error import HTTPError, URLError
from urllib.parse import urlsplit
from urllib.request import Request, urlopen


class BridgeUnavailable(RuntimeError):
    pass


class BridgeRejected(RuntimeError):
    pass


class BridgeClient:
    def __init__(self, base_url: str, pairing_key: str, *, opener=urlopen,
                 bridge_host: str = "127.0.0.1:8876", timeout: int = 5):
        parsed = urlsplit(base_url)
        if (parsed.scheme != "http" or parsed.hostname != "127.0.0.1"
                or parsed.port is None or parsed.username or parsed.password
                or parsed.path or parsed.query or parsed.fragment):
            raise ValueError("bridge destination must be a bare loopback URL")
        if (not isinstance(pairing_key, str) or len(pairing_key) < 32
                or any(c in pairing_key for c in "\r\n")):
            raise ValueError("invalid pairing key")
        if bridge_host != "127.0.0.1:8876" or not 1 <= timeout <= 15:
            raise ValueError("invalid bridge transport configuration")
        self.base_url = base_url
        self.key = pairing_key
        self.opener = opener
        self.bridge_host = bridge_host
        self.timeout = timeout

    def _call(self, path: str, payload: dict | None = None) -> dict:
        if path not in ("/status", "/quote", "/preview", "/submit", "/acknowledge", "/release-expired"):
            raise ValueError("unsupported bridge operation")
        if (path == "/status") != (payload is None):
            raise ValueError("invalid bridge operation body")
        data = None if payload is None else json.dumps(payload, separators=(",", ":")).encode()
        if data is not None and len(data) > 2048:
            raise ValueError("bridge request too large")
        headers = {"Host": self.bridge_host, "Authorization": "Bearer " + self.key}
        if data is not None:
            headers["Content-Type"] = "application/json"
        request = Request(self.base_url + path, headers=headers, data=data)
        try:
            with self.opener(request, timeout=self.timeout) as response:
                raw = response.read(65537)
        except HTTPError as error:
            try:
                result = json.loads(error.read(4096))
                code = result["error"]["code"]
                if isinstance(code, str) and code.isupper() and len(code) <= 50:
                    raise BridgeRejected(code) from None
            except (ValueError, KeyError, TypeError):
                pass
            raise BridgeUnavailable("BRIDGE_HTTP_ERROR") from None
        except (OSError, URLError):
            raise BridgeUnavailable("BRIDGE_UNAVAILABLE") from None
        if len(raw) > 65536:
            raise BridgeUnavailable("BRIDGE_RESPONSE_TOO_LARGE")
        try:
            result = json.loads(raw)
            if result.get("ok") is True and isinstance(result.get("data"), dict):
                return result["data"]
        except (ValueError, TypeError, AttributeError):
            pass
        raise BridgeUnavailable("BRIDGE_RESPONSE_INVALID")

    def status(self) -> dict:
        return self._call("/status")

    def quote(self, minutes: int, direction: str) -> dict:
        return self._call("/quote", {"minutes": minutes, "direction": direction})

    def preview(self, minutes: int, direction: str, amount: str) -> dict:
        return self._call("/preview", {"minutes": minutes, "direction": direction,
                                       "amount": amount})

    def submit(self, preview_id: str) -> dict:
        # The bridge persists this attempt before its single external POST.
        # An interrupted response is UNKNOWN, never an invitation to retry.
        return self._call("/submit", {"preview_id": preview_id})

    def acknowledge(self, attempt_id: str) -> dict:
        return self._call("/acknowledge", {"id": attempt_id,
                                            "checked_in_binance": True})

    def release_expired(self, attempt_id: str) -> dict:
        # This never states that an order filled; the bridge enforces the
        # full duration and buffer using its own persisted request time.
        return self._call("/release-expired", {"id": attempt_id})
