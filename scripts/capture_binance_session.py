#!/usr/bin/env python3
"""Capture one consenting Binance Web login into a private local file.

Run on a trusted computer with a visible browser. The customer scans and confirms
in their own Binance App. Transfer the output to their server over SSH afterward.
"""
from __future__ import annotations

import argparse
import json
import os
import tempfile
import time
from http.cookies import SimpleCookie
from pathlib import Path

from playwright.sync_api import sync_playwright


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True,
                        help="new private JSON file outside the Git repository")
    parser.add_argument("--browser-executable", type=Path,
                        help="optional installed Chrome executable")
    parser.add_argument("--timeout-seconds", type=int, default=600)
    args = parser.parse_args()
    if args.timeout_seconds < 30 or args.timeout_seconds > 1800:
        parser.error("timeout must be 30–1800 seconds")
    output = args.output.expanduser().absolute()
    if output.exists():
        parser.error("output file already exists; refusing to overwrite it")
    if args.browser_executable and not args.browser_executable.is_file():
        parser.error("browser executable does not exist")
    os.umask(0o077)
    output.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    state = {"csrf": "", "p20t": "", "expiry": -1, "qr": False}

    with tempfile.TemporaryDirectory(prefix="binance-customer-login-") as profile:
        with sync_playwright() as playwright:
            opts = dict(headless=False, viewport={"width": 1280, "height": 960},
                        locale="zh-CN", args=["--no-first-run", "--no-default-browser-check"])
            if args.browser_executable:
                opts["executable_path"] = str(args.browser_executable)
            context = playwright.chromium.launch_persistent_context(profile, **opts)
            page = context.pages[0] if context.pages else context.new_page()

            def on_request(request):
                if request.url.startswith("https://www.binance.com/bapi/"):
                    token = request.headers.get("csrftoken", "")
                    if token:
                        state["csrf"] = token

            def on_finished(request):
                try:
                    response = request.response()
                    if response is None:
                        return
                    if request.url.startswith("https://accounts.binance.com/bapi/accounts/v3/public/qrcode/login/get"):
                        body = response.json()
                        state["qr"] = bool(body.get("success") and body.get("data", {}).get("qrCode"))
                    if request.url.startswith("https://accounts.binance.com/bapi/accounts/v2/private/authcenter/setTrustDevice"):
                        for header in response.headers_array():
                            if header.get("name", "").lower() != "set-cookie":
                                continue
                            jar = SimpleCookie()
                            jar.load(header.get("value", ""))
                            if "p20t" in jar and jar["p20t"].value:
                                state["p20t"] = jar["p20t"].value
                                if jar["p20t"]["max-age"]:
                                    state["expiry"] = int(time.time()) + int(jar["p20t"]["max-age"])
                except Exception:
                    # Binance may change response shapes. Never print headers or tokens.
                    return

            context.on("request", on_request)
            context.on("requestfinished", on_finished)
            page.goto("https://accounts.binance.com/zh-CN/login?loginChannel=&return_to=",
                      wait_until="domcontentloaded", timeout=30000)
            print("Visible Binance login opened. Ask the customer to scan and confirm.", flush=True)
            deadline = time.monotonic() + args.timeout_seconds
            last = None
            while time.monotonic() < deadline:
                page.wait_for_timeout(1200)
                try:
                    if "accounts.binance.com" in page.url:
                        qr_button = page.get_by_role("button", name="二维码登录", exact=True)
                        if qr_button.count() and qr_button.first.is_visible() and not state["qr"]:
                            qr_button.first.click(timeout=600, force=True)
                    cookies = context.cookies("https://www.binance.com")
                    for cookie in cookies:
                        if cookie.get("name") == "p20t" and cookie.get("value"):
                            state["p20t"] = cookie["value"]
                            if cookie.get("expires", -1) > 0:
                                state["expiry"] = int(cookie["expires"])
                        if cookie.get("name") == "csrftoken" and cookie.get("value"):
                            state["csrf"] = cookie["value"]
                except Exception:
                    pass
                flags = (state["qr"], bool(state["csrf"]), bool(state["p20t"]))
                if flags != last:
                    print("QR=%s CSRF=%s WEB_SESSION=%s" % flags, flush=True)
                    last = flags
                if state["csrf"] and state["p20t"]:
                    payload = {"p20t": state["p20t"], "csrftoken": state["csrf"],
                               "expirationTimestamp": state["expiry"]}
                    fd = os.open(output, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
                    with os.fdopen(fd, "w", encoding="utf-8") as handle:
                        json.dump(payload, handle, separators=(",", ":"))
                        handle.flush()
                        os.fsync(handle.fileno())
                    print("Customer Web session saved to the requested private file.", flush=True)
                    break
            else:
                raise SystemExit("Login did not produce both Web credentials before timeout.")
            context.close()


if __name__ == "__main__":
    main()
