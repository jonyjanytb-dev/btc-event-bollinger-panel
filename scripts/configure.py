#!/usr/bin/env python3
"""Render a fresh, private Ubuntu deployment without storing secrets in Git."""
from __future__ import annotations

import argparse
import getpass
import ipaddress
import json
import os
import re
import sys
import time
from decimal import Decimal, InvalidOperation
from pathlib import Path
from urllib.parse import urlsplit


def origin_value(value: str) -> tuple[str, bool]:
    parsed = urlsplit(value)
    if (parsed.scheme != "https" or not parsed.hostname or parsed.username
            or parsed.password or parsed.path or parsed.query or parsed.fragment
            or not re.fullmatch(r"[A-Za-z0-9.:-]+", parsed.netloc)):
        raise ValueError("--origin must be a bare HTTPS domain or IP origin")
    try:
        ipaddress.ip_address(parsed.hostname)
        is_ip = True
    except ValueError:
        is_ip = False
    return value, is_ip


def positive_amount(value: str, *, minimum: str = "0", places: int = 2) -> str:
    if not re.fullmatch(rf"\d{{1,12}}(?:\.\d{{1,{places}}})?", value):
        raise ValueError("invalid amount or ratio")
    try:
        number = Decimal(value)
    except (InvalidOperation, TypeError):
        raise ValueError("invalid amount or ratio") from None
    if not number.is_finite() or number < Decimal(minimum):
        raise ValueError("invalid amount or ratio")
    return value


def unit(name: str, description: str, workdir: Path, command: str,
         after: str = "network-online.target", requires: str = "") -> str:
    return (f"[Unit]\nDescription={description}\nAfter={after}\n"
            + (f"Requires={requires}\n" if requires else "")
            + f"[Service]\nWorkingDirectory={workdir}\nExecStart={command}\n"
            + "Restart=on-failure\nRestartSec=10\nUMask=0077\n"
            + "[Install]\nWantedBy=default.target\n")


def write_new(path: Path, content: str, mode: int) -> None:
    fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, mode)
    with os.fdopen(fd, "w", encoding="utf-8") as handle:
        os.fchmod(handle.fileno(), mode)
        handle.write(content)
        handle.flush()
        os.fsync(handle.fileno())


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base", type=Path, required=True,
                        help="absolute path of this repository on the Ubuntu server")
    parser.add_argument("--origin", required=True,
                        help="public HTTPS origin, e.g. https://panel.example.com")
    parser.add_argument("--username", required=True, help="dashboard login username")
    parser.add_argument("--max-amount", required=True,
                        help="maximum per order USDT amount allowed by the dashboard")
    parser.add_argument("--remember-days", type=int, default=30)
    parser.add_argument("--ratio-10")
    parser.add_argument("--ratio-30")
    parser.add_argument("--ratio-60")
    parser.add_argument("--password-stdin", action="store_true",
                        help="read dashboard password from stdin instead of an interactive prompt")
    args = parser.parse_args()

    base = args.base
    if not base.is_absolute() or any(ch.isspace() for ch in str(base)):
        parser.error("--base must be an absolute path without whitespace")
    base = base.resolve(strict=True)
    if not (base / "app/shadow/site_auth.py").is_file() or not (base / "chart-strategy/intrabar_strategy.py").is_file():
        parser.error("--base must point to the repository root")
    if not args.username or not 1 <= args.remember_days <= 90:
        parser.error("invalid username or remember duration")
    try:
        origin, is_ip = origin_value(args.origin)
        positive_amount(args.max_amount, minimum="5")
        ratios = {}
        for minutes in (10, 30, 60):
            value = getattr(args, f"ratio_{minutes}")
            if value is not None:
                positive_amount(value, minimum="0.00000001", places=8)
                if Decimal(value) > 1:
                    raise ValueError("ratio must be <= 1")
                ratios[str(minutes)] = value
    except ValueError as error:
        parser.error(str(error))

    password = sys.stdin.readline().rstrip("\r\n") if args.password_stdin else getpass.getpass("Dashboard password: ")
    if not password:
        parser.error("dashboard password cannot be empty")

    sys.path.insert(0, str(base / "app"))
    from shadow.site_auth import make_config

    private = base / "private"
    data = base / "data"
    deploy = base / "deploy"
    python = base / "venv/bin/python"
    shadow = json.loads((base / "app/shadow/config.example.json").read_text())
    collector = json.loads((base / "app/event_data/config.example.json").read_text())
    shadow.update(data_directory=str(data),
                  index_history_path=str(data / "index-1m.jsonl"),
                  collector_config_path=str(private / "collector.json"),
                  oauth_mode="dedicated",
                  oauth_credentials_path=str(private / "openmarket.json"),
                  model_predictions_enabled=False, prevent_idle_sleep=False)
    collector.update(database_path=str(data / "market.sqlite"),
                     openmarket_credentials_path=str(private / "openmarket.json"))
    site_auth = make_config(args.username, password, public_origin=origin,
                            remember_days=args.remember_days)
    live = dict(control_db=str(private / "live.sqlite"),
                chart_db=str(base / "chart-strategy/observations.sqlite"),
                bridge_key_file=str(private / "bridge/bridge.key"),
                max_amount=args.max_amount, public_origin=origin)
    bridge = dict(port=8876, state_dir=str(private / "bridge"),
                  credentials_path=str(private / "binance-session.json"),
                  request_timeout_seconds=12, read_retries=1,
                  minimum_amount="5", preview_ttl_ms=15000,
                  quote_max_age_ms=15000,
                  verified_enums={"10": "TEN_MINUTE", "30": "THIRTY_MINUTE", "60": "ONE_HOUR"},
                  quote_source=None, parameter_mode="configured",
                  configured_ratios=ratios, auto_release_buffer_ms=60000)
    statistics = dict(start_ms=int(time.time() * 1000),
                      bridge_ledger=str(private / "bridge/ledger.sqlite"))
    configs = {name: json.dumps(value, ensure_ascii=False, indent=2) + "\n"
               for name, value in (
                   ("shadow.json", shadow), ("collector.json", collector),
                   ("site-auth.json", site_auth), ("live.json", live),
                   ("bridge.json", bridge), ("live-statistics.json", statistics))}

    units = {
        "btc-panel-shadow.service": unit("shadow", "BTC read-only collector", base / "app",
            f"{python} -u -m shadow run --config {private / 'shadow.json'}"),
        "btc-panel-chart.service": unit("chart", "BTC Bollinger observer", base / "chart-strategy",
            f"{python} -u {base / 'chart-strategy/intrabar_strategy.py'} --config {private / 'shadow.json'} --database {base / 'chart-strategy/observations.sqlite'} --ticks-db {data / 'shadow.sqlite'}",
            after="btc-panel-shadow.service"),
        "btc-panel-auth.service": unit("auth", "BTC panel login", base / "app",
            f"{python} -u -m shadow.site_auth --config {private / 'site-auth.json'} --host 127.0.0.1 --port 8766"),
        "btc-panel-bridge.service": unit("bridge", "BTC private order bridge", base / "app/live-bridge",
            f"{python} -u {base / 'app/live-bridge/bridge.py'} --serve --config {private / 'bridge.json'}"),
        "btc-panel-live-worker.service": unit("worker", "BTC live signal consumer", base / "app",
            f"{python} -u -m shadow.live_worker --chart-db {base / 'chart-strategy/observations.sqlite'} --control-db {private / 'live.sqlite'} --pairing-key-file {private / 'bridge/bridge.key'} --ticks-db {data / 'shadow.sqlite'} --poll-seconds 1 --release-buffer-ms 60000",
            after="btc-panel-chart.service btc-panel-bridge.service",
            requires="btc-panel-bridge.service"),
        "btc-panel-web.service": unit("web", "BTC panel web", base / "app",
            f"{python} -u -m shadow.web --config {private / 'shadow.json'} --live-config {private / 'live.json'} --host 127.0.0.1 --port 8765",
            after="btc-panel-shadow.service btc-panel-auth.service"),
    }
    global_options = "admin off\n    auto_https disable_redirects"
    tls_options = ""
    if is_ip:
        global_options += f"\n    default_sni {urlsplit(origin).hostname}"
        tls_options = "\n    tls {\n        issuer acme {\n            profile shortlived\n            disable_http_challenge\n        }\n    }"
    caddy = f"""{{
    {global_options}
}}
{origin} {{{tls_options}
    @login path /login /logout /auth.css
    handle @login {{
        reverse_proxy 127.0.0.1:8766
    }}
    handle {{
        route {{
            forward_auth 127.0.0.1:8766 {{
                uri /verify
            }}
            reverse_proxy 127.0.0.1:8765 {{
                header_up -Authorization
            }}
        }}
    }}
    header {{
        Cache-Control "no-store"
        X-Content-Type-Options "nosniff"
        X-Frame-Options "DENY"
        Referrer-Policy "same-origin"
        -Server
    }}
}}
"""

    targets = [private / name for name in configs] + [deploy / name for name in units] + [deploy / "Caddyfile"]
    if any(path.exists() for path in targets):
        parser.error("configuration already exists; refusing to overwrite it")
    os.umask(0o077)
    for directory, mode in ((private, 0o700), (data, 0o700), (deploy, 0o700)):
        directory.mkdir(mode=mode, exist_ok=True)
        os.chmod(directory, mode)
    for name, contents in configs.items():
        write_new(private / name, contents, 0o600)
    for name, contents in units.items():
        write_new(deploy / name, contents, 0o644)
    write_new(deploy / "Caddyfile", caddy, 0o644)
    print("Private configuration and service templates created. Live trading is OFF.")
    if not ratios:
        print("No payout ratio is configured; all trading periods remain unavailable.")


if __name__ == "__main__":
    main()
