"""Isolated intrabar BTCUSDT dual Bollinger observer. Never submits an order."""
from __future__ import annotations

import argparse
import json
import os
import signal
import sqlite3
import time
from decimal import Decimal, InvalidOperation
from pathlib import Path

import requests

from chart_strategy import (RULES, open_db, reconstructed_band,
                            require_healthy_clock, require_healthy_index,
                            resolve_proxy)


INTRABAR = json.loads(Path(__file__).with_name("intrabar_rules.json").read_text())
FIVE_MS = RULES["five_min"]["period_ms"]
ONE_MS = RULES["one_min"]["period_ms"]


def _current(data: dict, key: str, received_ms: int) -> dict:
    spec = RULES[key]
    period = spec["period_ms"]
    payload = data[key]
    if (payload.get("status") != "ok"
            or payload.get("source") != RULES["source_id"]
            or payload.get("symbol") != RULES["symbol"]
            or payload.get("interval") != spec["interval"]):
        raise ValueError("SOURCE_MISMATCH")
    bars = payload["candles"]
    if len(bars) < INTRABAR["minimum_indicator_bars"]:
        raise ValueError("INDICATOR_HISTORY_UNAVAILABLE")
    previous = None
    for bar in bars[-INTRABAR["minimum_indicator_bars"]:]:
        if not isinstance(bar, list) or len(bar) != 6 or type(bar[0]) is not int:
            raise ValueError("INVALID_CANDLE")
        if previous is not None and bar[0] != previous + period:
            raise ValueError("NONCONTIGUOUS_CANDLE")
        previous = bar[0]
        try:
            opened, high, low, closed, volume = (Decimal(str(v)) for v in bar[1:])
        except (InvalidOperation, TypeError):
            raise ValueError("INVALID_CANDLE") from None
        if (not all(v.is_finite() for v in (opened, high, low, closed, volume))
                or min(opened, high, low, closed) <= 0 or volume < 0
                or not low <= min(opened, closed) <= max(opened, closed) <= high):
            raise ValueError("INVALID_CANDLE")
    current = bars[-1]
    expected_open = received_ms // period * period
    if current[0] != expected_open or not expected_open <= received_ms < expected_open + period:
        raise ValueError("STALE_CURRENT_CANDLE")
    upper, _, lower = reconstructed_band(bars, len(bars)-1, period)
    return {"open_ms": current[0], "open": Decimal(current[1]),
            "high": Decimal(current[2]), "low": Decimal(current[3]),
            "upper": upper, "lower": lower}


def validate_snapshot(data: dict, received_ms: int) -> tuple[dict, dict]:
    """Require both *currently forming* bars from one Binance Futures source."""
    if type(received_ms) is not int or not isinstance(data, dict):
        raise ValueError("INVALID_SNAPSHOT")
    five = _current(data, "five_min", received_ms)
    one = _current(data, "one_min", received_ms)
    if not five["open_ms"] <= one["open_ms"] < five["open_ms"] + FIVE_MS:
        raise ValueError("CHARTS_NOT_ALIGNED")
    return five, one


class IntrabarDetector:
    """Latch observed first touches within one forming 5m candle only."""

    def __init__(self):
        self.bucket = None
        self.one_open = None
        self.last_five = None
        self.last_one = None
        self.last_received_ms = None
        self.five_lower = self.five_upper = False
        self.one_lower = self.one_upper = False
        self.fired = False

    def observe(self, data: dict, received_ms: int) -> dict | None:
        five, one = validate_snapshot(data, received_ms)
        first = (self.last_received_ms is None or received_ms <= self.last_received_ms
                 or received_ms - self.last_received_ms > INTRABAR["max_observation_gap_ms"])
        self.last_received_ms = received_ms
        if self.bucket != five["open_ms"] or first:
            self.bucket = five["open_ms"]
            self.one_open = one["open_ms"]
            self.five_lower = self.five_upper = False
            self.one_lower = self.one_upper = False
            self.fired = False
            # On a process restart, a past candle wick is not a new live touch.
            self.last_five = (five["high"], five["low"]) if first else (five["open"], five["open"])
            self.last_one = (one["high"], one["low"]) if first else (one["open"], one["open"])
            if first:
                return None
        if self.one_open != one["open_ms"]:
            self.one_open = one["open_ms"]
            self.last_one = (one["open"], one["open"])

        prior_high, prior_low = self.last_five
        self.five_upper |= five["high"] > prior_high and five["high"] >= five["upper"]
        self.five_lower |= five["low"] < prior_low and five["low"] <= five["lower"]
        self.last_five = (max(prior_high, five["high"]), min(prior_low, five["low"]))

        prior_high, prior_low = self.last_one
        self.one_upper |= one["high"] > prior_high and one["high"] >= one["upper"]
        self.one_lower |= one["low"] < prior_low and one["low"] <= one["lower"]
        self.last_one = (max(prior_high, one["high"]), min(prior_low, one["low"]))

        lower = self.five_lower and self.one_lower
        upper = self.five_upper and self.one_upper
        if self.fired or (lower and upper):
            if lower and upper:
                self.fired = True
            return None
        if not (lower or upper):
            return None
        self.fired = True
        direction = "HIGHER" if lower else "LOWER"
        entry_ms = received_ms
        return {"direction": direction,
                "reason": "BOTH_LOWER_TOUCHED_INTRABAR" if lower else "BOTH_UPPER_TOUCHED_INTRABAR",
                "bar_open_ms": five["open_ms"],
                "bar_close_ms": five["open_ms"] + FIVE_MS,
                "one_open_ms": one["open_ms"],
                "entry_ms": entry_ms,
                "expiry_ms": entry_ms + RULES["horizon_ms"],
                "strategy_version": INTRABAR["strategy_version"],
                "source": RULES["source_id"]}


def save_candidate(path: Path, data: dict, decision: dict) -> bool:
    """Use the existing unique 5m key; a restart cannot create a second candidate."""
    db = open_db(path)
    try:
        with db:
            cursor = db.execute(
                "INSERT OR IGNORE INTO observations(received_ms,workspace,payload,decision,bar_open_ms,entry_ms,expiry_ms,execution) VALUES(?,?,?,?,?,?,?,?)",
                (decision["entry_ms"], RULES["source_id"], json.dumps(data, ensure_ascii=False),
                 json.dumps(decision), decision["bar_open_ms"], decision["entry_ms"],
                 decision["expiry_ms"], "NO TRADE"))
            return cursor.rowcount == 1
    finally:
        db.close()


class BinanceRestIntrabarClient:
    """Two bounded read-only REST calls per poll; no browser session or trade API."""

    def __init__(self, session=None, clock=None, sleep=None):
        self.session = session or requests.Session()
        self.session.trust_env = False
        self.clock = clock or (lambda: time.time_ns() // 1_000_000)
        self.sleep = sleep or time.sleep

    def _bars(self, key: str, asof_ms: int) -> dict:
        spec = RULES[key]
        period = spec["period_ms"]
        params = {"symbol": RULES["symbol"], "interval": spec["interval"],
                  "endTime": asof_ms, "limit": INTRABAR["market_history_bars"]}
        for attempt in range(INTRABAR["request_attempts"]):
            try:
                response = self.session.get(RULES["market_data_url"], params=params,
                                            timeout=INTRABAR["request_timeout_seconds"],
                                            allow_redirects=False)
                response.raise_for_status()
                raw = response.json()
                break
            except (requests.RequestException, ValueError):
                if attempt + 1 == INTRABAR["request_attempts"]:
                    raise ValueError("MARKET_REQUEST_FAILED") from None
                self.sleep(INTRABAR["retry_delay_seconds"])
        if not isinstance(raw, list) or len(raw) != INTRABAR["market_history_bars"]:
            raise ValueError("MARKET_HISTORY_UNAVAILABLE")
        bars = []
        for item in raw:
            if (not isinstance(item, list) or len(item) < 7 or type(item[0]) is not int
                    or type(item[6]) is not int or item[6] != item[0] + period - 1
                    or item[0] > asof_ms or (bars and item[0] != bars[-1][0] + period)):
                raise ValueError("MARKET_CANDLE_INVALID")
            bars.append([item[0], *(str(item[i]) for i in (1, 2, 3, 4, 5))])
        return {"status": "ok", "source": RULES["source_id"], "truncated": False,
                "symbol": RULES["symbol"], "interval": spec["interval"],
                "candles": bars}

    def snapshot(self) -> tuple[dict, int]:
        started = self.clock()
        data = {key: self._bars(key, started) for key in ("five_min", "one_min")}
        received = self.clock()
        if (received < started or received - started > INTRABAR["max_snapshot_duration_ms"]
                or received // ONE_MS != started // ONE_MS):
            raise ValueError("SNAPSHOT_STALE")
        validate_snapshot(data, received)
        return data, received


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True)
    parser.add_argument("--database", required=True)
    parser.add_argument("--ticks-db", required=True)
    args = parser.parse_args()
    config = json.loads(Path(args.config).read_text())
    path = Path(args.database).expanduser()
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    os.umask(0o077)
    client = BinanceRestIntrabarClient()
    detector = IntrabarDetector()
    stopped = False
    next_proxy_at = time.monotonic()

    def stop(*_):
        nonlocal stopped
        stopped = True

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    try:
        while not stopped:
            cycle_started = time.monotonic()
            try:
                data, received = client.snapshot()
                require_healthy_clock(args.ticks_db, received, config["health_max_age_ms"],
                                      config["clock_max_error_ms"])
                require_healthy_index(args.ticks_db, received, config["tick_max_delay_ms"])
                decision = detector.observe(data, received)
                if decision and save_candidate(path, data, decision):
                    print(json.dumps(decision), flush=True)
                if time.monotonic() >= next_proxy_at:
                    db = open_db(path)
                    try:
                        resolve_proxy(db, args.ticks_db, received)
                    finally:
                        db.close()
                    next_proxy_at = time.monotonic() + INTRABAR["proxy_resolve_seconds"]
            except (ValueError, KeyError, TypeError, OSError, sqlite3.Error) as error:
                code = str(error) if isinstance(error, ValueError) and str(error) in (
                    "MARKET_REQUEST_FAILED", "MARKET_HISTORY_UNAVAILABLE", "MARKET_CANDLE_INVALID",
                    "SNAPSHOT_STALE", "STALE_CURRENT_CANDLE", "CLOCK_UNAVAILABLE", "INDEX_UNAVAILABLE",
                    "INVALID_CANDLE", "NONCONTIGUOUS_CANDLE", "SOURCE_MISMATCH") else "DATA_UNAVAILABLE"
                print(json.dumps({"status": "UNAVAILABLE", "error_code": code}), flush=True)
            while not stopped:
                remaining = INTRABAR["poll_seconds"] - (time.monotonic() - cycle_started)
                if remaining <= 0:
                    break
                time.sleep(min(0.1, remaining))
    finally:
        client.session.close()


if __name__ == "__main__":
    main()
