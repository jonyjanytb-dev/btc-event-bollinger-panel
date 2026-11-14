"""Isolated intrabar BTCUSDT Bollinger observer. Never submits an order."""
from __future__ import annotations

import argparse
import json
import os
import signal
import sqlite3
import time
from concurrent.futures import ThreadPoolExecutor
from decimal import Decimal, InvalidOperation
from pathlib import Path

import requests
from shadow.signal_periods import DEFAULT_PERIODS, PERIOD_MS, periods_path, read_periods

from chart_strategy import (RULES, open_db, reconstructed_band,
                            require_healthy_clock, require_healthy_index,
                            resolve_proxy)


INTRABAR = json.loads(Path(__file__).with_name("intrabar_rules.json").read_text())
FIVE_MS = RULES["five_min"]["period_ms"]
ONE_MS = RULES["one_min"]["period_ms"]
PERIOD_KEYS = {"1m": "one_min", "5m": "five_min", "10m": "ten_min",
               "15m": "fifteen_min", "30m": "thirty_min", "1h": "one_hour"}


def aggregate_ten_minute(bars: list[list]) -> list[list]:
    result = []
    for bar in bars:
        bucket = bar[0] // 600_000 * 600_000
        if not result or result[-1][0] != bucket:
            if bar[0] != bucket:
                continue
            result.append([bucket, *bar[1:]])
        else:
            current = result[-1]
            if bar[0] != bucket + 300_000:
                raise ValueError("MARKET_CANDLE_INVALID")
            current[2] = str(max(Decimal(current[2]), Decimal(bar[2])))
            current[3] = str(min(Decimal(current[3]), Decimal(bar[3])))
            current[4] = bar[4]
            current[5] = str(Decimal(current[5]) + Decimal(bar[5]))
    return result


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


def validate_selected_snapshot(data: dict, received_ms: int, periods: tuple[str, ...]) -> dict[str, dict]:
    if type(received_ms) is not int or not isinstance(data, dict):
        raise ValueError("INVALID_SNAPSHOT")
    result = {period: _current(data, PERIOD_KEYS[period], received_ms) for period in periods}
    for period, candle in result.items():
        if candle["open_ms"] != received_ms // PERIOD_MS[period] * PERIOD_MS[period]:
            raise ValueError("CHARTS_NOT_ALIGNED")
    return result


class IntrabarDetector:
    """Latch same-side touches within each current candle, once per longest candle."""

    def __init__(self, periods: tuple[str, ...] = DEFAULT_PERIODS):
        self.periods = periods
        self.bucket = None
        self.opens = {}
        self.last = {}
        self.lower = {}
        self.upper = {}
        self.last_received_ms = None
        self.fired = False

    def observe(self, data: dict, received_ms: int) -> dict | None:
        candles = validate_selected_snapshot(data, received_ms, self.periods)
        longest = self.periods[-1]
        first = (self.last_received_ms is None or received_ms <= self.last_received_ms
                 or received_ms - self.last_received_ms > INTRABAR["max_observation_gap_ms"])
        self.last_received_ms = received_ms
        if self.bucket != candles[longest]["open_ms"] or first:
            self.bucket = candles[longest]["open_ms"]
            self.opens = {}
            self.last = {}
            self.lower = {}
            self.upper = {}
            self.fired = False
        for period, candle in candles.items():
            if self.opens.get(period) != candle["open_ms"]:
                self.opens[period] = candle["open_ms"]
                self.lower[period] = self.upper[period] = False
                self.last[period] = ((candle["high"], candle["low"]) if first
                                     else (candle["open"], candle["open"]))
            prior_high, prior_low = self.last[period]
            if not first:
                self.upper[period] |= candle["high"] > prior_high and candle["high"] >= candle["upper"]
                self.lower[period] |= candle["low"] < prior_low and candle["low"] <= candle["lower"]
            self.last[period] = (max(prior_high, candle["high"]), min(prior_low, candle["low"]))
        if first:
            return None
        lower = all(self.lower.values())
        upper = all(self.upper.values())
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
                "bar_open_ms": candles[longest]["open_ms"],
                "bar_close_ms": candles[longest]["open_ms"] + PERIOD_MS[longest],
                "one_open_ms": candles.get("1m", {}).get("open_ms"),
                "periods": list(self.periods),
                "period_open_ms": {period: candles[period]["open_ms"] for period in self.periods},
                "entry_ms": entry_ms,
                "expiry_ms": entry_ms + RULES["horizon_ms"],
                "strategy_version": (INTRABAR["strategy_version"] if self.periods == DEFAULT_PERIODS
                                     else INTRABAR["multi_strategy_version"]),
                "source": RULES["source_id"]}


def save_candidate(path: Path, data: dict, decision: dict) -> bool:
    """Use the existing unique candle key; a restart cannot duplicate a candidate."""
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
    """Bounded concurrent read-only REST calls; no browser session or trade API."""

    def __init__(self, session=None, clock=None, sleep=None):
        self.session = session or requests.Session()
        self.session.trust_env = False
        self.clock = clock or (lambda: time.time_ns() // 1_000_000)
        self.sleep = sleep or time.sleep

    def _bars(self, key: str, asof_ms: int) -> dict:
        spec = RULES[key]
        synthetic = key == "ten_min"
        period = FIVE_MS if synthetic else spec["period_ms"]
        limit = INTRABAR["market_history_bars"] * (2 if synthetic else 1)
        params = {"symbol": RULES["symbol"], "interval": "5m" if synthetic else spec["interval"],
                  "endTime": asof_ms, "limit": limit}
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
        if not isinstance(raw, list) or len(raw) != limit:
            raise ValueError("MARKET_HISTORY_UNAVAILABLE")
        bars = []
        for item in raw:
            if (not isinstance(item, list) or len(item) < 7 or type(item[0]) is not int
                    or type(item[6]) is not int or item[6] != item[0] + period - 1
                    or item[0] % period or item[0] > asof_ms
                    or (bars and item[0] != bars[-1][0] + period)):
                raise ValueError("MARKET_CANDLE_INVALID")
            try:
                opened, high, low, closed, volume = (Decimal(str(item[i])) for i in (1, 2, 3, 4, 5))
            except (InvalidOperation, TypeError):
                raise ValueError("MARKET_CANDLE_INVALID") from None
            if (not all(value.is_finite() for value in (opened, high, low, closed, volume))
                    or min(opened, high, low, closed) <= 0 or volume < 0
                    or not low <= min(opened, closed) <= max(opened, closed) <= high):
                raise ValueError("MARKET_CANDLE_INVALID")
            bars.append([item[0], *(str(item[i]) for i in (1, 2, 3, 4, 5))])
        if synthetic:
            bars = aggregate_ten_minute(bars)
            if len(bars) != INTRABAR["market_history_bars"]:
                raise ValueError("MARKET_HISTORY_UNAVAILABLE")
        result = {"status": "ok", "source": RULES["source_id"], "truncated": False,
                  "symbol": RULES["symbol"], "interval": spec["interval"],
                  "candles": bars}
        if synthetic:
            result["derived_from"] = "5m"
        return result

    def snapshot(self, periods: tuple[str, ...] = DEFAULT_PERIODS) -> tuple[dict, int]:
        started = self.clock()
        keys = tuple(dict.fromkeys(PERIOD_KEYS[period] for period in periods))
        with ThreadPoolExecutor(max_workers=len(keys)) as pool:
            payloads = list(pool.map(lambda key: self._bars(key, started), keys))
        data = dict(zip(keys, payloads))
        received = self.clock()
        if (received < started or received - started > INTRABAR["max_snapshot_duration_ms"]
                or received // ONE_MS != started // ONE_MS):
            raise ValueError("SNAPSHOT_STALE")
        validate_selected_snapshot(data, received, periods)
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
    selected = read_periods(periods_path(path))
    detector = IntrabarDetector(selected)
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
                current_selection = read_periods(periods_path(path))
                if current_selection != selected:
                    selected = current_selection
                    detector = IntrabarDetector(selected)
                data, received = client.snapshot(selected)
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
