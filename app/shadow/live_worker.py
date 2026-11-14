"""Read-only chart observation consumer for the isolated live control ledger.

The worker never creates a signal. It consumes only observations written after
an explicit enable, and the control ledger decides whether a signal is fresh.
"""
from __future__ import annotations

import argparse
import sqlite3
import time
from decimal import Decimal, InvalidOperation
from pathlib import Path
from urllib.parse import quote

from .live_bridge import BridgeClient, BridgeRejected, BridgeUnavailable
from .live_control import LiveControl, LiveControlError, LiveExecutor

FIELDS = "id,received_ms,entry_ms,expiry_ms,decision,execution"
PROXY_LAG_MS = 5000


def resolve_live_proxy(control: LiveControl, ticks_db: Path, now_ms: int) -> int:
    """Persist received index proxy outcomes independently of the trading switch."""
    with sqlite3.connect(Path(ticks_db).resolve().as_uri() + "?mode=ro", uri=True, timeout=2) as ticks:
        ticks.execute("PRAGMA query_only=ON")
        resolved = 0
        for attempt in control.attempts():
            if (attempt["state"] not in ("API_ACCEPTED_UNVERIFIED", "TIME_EXPIRED_UNVERIFIED", "USER_REVIEWED_UNVERIFIED")
                    or not attempt["bridge_attempt_id"] or attempt["expiry_ms"] is None):
                continue
            prior = control.db.execute("SELECT status,entry_price,exit_price FROM live_proxy_results WHERE attempt_id=?", (attempt["id"],)).fetchone()
            if prior and prior["status"] in ("VERIFIED", "EQUAL"):
                continue
            created_ms, expiry_ms = attempt["created_ms"], attempt["expiry_ms"]
            # The entry snapshot must have reached us before the request began.
            entry = ticks.execute(
                "SELECT index_price,event_ms FROM ticks WHERE event_ms BETWEEN ? AND ? "
                "AND received_ms<=? ORDER BY received_ms DESC,event_ms DESC LIMIT 1",
                (created_ms - PROXY_LAG_MS, created_ms, created_ms)).fetchone()
            if entry is None:
                entry = ticks.execute(
                    "SELECT index_price,event_ms FROM ticks WHERE event_ms BETWEEN ? AND ? "
                    "AND received_ms<=? ORDER BY event_ms,received_ms LIMIT 1",
                    (created_ms, created_ms + PROXY_LAG_MS,
                     created_ms + PROXY_LAG_MS)).fetchone()
            exit_ = ticks.execute(
                "SELECT index_price,event_ms FROM ticks WHERE event_ms BETWEEN ? AND ? "
                "AND received_ms<=? ORDER BY event_ms,received_ms LIMIT 1",
                (expiry_ms, expiry_ms + PROXY_LAG_MS,
                 expiry_ms + PROXY_LAG_MS)).fetchone()
            if now_ms < attempt["expiry_ms"] + PROXY_LAG_MS:
                status, outcome = "PENDING", None
            elif not entry or not exit_:
                status, outcome = "MISSING", None
            else:
                try:
                    entry_value, exit_value = Decimal(entry[0]), Decimal(exit_[0])
                    valid = all(value.is_finite() and value > 0 for value in (entry_value, exit_value))
                except (InvalidOperation, TypeError):
                    valid = False
                if not valid:
                    status, outcome = "MISSING", None
                elif exit_value == entry_value:
                    status, outcome = "EQUAL", None
                else:
                    correct = (exit_value > entry_value) == (attempt["direction"] == "LONG")
                    status, outcome = "VERIFIED", "WIN" if correct else "LOSS"
            if (prior and prior["status"] == status and prior["entry_price"] == (entry[0] if entry else None)
                    and prior["exit_price"] == (exit_[0] if exit_ else None)):
                continue
            with control.transaction():
                control.db.execute(
                    "INSERT INTO live_proxy_results VALUES(?,?,?,?,?,?,?,?) ON CONFLICT(attempt_id) DO UPDATE SET status=excluded.status,entry_price=excluded.entry_price,exit_price=excluded.exit_price,outcome=excluded.outcome,entry_event_ms=excluded.entry_event_ms,exit_event_ms=excluded.exit_event_ms,resolved_ms=excluded.resolved_ms",
                    (attempt["id"], status, entry[0] if entry else None, exit_[0] if exit_ else None,
                     outcome, entry[1] if entry else None, exit_[1] if exit_ else None, now_ms))
            resolved += 1
        return resolved


def _open_observations(path: Path):
    path = Path(path)
    if path.is_symlink() or not path.is_file():
        raise ValueError("CHART_DATABASE_UNAVAILABLE")
    uri = "file:" + quote(str(path.resolve(strict=True)), safe="/") + "?mode=ro"
    db = sqlite3.connect(uri, uri=True, timeout=2)
    db.row_factory = sqlite3.Row
    db.execute("PRAGMA query_only=ON")
    return db


def latest_observation_id(path: Path) -> int:
    with _open_observations(path) as db:
        row = db.execute("SELECT COALESCE(MAX(id),0) FROM observations").fetchone()
        return int(row[0])


def read_observations_after(path: Path, cursor: int, limit: int = 100) -> list[dict]:
    if type(cursor) is not int or cursor < 0 or not 1 <= limit <= 100:
        raise ValueError("INVALID_CHART_CURSOR")
    with _open_observations(path) as db:
        return [dict(row) for row in db.execute(
            f"SELECT {FIELDS} FROM observations WHERE id>? ORDER BY id LIMIT ?",
            (cursor, limit),
        )]


def run_tick(control: LiveControl, chart_db: Path, bridge, now_ms: int,
             *, clock=None, release_buffer_ms: int = 60000) -> list[dict]:
    state = control.state()
    if not state:
        return []
    pending = [a for a in control.attempts() if a["state"] == "API_ACCEPTED_UNVERIFIED"]
    if len(pending) > 1:
        raise LiveControlError("MULTIPLE_UNVERIFIED_ATTEMPTS")
    if pending:
        attempt = pending[0]
        effective_buffer_ms = max(release_buffer_ms, attempt["release_buffer_ms"])
        if (attempt["expiry_ms"] is None
                or now_ms < attempt["expiry_ms"] + effective_buffer_ms):
            return []
        try:
            result = bridge.release_expired(attempt["bridge_attempt_id"])
        except BridgeRejected as error:
            if str(error) == "NOT_EXPIRED":
                return []
            raise LiveControlError("BRIDGE_RELEASE_FAILED") from None
        except BridgeUnavailable:
            raise LiveControlError("BRIDGE_RELEASE_UNAVAILABLE") from None
        if (result.get("id") != attempt["bridge_attempt_id"]
                or result.get("status") != "ACCEPTED_UNVERIFIED"
                or (result.get("auto_released_ms") is None
                    and result.get("acknowledged_ms") is None)):
            raise LiveControlError("BRIDGE_RELEASE_INVALID")
        control.release_accepted(attempt["id"], now_ms,
                                 attempt["bridge_attempt_id"], buffer_ms=effective_buffer_ms)
    control.advance_ladder(now_ms)
    state = control.state()
    if not state["enabled"]:
        # Persist why a valid chart observation produced no order while paused.
        # Re-arming still starts at the latest observation; it never replays one.
        control.audit_disabled_observations(
            read_observations_after(chart_db, control.disabled_audit_cursor()), now_ms)
        return []
    results = []
    for row in read_observations_after(chart_db, state["cursor"]):
        if type(row.get("entry_ms")) is int and row["entry_ms"] > now_ms:
            break
        result = LiveExecutor(control, bridge, clock=clock).process(row, now_ms)
        if result is not None:
            results.append(result)
        state = control.state()
        if not state or not state["enabled"]:
            break
    return results


def main():
    parser = argparse.ArgumentParser(description="Isolated one-attempt live signal consumer")
    parser.add_argument("--chart-db", type=Path, required=True)
    parser.add_argument("--control-db", type=Path, required=True)
    parser.add_argument("--pairing-key-file", type=Path, required=True)
    parser.add_argument("--poll-seconds", type=float, default=1.0)
    parser.add_argument("--release-buffer-ms", type=int, default=60000)
    parser.add_argument("--ticks-db", type=Path)
    args = parser.parse_args()
    if not 0.5 <= args.poll_seconds <= 5:
        parser.error("poll interval must be 0.5 to 5 seconds")
    if not 60000 <= args.release_buffer_ms <= 600000:
        parser.error("release buffer must be 1 to 10 minutes")
    key_file = args.pairing_key_file
    if key_file.is_symlink() or not key_file.is_file() or key_file.stat().st_mode & 0o077:
        parser.error("private pairing key file required")
    bridge = BridgeClient("http://127.0.0.1:8876", key_file.read_text().strip())
    ticks_db = args.ticks_db or args.chart_db.parent.parent / "data/shadow.sqlite"
    with LiveControl(args.control_db) as control:
        control.recover_pending(time.time_ns() // 1_000_000)
    while True:
        try:
            with LiveControl(args.control_db) as control:
                resolve_live_proxy(control, ticks_db, time.time_ns() // 1_000_000)
                run_tick(control, args.chart_db, bridge, time.time_ns() // 1_000_000,
                         release_buffer_ms=args.release_buffer_ms)
        except (ValueError, OSError, sqlite3.Error, LiveControlError) as error:
            # Only fixed error codes leave the worker; never print a request,
            # token, response body or external exception text.
            print("LIVE_WORKER_DATA_UNAVAILABLE", type(error).__name__, flush=True)
        time.sleep(args.poll_seconds)


if __name__ == "__main__":
    main()
