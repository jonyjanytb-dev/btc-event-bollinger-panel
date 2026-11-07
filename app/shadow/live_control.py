"""Fail-closed reservation ledger for the user-selected Bollinger live strategy.

No exchange call is made here. A separate adapter may submit a reserved signal
once, after a user has enabled the strategy and the bridge is authenticated.
"""
from __future__ import annotations

import json
import sqlite3
import time
from contextlib import contextmanager
from decimal import Decimal, InvalidOperation
from pathlib import Path
from uuid import uuid4

LEGACY_STRATEGY = "bb_dual_touch_30m_v1"
STRATEGY = "bb_dual_touch_30m_intrabar_v2"
SUPPORTED_STRATEGIES = frozenset((LEGACY_STRATEGY, STRATEGY))
HORIZON_MS = 30 * 60_000
PERIOD_ENUMS = {10: "TEN_MINUTE", 30: "THIRTY_MINUTE", 60: "ONE_HOUR"}
DIRECTION_MODES = frozenset(("SIGNAL", "REVERSE", "LONG_ONLY", "SHORT_ONLY"))
MAX_ENTRY_LAG_MS = 2_000
INTRABAR_ENTRY_LAG_MS = 5_000
APP_ID = 1162894410


class LiveControlError(RuntimeError):
    pass


def _amount(value: str) -> Decimal:
    try:
        number = Decimal(value)
    except (InvalidOperation, TypeError):
        raise LiveControlError("INVALID_AMOUNT") from None
    if (not isinstance(value, str) or not number.is_finite()
            or number.as_tuple().exponent < -2 or number <= 0):
        raise LiveControlError("INVALID_AMOUNT")
    return number


def eligible_signal(row: dict, now_ms: int, *, strategy: str | None = None) -> str | None:
    """Return the bridge direction only for a fresh, matching 30M signal."""
    try:
        if not isinstance(row, dict) or type(now_ms) is not int:
            return None
        observation_id = row["id"]
        received = row["received_ms"]
        entry = row["entry_ms"]
        expiry = row["expiry_ms"]
        if any(type(v) is not int for v in (observation_id, received, entry, expiry)):
            return None
        decision = json.loads(row["decision"])
        direction = decision["direction"]
        reason = decision["reason"]
        version = decision.get("strategy_version")
        if (version not in SUPPORTED_STRATEGIES or (strategy is not None and version != strategy)
                or row["execution"] != "NO TRADE"
                or expiry - entry != HORIZON_MS
                or decision.get("entry_ms") != entry
                or decision.get("expiry_ms") != expiry
                or not received <= entry <= now_ms):
            return None
        if version == STRATEGY:
            five_open = decision.get("bar_open_ms")
            one_open = decision.get("one_open_ms")
            if (now_ms > entry + INTRABAR_ENTRY_LAG_MS or entry != received
                    or decision.get("source") != "BINANCE_FUTURES_KLINES"
                    or type(five_open) is not int or type(one_open) is not int
                    or five_open % 300_000 or one_open % 60_000
                    or not five_open <= one_open <= received < one_open + 60_000
                    or not received < five_open + 300_000
                    or decision.get("bar_close_ms") != five_open + 300_000):
                return None
            if direction == "HIGHER" and reason == "BOTH_LOWER_TOUCHED_INTRABAR":
                return "LONG"
            if direction == "LOWER" and reason == "BOTH_UPPER_TOUCHED_INTRABAR":
                return "SHORT"
            return None
        if (now_ms > entry + MAX_ENTRY_LAG_MS or entry - received > 60_000
                or type(decision.get("bar_close_ms")) is not int
                or not entry - 60_000 <= decision["bar_close_ms"] <= received):
            return None
        if direction == "HIGHER" and reason == "BOTH_LOWER_TOUCHED":
            return "LONG"
        if direction == "LOWER" and reason == "BOTH_UPPER_TOUCHED":
            return "SHORT"
    except (KeyError, ValueError, TypeError, json.JSONDecodeError):
        return None
    return None


class LiveControl:
    def __init__(self, path: Path):
        path = Path(path)
        if path.is_symlink() or not path.parent.is_dir():
            raise LiveControlError("INVALID_DATABASE_PATH")
        self.db = sqlite3.connect(path, timeout=10, isolation_level=None)
        self.db.row_factory = sqlite3.Row
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA synchronous=FULL")
        version = self.db.execute("PRAGMA user_version").fetchone()[0]
        app_id = self.db.execute("PRAGMA application_id").fetchone()[0]
        existing = self.db.execute("SELECT count(*) FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'").fetchone()[0]
        if (version not in (0, 1, 2, 3, 4, 5, 6) or (version and app_id != APP_ID)
                or (version == 0 and existing)):
            self.db.close()
            raise LiveControlError("UNRELATED_DATABASE")
        if version == 0:
            with self.transaction():
                self.db.execute("CREATE TABLE config(id INTEGER PRIMARY KEY CHECK(id=1),enabled INTEGER NOT NULL, strategy TEXT NOT NULL,horizon INTEGER NOT NULL,amount TEXT NOT NULL,cursor INTEGER NOT NULL,updated_ms INTEGER NOT NULL,reverse INTEGER NOT NULL DEFAULT 0 CHECK(reverse IN (0,1)),cooldown_five_minutes INTEGER NOT NULL DEFAULT 0 CHECK(cooldown_five_minutes IN (0,1)),direction_mode TEXT NOT NULL DEFAULT 'SIGNAL' CHECK(direction_mode IN ('SIGNAL','REVERSE','LONG_ONLY','SHORT_ONLY')))")
                self.db.execute("CREATE TABLE attempts(id TEXT PRIMARY KEY,observation_id INTEGER NOT NULL UNIQUE,direction TEXT NOT NULL,amount TEXT NOT NULL,state TEXT NOT NULL,created_ms INTEGER NOT NULL,updated_ms INTEGER NOT NULL,bridge_attempt_id TEXT,expiry_ms INTEGER)")
                self.db.execute("PRAGMA application_id=%d" % APP_ID)
                self.db.execute("PRAGMA user_version=5")
        elif version == 1:
            with self.transaction():
                self.db.execute("ALTER TABLE attempts ADD COLUMN expiry_ms INTEGER")
                self.db.execute("ALTER TABLE config ADD COLUMN reverse INTEGER NOT NULL DEFAULT 0 CHECK(reverse IN (0,1))")
                self.db.execute("ALTER TABLE config ADD COLUMN cooldown_five_minutes INTEGER NOT NULL DEFAULT 0 CHECK(cooldown_five_minutes IN (0,1))")
                self.db.execute("ALTER TABLE config ADD COLUMN direction_mode TEXT NOT NULL DEFAULT 'SIGNAL' CHECK(direction_mode IN ('SIGNAL','REVERSE','LONG_ONLY','SHORT_ONLY'))")
                self.db.execute("PRAGMA user_version=5")
        elif version == 2:
            with self.transaction():
                self.db.execute("ALTER TABLE config ADD COLUMN reverse INTEGER NOT NULL DEFAULT 0 CHECK(reverse IN (0,1))")
                self.db.execute("ALTER TABLE config ADD COLUMN cooldown_five_minutes INTEGER NOT NULL DEFAULT 0 CHECK(cooldown_five_minutes IN (0,1))")
                self.db.execute("ALTER TABLE config ADD COLUMN direction_mode TEXT NOT NULL DEFAULT 'SIGNAL' CHECK(direction_mode IN ('SIGNAL','REVERSE','LONG_ONLY','SHORT_ONLY'))")
                self.db.execute("PRAGMA user_version=5")
        elif version == 3:
            with self.transaction():
                self.db.execute("ALTER TABLE config ADD COLUMN cooldown_five_minutes INTEGER NOT NULL DEFAULT 0 CHECK(cooldown_five_minutes IN (0,1))")
                self.db.execute("ALTER TABLE config ADD COLUMN direction_mode TEXT NOT NULL DEFAULT 'SIGNAL' CHECK(direction_mode IN ('SIGNAL','REVERSE','LONG_ONLY','SHORT_ONLY'))")
                self.db.execute("UPDATE config SET direction_mode='REVERSE' WHERE reverse=1")
                self.db.execute("PRAGMA user_version=5")
        elif version == 4:
            with self.transaction():
                self.db.execute("ALTER TABLE config ADD COLUMN direction_mode TEXT NOT NULL DEFAULT 'SIGNAL' CHECK(direction_mode IN ('SIGNAL','REVERSE','LONG_ONLY','SHORT_ONLY'))")
                self.db.execute("UPDATE config SET direction_mode='REVERSE' WHERE reverse=1")
                self.db.execute("PRAGMA user_version=5")
        if self.db.execute("PRAGMA user_version").fetchone()[0] == 5:
            with self.transaction():
                self.db.execute("ALTER TABLE attempts ADD COLUMN release_buffer_ms INTEGER NOT NULL DEFAULT 60000")
                self.db.execute("UPDATE attempts SET release_buffer_ms=300000 WHERE state='API_ACCEPTED_UNVERIFIED' AND (SELECT cooldown_five_minutes FROM config WHERE id=1)=1")
                self.db.execute("CREATE TABLE live_proxy_results(attempt_id TEXT PRIMARY KEY REFERENCES attempts(id),status TEXT NOT NULL,entry_price TEXT,exit_price TEXT,outcome TEXT,entry_event_ms INTEGER,exit_event_ms INTEGER,resolved_ms INTEGER NOT NULL)")
                self.db.execute("PRAGMA user_version=6")

    @contextmanager
    def transaction(self):
        self.db.execute("BEGIN IMMEDIATE")
        try:
            yield
            self.db.commit()
        except BaseException:
            self.db.rollback()
            raise

    def close(self):
        self.db.close()

    def recover_pending(self, now_ms: int) -> int:
        """Fail closed after restart; an interrupted send cannot be retried."""
        with self.transaction():
            row = self.db.execute("SELECT COUNT(*) FROM attempts WHERE state IN ('RESERVED','SUBMITTING')").fetchone()
            count = row[0]
            if count:
                self.db.execute("UPDATE attempts SET state='UNKNOWN',updated_ms=? WHERE state IN ('RESERVED','SUBMITTING')", (now_ms,))
                self.db.execute("UPDATE config SET enabled=0,updated_ms=? WHERE id=1", (now_ms,))
            return count

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()

    def enable(self, *, strategy: str, horizon: int, amount: str,
               max_amount: str, current_observation_id: int, now_ms: int,
               reverse: bool = False, cooldown_five_minutes: bool = False,
               direction_mode: str | None = None):
        stake, cap = _amount(amount), _amount(max_amount)
        mode = direction_mode if direction_mode is not None else ("REVERSE" if reverse else "SIGNAL")
        if (strategy not in SUPPORTED_STRATEGIES or type(horizon) is not int or horizon not in PERIOD_ENUMS or stake < 5 or stake > cap
                or type(current_observation_id) is not int or current_observation_id < 0
                or type(now_ms) is not int or type(reverse) is not bool
                or type(cooldown_five_minutes) is not bool or type(mode) is not str or mode not in DIRECTION_MODES
                or (reverse and direction_mode is not None and mode != "REVERSE")):
            raise LiveControlError("INVALID_LIVE_CONFIGURATION")
        with self.transaction():
            if self.db.execute("SELECT 1 FROM attempts WHERE state IN ('RESERVED','SUBMITTING','UNKNOWN','REJECTED_UNVERIFIED') LIMIT 1").fetchone():
                raise LiveControlError("UNRESOLVED_ATTEMPT")
            self.db.execute("INSERT INTO config(id,enabled,strategy,horizon,amount,cursor,updated_ms,reverse,cooldown_five_minutes,direction_mode) VALUES(1,1,?,?,?,?,?,?,?,?) ON CONFLICT(id) DO UPDATE SET enabled=1,strategy=excluded.strategy,horizon=excluded.horizon,amount=excluded.amount,cursor=excluded.cursor,updated_ms=excluded.updated_ms,reverse=excluded.reverse,cooldown_five_minutes=excluded.cooldown_five_minutes,direction_mode=excluded.direction_mode",
                            (strategy, horizon, str(stake), current_observation_id, now_ms, int(mode == "REVERSE"), int(cooldown_five_minutes), mode))

    def disable(self, now_ms: int):
        with self.transaction():
            self.db.execute("UPDATE config SET enabled=0,updated_ms=? WHERE id=1", (now_ms,))

    def state(self):
        row = self.db.execute("SELECT * FROM config WHERE id=1").fetchone()
        return dict(row) if row else None

    def reserve(self, row: dict, now_ms: int):
        if not isinstance(row, dict) or type(row.get("id")) is not int:
            raise LiveControlError("INVALID_OBSERVATION")
        observation_id = row["id"]
        with self.transaction():
            config = self.db.execute("SELECT * FROM config WHERE id=1").fetchone()
            if not config or observation_id <= config["cursor"]:
                return None
            if self.db.execute("SELECT 1 FROM attempts WHERE state IN ('RESERVED','SUBMITTING','UNKNOWN','API_ACCEPTED_UNVERIFIED','REJECTED_UNVERIFIED') LIMIT 1").fetchone():
                raise LiveControlError("UNRESOLVED_ATTEMPT")
            if not config["enabled"]:
                return None
            if type(row.get("entry_ms")) is int and row["entry_ms"] > now_ms:
                return None
            # Consume the observation before any external action. A restart must
            # not replay a signal whose prior send outcome is uncertain.
            self.db.execute("UPDATE config SET cursor=?,updated_ms=? WHERE id=1", (observation_id, now_ms))
            direction = eligible_signal(row, now_ms, strategy=config["strategy"])
            if direction is None:
                return None
            mode = config["direction_mode"]
            if ((mode == "LONG_ONLY" and direction != "LONG")
                    or (mode == "SHORT_ONLY" and direction != "SHORT")):
                return None
            if mode == "REVERSE":
                direction = "SHORT" if direction == "LONG" else "LONG"
            ident = uuid4().hex
            self.db.execute("INSERT INTO attempts(id,observation_id,direction,amount,state,created_ms,updated_ms,bridge_attempt_id,expiry_ms,release_buffer_ms) VALUES(?,?,?,?,?,?,?,NULL,?,?)",
                            (ident, observation_id, direction, config["amount"],
                             "RESERVED", now_ms, now_ms, row["entry_ms"] + config["horizon"] * 60_000,
                             300000 if config["cooldown_five_minutes"] else 60000))
            return dict(self.db.execute("SELECT * FROM attempts WHERE id=?", (ident,)).fetchone())

    def mark_unknown(self, ident: str, now_ms: int):
        with self.transaction():
            row = self.db.execute("SELECT state FROM attempts WHERE id=?", (ident,)).fetchone()
            if not row or row["state"] not in ("RESERVED", "SUBMITTING"):
                raise LiveControlError("INVALID_ATTEMPT_TRANSITION")
            self.db.execute("UPDATE attempts SET state='UNKNOWN',updated_ms=? WHERE id=?", (now_ms, ident))
            self.db.execute("UPDATE config SET enabled=0,updated_ms=? WHERE id=1", (now_ms,))

    def mark_submitting(self, ident: str, now_ms: int, bridge_attempt_id: str):
        if not isinstance(bridge_attempt_id, str) or len(bridge_attempt_id) != 32:
            raise LiveControlError("INVALID_BRIDGE_ATTEMPT_ID")
        with self.transaction():
            row = self.db.execute("SELECT state FROM attempts WHERE id=?", (ident,)).fetchone()
            if not row or row["state"] != "RESERVED":
                raise LiveControlError("INVALID_ATTEMPT_TRANSITION")
            self.db.execute("UPDATE attempts SET state='SUBMITTING',updated_ms=?,bridge_attempt_id=? WHERE id=?",
                            (now_ms, bridge_attempt_id, ident))

    def finish(self, ident: str, now_ms: int, state: str, bridge_attempt_id: str | None = None):
        if state not in ("API_ACCEPTED_UNVERIFIED", "REJECTED_UNVERIFIED", "UNKNOWN", "SKIPPED"):
            raise LiveControlError("INVALID_ATTEMPT_STATE")
        expected = "RESERVED" if state == "SKIPPED" else "SUBMITTING"
        with self.transaction():
            row = self.db.execute("SELECT state FROM attempts WHERE id=?", (ident,)).fetchone()
            if not row or row["state"] != expected:
                raise LiveControlError("INVALID_ATTEMPT_TRANSITION")
            self.db.execute("UPDATE attempts SET state=?,updated_ms=?,bridge_attempt_id=COALESCE(?,bridge_attempt_id) WHERE id=?",
                            (state, now_ms, bridge_attempt_id, ident))
            # An accepted response enters a 30M cooldown, then releases without
            # claiming a fill. Other outcomes stop unattended trading.
            if state != "API_ACCEPTED_UNVERIFIED":
                self.db.execute("UPDATE config SET enabled=0,updated_ms=? WHERE id=1", (now_ms,))

    def release_accepted(self, ident: str, now_ms: int, bridge_attempt_id: str, *, buffer_ms: int):
        if type(buffer_ms) is not int or not 60000 <= buffer_ms <= 600000:
            raise LiveControlError("INVALID_RELEASE_BUFFER")
        with self.transaction():
            row = self.db.execute("SELECT state,bridge_attempt_id,expiry_ms FROM attempts WHERE id=?", (ident,)).fetchone()
            if (not row or row["state"] != "API_ACCEPTED_UNVERIFIED"
                    or row["bridge_attempt_id"] != bridge_attempt_id
                    or row["expiry_ms"] is None
                    or now_ms < row["expiry_ms"] + buffer_ms):
                raise LiveControlError("RELEASE_NOT_ALLOWED")
            self.db.execute("UPDATE attempts SET state='TIME_EXPIRED_UNVERIFIED',updated_ms=? WHERE id=?",
                            (now_ms, ident))

    def attempts(self):
        return [dict(row) for row in self.db.execute("SELECT * FROM attempts ORDER BY created_ms DESC")]

    def review(self, ident: str, bridge_attempt_id: str, now_ms: int):
        """Record a human check, without claiming the exchange confirmed a fill."""
        with self.transaction():
            row = self.db.execute("SELECT state,bridge_attempt_id FROM attempts WHERE id=?", (ident,)).fetchone()
            if (not row or row["state"] not in
                    ("API_ACCEPTED_UNVERIFIED", "REJECTED_UNVERIFIED", "UNKNOWN")
                    or row["bridge_attempt_id"] != bridge_attempt_id):
                raise LiveControlError("INVALID_REVIEW_TRANSITION")
            self.db.execute("UPDATE attempts SET state='USER_REVIEWED_UNVERIFIED',updated_ms=? WHERE id=?",
                            (now_ms, ident))

    def attempt(self, ident: str):
        row = self.db.execute("SELECT * FROM attempts WHERE id=?", (ident,)).fetchone()
        return dict(row) if row else None


class LiveExecutor:
    """Submit fresh signals serially through the bridge while armed."""

    def __init__(self, control: LiveControl, bridge, *, clock=None):
        self.control = control
        self.bridge = bridge
        self.clock = clock or (lambda: time.time_ns() // 1_000_000)

    def process(self, observation: dict, now_ms: int):
        attempt = self.control.reserve(observation, now_ms)
        if attempt is None:
            return None
        ident = attempt["id"]
        horizon_ms = attempt["expiry_ms"] - observation["entry_ms"]
        if horizon_ms % 60_000 or horizon_ms // 60_000 not in PERIOD_ENUMS:
            self.control.finish(ident, now_ms, "SKIPPED")
            return self.control.attempt(ident)
        minutes = horizon_ms // 60_000
        try:
            status = self.bridge.status()
            if (not isinstance(status.get("history"), list)
                    or any(row.get("acknowledged_ms") is None and row.get("auto_released_ms") is None
                           for row in status["history"])):
                raise LiveControlError("BRIDGE_RECONCILE_REQUIRED")
            quote = self.bridge.quote(minutes, attempt["direction"])
            if (quote.get("symbol") != "BTCUSDT" or quote.get("minutes") != minutes
                    or quote.get("direction") != attempt["direction"]
                    or quote.get("enum") != PERIOD_ENUMS[minutes]
                    or not 0 < _amount(quote.get("ratio")) <= 1):
                raise LiveControlError("INVALID_BRIDGE_QUOTE")
            preview = self.bridge.preview(minutes, attempt["direction"], attempt["amount"])
            preview_id = preview.get("id")
            if (not isinstance(preview_id, str) or len(preview_id) != 32
                    or preview.get("amount") != attempt["amount"]):
                raise LiveControlError("INVALID_BRIDGE_PREVIEW")
        except Exception:
            # No external order POST occurred in this branch.
            self.control.finish(ident, now_ms, "SKIPPED")
            return self.control.attempt(ident)
        version = json.loads(observation["decision"]).get("strategy_version")
        max_lag = INTRABAR_ENTRY_LAG_MS if version == STRATEGY else MAX_ENTRY_LAG_MS
        if not observation["entry_ms"] <= self.clock() <= observation["entry_ms"] + max_lag:
            self.control.finish(ident, self.clock(), "SKIPPED")
            return self.control.attempt(ident)
        self.control.mark_submitting(ident, now_ms, preview_id)
        try:
            result = self.bridge.submit(preview_id)
            state = {"ACCEPTED_UNVERIFIED": "API_ACCEPTED_UNVERIFIED",
                     "REJECTED_UNVERIFIED": "REJECTED_UNVERIFIED"}.get(result.get("status"), "UNKNOWN")
            bridge_id = result.get("id") if result.get("id") == preview_id else None
            if bridge_id is None:
                state = "UNKNOWN"
        except Exception:
            state, bridge_id = "UNKNOWN", None
        self.control.finish(ident, now_ms, state, bridge_id)
        return self.control.attempt(ident)
