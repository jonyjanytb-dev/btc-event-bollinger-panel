"""Offline BTC event-contract *paper* lifecycle; deliberately has no exchange client.

The 85% payout is a research assumption, not a live quote. Official Binance
rules state that equal Index Price at expiry refunds the Premium, but a
SHADOW proxy does not prove the exact next-second exchange strike or settlement.
Never submit an exchange order here.
"""
from __future__ import annotations

import hashlib
import json
import math
import sqlite3
from decimal import Decimal, InvalidOperation
from pathlib import Path

from shadow.store import validate_candidate
from .official_rules import FAQ_URL, proxy_equal_result

PAYOUT = Decimal("0.85")
MARGIN = Decimal("0.03")
THRESHOLD = Decimal(1) / (Decimal(1) + PAYOUT) + MARGIN
APP_ID = 1162238544
DATABASE_VERSION = 1
SCHEMA = """
CREATE TABLE IF NOT EXISTS decisions(
  id TEXT PRIMARY KEY, snapshot_hash TEXT NOT NULL, decision_ms INTEGER NOT NULL,
  entry_ms INTEGER NOT NULL, expiry_ms INTEGER NOT NULL, mechanism TEXT NOT NULL,
  horizon_minutes INTEGER NOT NULL, direction TEXT NOT NULL,
  probability TEXT NOT NULL, virtual_stake TEXT NOT NULL,
  status TEXT NOT NULL, reasons TEXT NOT NULL, created_ms INTEGER NOT NULL,
  settled_ms INTEGER, proxy_direction TEXT, hypothetical_pnl TEXT,
  interpretation TEXT NOT NULL,
  UNIQUE(mechanism, horizon_minutes, entry_ms)
);
CREATE TABLE IF NOT EXISTS events(
  id INTEGER PRIMARY KEY, decision_id TEXT NOT NULL,
  type TEXT NOT NULL, time_ms INTEGER NOT NULL, detail TEXT NOT NULL
);
"""


class PaperEvidenceError(ValueError):
    """Invalid source evidence or unsafe paper state change."""


def _decimal(value, *, positive=False):
    try:
        x = Decimal(str(value))
    except (ValueError, TypeError, InvalidOperation):
        raise PaperEvidenceError("invalid decimal evidence") from None
    if not x.is_finite() or (positive and x <= 0):
        raise PaperEvidenceError("invalid decimal evidence")
    return x


def _int(value):
    if type(value) is not int:
        raise PaperEvidenceError("invalid integer evidence")
    return value


def _meta(row):
    raw = row.get("metadata")
    try:
        payload = json.loads(raw) if isinstance(raw, str) else raw
    except (TypeError, ValueError):
        raise PaperEvidenceError("invalid prediction metadata") from None
    if not isinstance(payload, dict):
        raise PaperEvidenceError("invalid prediction metadata")
    return payload


def _source(row):
    if row.get("classification") != "shadow_v1":
        raise PaperEvidenceError("not a formal SHADOW prediction")
    meta = _meta(row)
    observed = _int(row.get("recorded_ms"))
    try:
        validate_candidate(meta, observed)
    except (KeyError, TypeError, ValueError) as exc:
        raise PaperEvidenceError("formal SHADOW evidence rejected") from exc

    for key in ("symbol", "mechanism", "horizon_minutes", "decision_ms",
                "entry_ms", "expiry_ms", "model_version", "screen_version",
                "probability_up", "candidate_direction", "execution_decision"):
        if row.get(key) != meta.get(key):
            raise PaperEvidenceError("prediction columns disagree with evidence")
    if row.get("screen_passed") not in (0, 1) or bool(row["screen_passed"]) is not meta["screen_passed"]:
        raise PaperEvidenceError("screen evidence mismatch")
    if row.get("no_trade_reasons") is not None:
        try:
            reasons = json.loads(row["no_trade_reasons"])
        except (TypeError, ValueError):
            raise PaperEvidenceError("invalid no-trade reasons") from None
        if reasons != meta["no_trade_reasons"]:
            raise PaperEvidenceError("no-trade reasons mismatch")
    if row.get("features") is not None:
        try:
            features = json.loads(row["features"])
        except (TypeError, ValueError):
            raise PaperEvidenceError("invalid feature evidence") from None
        if features != meta["features"]:
            raise PaperEvidenceError("feature evidence mismatch")

    identity = hashlib.sha256(
        f"BTCUSDT:{meta['mechanism']}:{meta['horizon_minutes']}:{meta['entry_ms']}".encode()
    ).hexdigest()
    if row.get("id") != identity:
        raise PaperEvidenceError("invalid SHADOW slot identifier")
    if meta["research_payout_ratio"] != "0.85" or meta["execution_decision"] != "NO TRADE":
        raise PaperEvidenceError("research payout or execution changed")
    return meta, json.dumps(meta, sort_keys=True, separators=(",", ":"), allow_nan=False)


def assess_signal(row, now_ms):
    """Evaluate a forward-captured signal without implying an executable quote."""
    meta, serialized = _source(row)
    now_ms = _int(now_ms)
    recorded = _int(row["recorded_ms"])
    if now_ms < recorded or now_ms >= meta["entry_ms"]:
        raise PaperEvidenceError("not a prospective prediction")

    confidence = max(_decimal(meta["probability_up"]), Decimal(1) - _decimal(meta["probability_up"]))
    reasons = ["real_quote_unverified", "actual_exchange_strike_and_order_confirmation_unverified"]
    if not meta["screen_passed"]:
        reasons.append("historical_screen_failed")
    if confidence <= THRESHOLD:
        reasons.append("confidence_below_85pct_research_threshold")
    eligible = meta["screen_passed"] and confidence > THRESHOLD
    return {
        "prediction_id": row["id"], "eligible_for_virtual_tracking": eligible,
        "exchange_execution_decision": "NO TRADE",
        "hypothetical_payout_ratio": "0.85",
        "hypothetical_break_even": str(Decimal(1) / (Decimal(1)+PAYOUT)),
        "hypothetical_threshold": str(THRESHOLD),
        "confidence": str(confidence),
        "reasons": reasons,
        "snapshot_hash": hashlib.sha256(serialized.encode()).hexdigest(),
        "evidence": meta,
    }


def _price(value):
    return _decimal(value, positive=True)


class PaperLedger:
    """Separate SQLite journal for prospective virtual decisions only."""

    def __init__(self, path):
        path = Path(path).expanduser()
        if path.is_symlink() or path.name in ("shadow.sqlite", "market.sqlite"):
            raise PaperEvidenceError("paper database must be separate")
        if not path.parent.is_dir():
            raise PaperEvidenceError("paper parent directory not found")
        self.db = sqlite3.connect(str(path), timeout=15)
        self.db.row_factory = sqlite3.Row
        old_version = self.db.execute("PRAGMA user_version").fetchone()[0]
        app = self.db.execute("PRAGMA application_id").fetchone()[0]
        count = self.db.execute(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
        ).fetchone()[0]
        if (old_version == 0 and count) or old_version not in (0, DATABASE_VERSION) or (old_version and app != APP_ID):
            self.db.close()
            raise PaperEvidenceError("refusing unrelated or unsupported paper database")
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA synchronous=FULL")
        if old_version == 0:
            self.db.executescript(
                "BEGIN;\n" + SCHEMA + f"\nPRAGMA application_id={APP_ID};"
                + f"\nPRAGMA user_version={DATABASE_VERSION};\nCOMMIT;"
            )

    def close(self):
        self.db.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        self.close()

    def record(self, row, *, now_ms, virtual_stake, max_parallel=1):
        """Record a pre-entry paper scenario; always leave live exchange NO TRADE."""
        # Recovery is idempotent even after the entry deadline. Only an
        # already-persisted, byte-equivalent pre-entry decision may be replayed;
        # a brand new backdated entry is always rejected.
        meta, serialized = _source(row)
        evidence_hash = hashlib.sha256(serialized.encode()).hexdigest()
        stake = _decimal(virtual_stake, positive=True)
        if type(max_parallel) is not int or not 1 <= max_parallel <= 100:
            raise PaperEvidenceError("invalid virtual exposure limit")
        ident = row["id"]
        with self.db:
            self.db.execute("BEGIN IMMEDIATE")
            previous = self.db.execute("SELECT * FROM decisions WHERE id=?", (ident,)).fetchone()
            if previous:
                if previous["snapshot_hash"] != evidence_hash or previous["virtual_stake"] != str(stake):
                    raise PaperEvidenceError("conflicting paper decision")
                return dict(previous)
            assessed = assess_signal(row, now_ms)
            selected = bool(assessed["eligible_for_virtual_tracking"])
            reasons = list(assessed["reasons"])
            if selected:
                overlapping = self.db.execute(
                    "SELECT COUNT(*) FROM decisions WHERE status='TRACKING' "
                    "AND entry_ms<? AND expiry_ms>?",
                    (meta["expiry_ms"], meta["entry_ms"]),
                ).fetchone()[0]
                if overlapping >= max_parallel:
                    selected = False
                    reasons.append("virtual_exposure_limit")
            status = "TRACKING" if selected else "NOT_SELECTED"
            self.db.execute(
                "INSERT INTO decisions(id,snapshot_hash,decision_ms,entry_ms,expiry_ms,"
                "mechanism,horizon_minutes,direction,probability,virtual_stake,status,"
                "reasons,created_ms,interpretation) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (ident,assessed["snapshot_hash"],meta["decision_ms"],meta["entry_ms"],
                 meta["expiry_ms"],meta["mechanism"],meta["horizon_minutes"],
                 meta["candidate_direction"],str(meta["probability_up"]),str(stake),
                 status,json.dumps(reasons,sort_keys=True),now_ms,
                 "INDEX_PROXY_AND_ASSUMED_85PCT_PAYOUT_NOT_REAL_PNL"),
            )
            self.db.execute(
                "INSERT INTO events(decision_id,type,time_ms,detail) VALUES(?,?,?,?)",
                (ident, "PAPER_"+status, now_ms, json.dumps({"reasons":reasons})),
            )
        return dict(self.db.execute("SELECT * FROM decisions WHERE id=?", (ident,)).fetchone())

    def reconcile(self, shadow_row, *, now_ms, proxy_max_lag_ms=5000):
        """Score ONLY previously tracked signals from *proxy* snapshots.

        The Binance Event Contract settlement price, payout and tie rules are
        not proven; outputs never claim actual order or actual realized P&L.
        """
        now_ms = _int(now_ms)
        lag = _int(proxy_max_lag_ms)
        if lag < 0:
            raise PaperEvidenceError("invalid proxy lag")
        meta, serialized = _source(shadow_row)
        ident = shadow_row["id"]
        with self.db:
            self.db.execute("BEGIN IMMEDIATE")
            old = self.db.execute("SELECT * FROM decisions WHERE id=?", (ident,)).fetchone()
            if not old:
                raise PaperEvidenceError("paper position was never recorded before entry")
            if old["snapshot_hash"] != hashlib.sha256(serialized.encode()).hexdigest():
                raise PaperEvidenceError("historical prediction changed")
            if old["status"] != "TRACKING":
                return dict(old)
            if now_ms < meta["expiry_ms"] + lag:
                raise PaperEvidenceError("proxy expiry window still open")
            outcome = shadow_row.get("result_status")
            if outcome == "PENDING":
                return dict(old)
            if outcome == "MISSING":
                status, direction, pnl = "PROXY_MISSING", None, None
            elif outcome == "VERIFIED":
                entry = _price(shadow_row.get("entry_price"))
                exit_price = _price(shadow_row.get("exit_price"))
                for stem, target in (("entry", meta["entry_ms"]),("exit", meta["expiry_ms"])):
                    event = _int(shadow_row.get(stem+"_event_ms"))
                    received = _int(shadow_row.get(stem+"_received_ms"))
                    if not target <= event <= target+lag or received > target+lag:
                        raise PaperEvidenceError("proxy receipt outside evidence window")
                direction = "HIGHER" if exit_price > entry else "LOWER" if exit_price < entry else "EQUAL"
                if shadow_row.get("direction_result") != direction:
                    raise PaperEvidenceError("proxy direction mismatch")
                if direction == "EQUAL":
                    # Official Binance product rules refund the Premium when
                    # expiry Index Price equals the Strike Price. This remains
                    # a hypothetical proxy result: our entry tick is not an
                    # authenticated exchange strike and no real order exists.
                    if not proxy_equal_result(entry, exit_price):
                        raise PaperEvidenceError("proxy equality check inconsistent")
                    status, pnl = "PROXY_TIE_REFUND", "0"
                else:
                    status = "PROXY_SCORED"
                    stake = Decimal(old["virtual_stake"])
                    pnl = str(stake * PAYOUT if direction == old["direction"] else -stake)
            else:
                raise PaperEvidenceError("unknown proxy outcome")
            self.db.execute(
                "UPDATE decisions SET status=?,settled_ms=?,proxy_direction=?,hypothetical_pnl=? WHERE id=?",
                (status,now_ms,direction,pnl,ident),
            )
            self.db.execute(
                "INSERT INTO events(decision_id,type,time_ms,detail) VALUES(?,?,?,?)",
                (ident,status,now_ms,json.dumps({"proxy_direction":direction,
                                                "hypothetical_pnl":pnl,
                                                "actual_contract_settlement_verified":False,
                                                "official_tie_rule_reference":FAQ_URL if direction=="EQUAL" else None})),
            )
        return dict(self.db.execute("SELECT * FROM decisions WHERE id=?", (ident,)).fetchone())

    def decisions(self):
        return [dict(x) for x in self.db.execute("SELECT * FROM decisions ORDER BY entry_ms,id")]

    def events(self):
        return [dict(x) for x in self.db.execute("SELECT * FROM events ORDER BY id")]
