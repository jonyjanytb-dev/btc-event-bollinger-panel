"""Durable *local* attempt evidence journal. NO exchange send implementation.

This is a reconciliation state machine for Codex's separately owned order
client, NOT a wrapper that sends orders or enables live execution.
"""
from __future__ import annotations

import hashlib
import json
import sqlite3
from decimal import Decimal
from pathlib import Path

from .contract_evidence import (
    EvidenceError, _int, _money, _sha, _text,
    classify_order_response, inspect_settlement_proof,
)

APP_ID = 1162894409
DATABASE_VERSION = 1
LOCKED_STATES = (
    "PREPARED", "SUBMITTING", "UNKNOWN", "API_ACCEPTED_UNVERIFIED",
    "ORDER_CONFIRMED", "POSITION_OPEN"
)
SCHEMA = """
CREATE TABLE IF NOT EXISTS attempts(
 id TEXT PRIMARY KEY, symbol TEXT NOT NULL, horizon_minutes INTEGER NOT NULL,
 direction TEXT NOT NULL, entry_ms INTEGER NOT NULL, expiry_ms INTEGER NOT NULL,
 stake TEXT NOT NULL, request_sha256 TEXT NOT NULL,
 state TEXT NOT NULL, created_ms INTEGER NOT NULL, updated_ms INTEGER NOT NULL,
 order_id TEXT, exchange_submission_enabled INTEGER NOT NULL DEFAULT 0,
 UNIQUE(symbol,horizon_minutes,entry_ms)
);
CREATE TABLE IF NOT EXISTS evidence_events(
 id INTEGER PRIMARY KEY, attempt_id TEXT NOT NULL, event_kind TEXT NOT NULL,
 observed_ms INTEGER NOT NULL, source_ref TEXT NOT NULL,
 raw_sha256 TEXT, detail TEXT NOT NULL
);
"""


class OrderAttemptJournal:
    """Append-only evidence timeline and permanently unique event slots."""

    def __init__(self, path):
        path = Path(path).expanduser()
        if path.is_symlink() or path.name in ("shadow.sqlite","market.sqlite","paper.sqlite"):
            raise EvidenceError("use an isolated attempts database")
        if not path.parent.is_dir():
            raise EvidenceError("attempts parent directory missing")
        self.db = sqlite3.connect(str(path), timeout=15)
        self.db.row_factory = sqlite3.Row
        ver=self.db.execute("PRAGMA user_version").fetchone()[0]
        app=self.db.execute("PRAGMA application_id").fetchone()[0]
        table_count=self.db.execute(
            "SELECT count(*) FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
        ).fetchone()[0]
        if (ver==0 and table_count) or ver not in (0,DATABASE_VERSION) or (ver and app!=APP_ID):
            self.db.close()
            raise EvidenceError("unrelated or unsupported attempts database")
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA synchronous=FULL")
        if ver==0:
            self.db.executescript(
                "BEGIN;\n"+SCHEMA+f"\nPRAGMA application_id={APP_ID};"
                +f"\nPRAGMA user_version={DATABASE_VERSION};\nCOMMIT;"
            )

    def close(self):
        self.db.close()

    def __enter__(self):
        return self

    def __exit__(self,exc_type,exc,tb):
        self.close()

    def _row(self, ident):
        row=self.db.execute("SELECT * FROM attempts WHERE id=?", (ident,)).fetchone()
        if not row:
            raise EvidenceError("attempt not found")
        return row

    def _log(self, ident, event, now, source, digest=None, detail=None):
        self.db.execute(
            "INSERT INTO evidence_events(attempt_id,event_kind,observed_ms,"
            "source_ref,raw_sha256,detail) VALUES(?,?,?,?,?,?)",
            (ident,event,now,source,digest,
             json.dumps(detail or {},sort_keys=True,separators=(",",":")))
        )

    def reserve(self, *, horizon_minutes, direction, entry_ms, stake,
                request_sha256, now_ms, max_parallel, max_stake):
        """Durably reserve a slot BEFORE any separately authorized network send."""
        if horizon_minutes not in (30,60) or direction not in ("HIGHER","LOWER"):
            raise EvidenceError("unsupported profile")
        entry=_int(entry_ms,"entry_ms")
        now=_int(now_ms,"now_ms")
        if entry%60_000 or type(max_parallel) is not int or max_parallel<1:
            raise EvidenceError("invalid event entry or risk cap")
        amount=_money(stake,"stake",positive=True)
        limit=_money(max_stake,"max_stake",positive=True)
        if amount>limit:
            raise EvidenceError("stake exceeds approved local risk limit")
        digest=_sha(request_sha256,"request_sha256")
        expiry=entry+horizon_minutes*60_000
        ident=hashlib.sha256(
            f"BTCUSDT:{horizon_minutes}:{entry}:{direction}:{digest}".encode()
        ).hexdigest()
        with self.db:
            self.db.execute("BEGIN IMMEDIATE")
            existing=self.db.execute(
                "SELECT * FROM attempts WHERE symbol='BTCUSDT' AND horizon_minutes=? AND entry_ms=?",
                (horizon_minutes,entry),
            ).fetchone()
            if existing:
                if existing["id"]!=ident or Decimal(existing["stake"])!=amount:
                    raise EvidenceError("conflicting attempt in permanent event slot")
                return dict(existing)
            # Only exact pre-committed attempts may be replayed after the
            # planned entry. New retrospective intents are never created.
            if entry<=now:
                raise EvidenceError("cannot reserve an expired event entry")
            # UNKNOWN, API_ACCEPTED_UNVERIFIED or an interrupted SUBMITTING
            # remain global freezes, even after their predicted expiry.
            uncertain=self.db.execute(
                "SELECT count(*) FROM attempts WHERE state IN "
                "('SUBMITTING','UNKNOWN','API_ACCEPTED_UNVERIFIED')"
            ).fetchone()[0]
            if uncertain:
                raise EvidenceError("unresolved external order blocks new attempts")
            overlapping=self.db.execute(
                "SELECT count(*) FROM attempts WHERE state IN ("+
                ",".join("?" for _ in LOCKED_STATES)+
                ") AND entry_ms<? AND expiry_ms>?",
                (*LOCKED_STATES,expiry,entry),
            ).fetchone()[0]
            if overlapping>=max_parallel:
                raise EvidenceError("independent attempt exposure cap reached")
            self.db.execute(
                "INSERT INTO attempts(id,symbol,horizon_minutes,direction,entry_ms,expiry_ms,"
                "stake,request_sha256,state,created_ms,updated_ms) "
                "VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                (ident,"BTCUSDT",horizon_minutes,direction,entry,expiry,
                 str(amount),digest,"PREPARED",now,now),
            )
            self._log(ident,"PREPARED",now,"LOCAL_PRECOMMIT",digest,
                      {"exchange_submission_enabled":False})
        return dict(self._row(ident))

    def _transition(self, ident, now_ms, allowed, new_state, source_ref,
                    raw_sha256=None, detail=None, order_id=None):
        now=_int(now_ms,"now_ms")
        source=_text(source_ref,"source_ref")
        if raw_sha256 is not None:
            _sha(raw_sha256,"raw_sha256")
        with self.db:
            self.db.execute("BEGIN IMMEDIATE")
            current=self._row(ident)
            if current["state"] not in allowed or now<current["updated_ms"]:
                raise EvidenceError("unsafe or retroactive order transition")
            if order_id is not None:
                _text(order_id,"order_id")
                if current["order_id"] not in (None,order_id):
                    raise EvidenceError("exchange order identity conflict")
            self.db.execute(
                "UPDATE attempts SET state=?,updated_ms=?,order_id=COALESCE(?,order_id) WHERE id=?",
                (new_state,now,order_id,ident),
            )
            self._log(ident,new_state,now,source,raw_sha256,detail)
        return dict(self._row(ident))

    def note_external_submit_started(self, ident, *, now_ms, adapter_reference):
        """Records an OUTSIDE client's reported send start; this method does NOT send."""
        row=self._row(ident)
        if now_ms>row["entry_ms"]:
            raise EvidenceError("external submission recorded after intended entry")
        return self._transition(ident,now_ms,("PREPARED",),"SUBMITTING",adapter_reference)

    def note_external_response(self, ident, *, now_ms, network_outcome,
                               http_status=None, payload=None, raw_sha256=None):
        """Never turns HTTP acceptance into confirmed order or safe retry."""
        result=classify_order_response(
            network_outcome=network_outcome,http_status=http_status,payload=payload
        )
        if network_outcome=="RESPONSE" and raw_sha256 is None:
            raise EvidenceError("missing raw response evidence digest")
        return self._transition(
            ident,now_ms,("SUBMITTING",),result["state"],
            "EXTERNAL_ADAPTER_RESPONSE",raw_sha256,
            {"classification":result["state"],"safe_to_retry":False,
             "claimed_order_id_unverified":result.get("claimed_order_id")}
        )

    def confirm_with_independent_readonly_proof(
        self,ident,*,now_ms,order_id,source_ref,raw_sha256,verifier
    ):
        """Requires an external trusted read-only verifier; none bundled here."""
        _sha(raw_sha256,"raw_sha256")
        _text(order_id,"order_id")
        if verifier is None or not callable(verifier) or verifier(
            {"order_id":order_id,"source_ref":source_ref,"raw_sha256":raw_sha256}
        ) is not True:
            raise EvidenceError("independent order identity proof not verified")
        return self._transition(
            ident,now_ms,("UNKNOWN","API_ACCEPTED_UNVERIFIED"),
            "ORDER_CONFIRMED",source_ref,raw_sha256,
            {"source":"INDEPENDENT_READ_ONLY_ORDER_QUERY"},order_id=order_id
        )

    def note_position_open(self,ident,*,now_ms,source_ref,raw_sha256,verifier):
        if verifier is None or not callable(verifier) or verifier(
            {"source_ref":source_ref,"raw_sha256":raw_sha256}
        ) is not True:
            raise EvidenceError("position open proof not verified")
        return self._transition(
            ident,now_ms,("ORDER_CONFIRMED",),"POSITION_OPEN",
            source_ref,raw_sha256
        )

    def note_verified_settlement(self,ident,*,now_ms,proof,verifier):
        row=self._row(ident)
        if not row["order_id"]:
            raise EvidenceError("no order id for settlement")
        result=inspect_settlement_proof(proof,expected_order_id=row["order_id"])
        if (result["horizon_minutes"]!=row["horizon_minutes"] or
            result["direction"]!=row["direction"] or
            _int(proof.get("entry_ms"),"entry_ms")!=row["entry_ms"] or
            _int(proof.get("expiry_ms"),"expiry_ms")!=row["expiry_ms"]):
            raise EvidenceError("settlement does not match request")
        if Decimal(result["stake"])!=Decimal(row["stake"]):
            raise EvidenceError("settlement amount does not match request")
        if now_ms<row["expiry_ms"] or verifier is None or not callable(verifier) or verifier(proof) is not True:
            raise EvidenceError("read-only settlement evidence not independently verified")
        return self._transition(
            ident,now_ms,("POSITION_OPEN",),"SETTLED",
            result["source_ref"],result["source_hash"],
            {"gross_return":result["gross_return"],"stake":result["stake"],
             "net_before_unverified_fees":result["gross_minus_stake"]}
        )

    def note_result_recorded(self,ident,*,now_ms,source_ref,raw_sha256):
        """Marks manual reconciliation of an already independently settled order."""
        return self._transition(
            ident,now_ms,("SETTLED",),"RESULT_RECORDED",
            source_ref,raw_sha256
        )

    def attempts(self):
        return [dict(x) for x in self.db.execute("SELECT * FROM attempts ORDER BY entry_ms,id")]

    def events(self,ident=None):
        if ident:
            return [dict(x) for x in self.db.execute(
                "SELECT * FROM evidence_events WHERE attempt_id=? ORDER BY id",(ident,)
            )]
        return [dict(x) for x in self.db.execute("SELECT * FROM evidence_events ORDER BY id")]
