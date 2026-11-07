"""Explicitly started, read-only SHADOW -> isolated paper ledger bridge.

NO Binance HTTP, OAuth, browser, trade transport or production-service hooks.
Never reconstruct missed pre-entry predictions after their deadline.
"""
from __future__ import annotations

import argparse
import json
import math
import os
import sqlite3
import time
from pathlib import Path

from shadow.audit import APP_ID as SHADOW_APP_ID, audit_rows
from .paper import PaperEvidenceError, PaperLedger


def next_poll_delay(now_ms, poll_seconds, fast_poll_seconds, pre_entry_window_seconds):
    """Poll quickly near UTC minute entry boundaries, normally at a low rate."""
    values=(poll_seconds,fast_poll_seconds,pre_entry_window_seconds)
    if (type(now_ms) is not int or now_ms<0 or
        any(not isinstance(x,(int,float)) or isinstance(x,bool) or not math.isfinite(x) for x in values) or
        poll_seconds<=0 or fast_poll_seconds<=0 or fast_poll_seconds>poll_seconds or
        not 0<pre_entry_window_seconds<60):
        raise ValueError('invalid paper polling configuration')
    seconds_to_entry=(60_000-now_ms%60_000)/1000
    if seconds_to_entry<=pre_entry_window_seconds:
        return min(fast_poll_seconds,seconds_to_entry)
    return min(poll_seconds,seconds_to_entry-pre_entry_window_seconds)


def _read_snapshot(shadow_path, now_ms, max_lag_ms):
    """Take a single read-only SQLite snapshot for BOTH audit and consumption."""
    path=Path(shadow_path).expanduser().resolve()
    if not path.is_file():
        raise FileNotFoundError("shadow database not found")
    with sqlite3.connect(path.as_uri()+"?mode=ro",uri=True,timeout=5) as db:
        db.row_factory=sqlite3.Row
        db.execute("PRAGMA query_only=ON")
        if (db.execute("PRAGMA application_id").fetchone()[0]!=SHADOW_APP_ID or
            db.execute("PRAGMA user_version").fetchone()[0]!=2):
            raise PaperEvidenceError("unsupported or unrelated SHADOW database")
        db.execute("BEGIN")
        try:
            rows=[dict(x) for x in db.execute(
                "SELECT * FROM predictions ORDER BY recorded_ms,id"
            )]
            def tick_exists(event_ms,received_ms,price):
                return db.execute(
                    "SELECT 1 FROM ticks WHERE event_ms=? AND received_ms=? "
                    "AND index_price=? LIMIT 1",
                    (event_ms,received_ms,str(price)),
                ).fetchone() is not None

            def first_tick_key(target_ms,lag_ms):
                found=db.execute(
                    "SELECT event_ms,received_ms FROM ticks "
                    "WHERE event_ms>=? AND event_ms<=? AND received_ms<=? "
                    "ORDER BY event_ms,received_ms LIMIT 1",
                    (target_ms,target_ms+lag_ms,target_ms+lag_ms),
                ).fetchone()
                return (found["event_ms"],found["received_ms"]) if found else None

            report=audit_rows(
                rows,now_ms,max_lag_ms,tick_exists=tick_exists,
                tick_first=first_tick_key,
            )
            report["unresolved_second_gaps"]=db.execute(
                "SELECT count(*) FROM gaps WHERE status='OPEN'"
            ).fetchone()[0]
            report["candidate_skipped_events"]=db.execute(
                "SELECT count(*) FROM events WHERE kind='CANDIDATE_SKIPPED'"
            ).fetchone()[0]
        finally:
            db.execute("ROLLBACK")
    return rows,report


def paper_step(*, shadow_database, paper_database, now_ms,
               virtual_stake, max_parallel=1, proxy_max_lag_ms=5000):
    """Perform one bounded, idempotent reconciliation pass.

    Return warning codes only; never serialize raw prediction data.
    """
    if type(now_ms) is not int or now_ms<0 or type(proxy_max_lag_ms) is not int or proxy_max_lag_ms<0:
        raise PaperEvidenceError("invalid sidecar clock or proxy lag")
    source=Path(shadow_database).expanduser().resolve()
    target=Path(paper_database).expanduser()
    if target.is_symlink() or not target.parent.is_dir():
        raise PaperEvidenceError("paper target must be in an existing private directory")
    dest=target.resolve()
    info=dest.parent.stat()
    if info.st_uid!=os.getuid() or info.st_mode & 0o077:
        raise PaperEvidenceError("paper directory must be private (0700) and user-owned")
    if dest.parent==source.parent or (dest.exists() and source.samefile(dest)):
        raise PaperEvidenceError("paper database must not share the live SHADOW directory")

    rows,audit=_read_snapshot(source,now_ms,proxy_max_lag_ms)
    result={
        "status":"OK" if not audit["issue_count"] else "AUDIT_BLOCKED",
        "audit_issues":audit["issue_count"],
        "audit_codes":audit["issue_codes"],
        "review_count":audit["review_count"],
        "review_codes":audit["review_codes"],
        "formal_predictions":audit["formal_predictions"],
        "new_virtual_records":0,"proxy_reconciled":0,
        "past_predictions_skipped":0,"record_failures":0,
        "exchange_execution":"NO TRADE",
        "interpretation":"index price proxy / assumed 85% payout only",
    }
    if audit["issue_count"]:
        return result
    # A different first eligible raw tick may have arrived after resolution.
    # Without durable tick-ingestion chronology, do not score even proxy P&L
    # until an operator reviews this ambiguous evidence.
    if audit["review_count"]:
        result["status"]="REVIEW_REQUIRED"
        return result

    with PaperLedger(dest) as ledger:
        # Avoid O(N^2) rescans as a forward journal grows over days.
        existing={p["id"]:p for p in ledger.decisions()}
        for row in rows:
            if row.get("classification")!="shadow_v1":
                continue
            entry=row["entry_ms"]
            if row["recorded_ms"]<=now_ms<entry:
                try:
                    fresh=row["id"] not in existing
                    recorded=ledger.record(
                        row,now_ms=now_ms,virtual_stake=virtual_stake,
                        max_parallel=max_parallel,
                    )
                    existing[row["id"]]=recorded
                    if fresh:
                        result["new_virtual_records"]+=1
                except PaperEvidenceError:
                    result["record_failures"]+=1
            elif now_ms>=entry and row["id"] not in existing:
                # NO retroactive paper entries, even if a historical
                # shadow row looks correct in hindsight.
                result["past_predictions_skipped"]+=1

            if row.get("result_status") in ("VERIFIED","MISSING") and now_ms>=row["expiry_ms"]+proxy_max_lag_ms:
                prior=existing.get(row["id"])
                if prior and prior["status"]=="TRACKING":
                    try:
                        after=ledger.reconcile(
                            row,now_ms=now_ms,
                            proxy_max_lag_ms=proxy_max_lag_ms,
                        )
                        existing[row["id"]]=after
                        if after["status"]!="TRACKING":
                            result["proxy_reconciled"]+=1
                    except PaperEvidenceError:
                        result["record_failures"]+=1
        result["paper_decisions"]=len(existing)
    if result["record_failures"]:
        result["status"]="ERROR"
    return result


def main(argv=None):
    parser=argparse.ArgumentParser(
        description="Bounded SHADOW paper study. NO live order API."
    )
    parser.add_argument("--shadow-db",type=Path,required=True)
    parser.add_argument("--paper-db",type=Path,required=True)
    parser.add_argument("--virtual-stake",required=True)
    parser.add_argument("--max-parallel",type=int,default=1)
    parser.add_argument("--proxy-max-lag-ms",type=int,default=5000)
    parser.add_argument("--poll-seconds",type=float,default=15)
    parser.add_argument("--fast-poll-seconds",type=float,default=0.2)
    parser.add_argument("--pre-entry-window-seconds",type=float,default=3)
    parser.add_argument("--duration-seconds",type=int,default=0,
                        help="0 for one read-only pass; otherwise bounded polling")
    parser.add_argument("--continuous",action="store_true",
                        help="continue until interrupted; incompatible with a bounded duration")
    args=parser.parse_args(argv)
    try:
        next_poll_delay(0,args.poll_seconds,args.fast_poll_seconds,args.pre_entry_window_seconds)
    except ValueError:
        parser.error("invalid polling configuration")
    if args.duration_seconds<0 or (args.continuous and args.duration_seconds):
        parser.error("invalid bounded polling interval")
    start=time.monotonic()
    while True:
        result=paper_step(
            shadow_database=args.shadow_db,paper_database=args.paper_db,
            now_ms=time.time_ns()//1_000_000,
            virtual_stake=args.virtual_stake,max_parallel=args.max_parallel,
            proxy_max_lag_ms=args.proxy_max_lag_ms,
        )
        print(json.dumps(result,sort_keys=True,ensure_ascii=False),flush=True)
        if result["status"]!="OK":
            return 2
        if args.duration_seconds==0 and not args.continuous:
            return 0
        delay=next_poll_delay(time.time_ns()//1_000_000,args.poll_seconds,
                              args.fast_poll_seconds,args.pre_entry_window_seconds)
        if not args.continuous:
            remain=args.duration_seconds-(time.monotonic()-start)
            if remain<=0:
                return 0
            delay=min(delay,remain)
        time.sleep(delay)


if __name__=="__main__":
    raise SystemExit(main())
