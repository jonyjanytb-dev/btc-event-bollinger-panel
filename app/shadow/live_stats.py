"""Read-only proxy accounting for locally accepted event-contract requests.

The submitted payout ratio and index-price outcome are evidence for an estimate,
not proof of an exchange fill, settlement, fees, or realised account profit.
"""
from __future__ import annotations

import json
import sqlite3
from contextlib import closing
from decimal import Decimal, InvalidOperation
from pathlib import Path
from urllib.parse import quote

from .live_control import APP_ID, PERIOD_ENUMS

ELIGIBLE_STATES = frozenset((
    "API_ACCEPTED_UNVERIFIED", "TIME_EXPIRED_UNVERIFIED", "USER_REVIEWED_UNVERIFIED"
))


def _readonly(path: Path) -> sqlite3.Connection:
    path = Path(path)
    if path.is_symlink() or not path.is_file():
        raise ValueError("STATISTICS_SOURCE_UNAVAILABLE")
    uri = "file:" + quote(str(path.resolve(strict=True)), safe="/") + "?mode=ro"
    db = sqlite3.connect(uri, uri=True, timeout=2)
    db.row_factory = sqlite3.Row
    db.execute("PRAGMA query_only=ON")
    return db


def _config(path: Path) -> tuple[int, Path]:
    path = Path(path)
    if path.is_symlink() or not path.is_file():
        raise ValueError("STATISTICS_CONFIG_UNAVAILABLE")
    raw = json.loads(path.read_text(encoding="utf-8"))
    if (not isinstance(raw, dict) or set(raw) != {"start_ms", "bridge_ledger"}
            or type(raw["start_ms"]) is not int or raw["start_ms"] < 0
            or not isinstance(raw["bridge_ledger"], str)
            or not Path(raw["bridge_ledger"]).is_absolute()):
        raise ValueError("STATISTICS_CONFIG_INVALID")
    return raw["start_ms"], Path(raw["bridge_ledger"])


def _positive_decimal(value: object, *, max_value: Decimal | None = None) -> Decimal | None:
    if not isinstance(value, str):
        return None
    try:
        number = Decimal(value)
    except InvalidOperation:
        return None
    if not number.is_finite() or number <= 0 or (max_value is not None and number > max_value):
        return None
    return number


def _matched_payload(bridge: sqlite3.Connection, attempt: sqlite3.Row) -> tuple[Decimal, Decimal, int] | None:
    receipt = bridge.execute("SELECT status,payload FROM attempts WHERE id=?",
                             (attempt["bridge_attempt_id"],)).fetchone()
    if not receipt or receipt["status"] != "ACCEPTED_UNVERIFIED":
        return None
    try:
        payload = json.loads(receipt["payload"])
    except (ValueError, TypeError):
        return None
    if not isinstance(payload, dict):
        return None
    amount = _positive_decimal(attempt["amount"])
    submitted_amount = _positive_decimal(payload.get("orderAmount"))
    ratio = _positive_decimal(payload.get("payoutRatio"), max_value=Decimal("1"))
    if (amount is None or submitted_amount != amount or ratio is None
            or payload.get("symbolName") != "BTCUSDT"
            or payload.get("direction") != attempt["direction"]
            or attempt["expiry_ms"] is None):
        return None
    duration = attempt["expiry_ms"] - attempt["created_ms"]
    periods = [minutes for minutes, enum in PERIOD_ENUMS.items()
               if payload.get("timeIncrements") == enum
               and abs(duration - minutes * 60_000) <= 5000]
    return (amount, ratio, periods[0]) if len(periods) == 1 else None


def read_live_statistics(live_db: Path, statistics_config: Path) -> dict:
    """Build an auditable cumulative estimate from each accepted request.

    No database is altered. A missing or mismatched bridge record is excluded
    from win rate and PnL rather than receiving a guessed ratio or stake.
    """
    start_ms, bridge_path = _config(statistics_config)
    with closing(_readonly(live_db)) as live, closing(_readonly(bridge_path)) as bridge:
        if live.execute("PRAGMA application_id").fetchone()[0] != APP_ID:
            raise ValueError("STATISTICS_LEDGER_INVALID")
        attempts = live.execute(
            "SELECT a.id,a.created_ms,a.expiry_ms,a.direction,a.amount,a.state,a.bridge_attempt_id,"
            "p.status AS proxy_status,p.outcome AS proxy_outcome "
            "FROM attempts a LEFT JOIN live_proxy_results p ON p.attempt_id=a.id "
            "WHERE a.created_ms>=? AND a.bridge_attempt_id IS NOT NULL "
            "ORDER BY a.expiry_ms,a.created_ms,a.id", (start_ms,)).fetchall()
        rows: list[dict] = []
        scored: list[dict] = []
        counts = {"requests": 0, "scored": 0, "wins": 0, "losses": 0,
                  "pending": 0, "missing": 0, "equal": 0, "unverifiable": 0}
        for attempt in attempts:
            if attempt["state"] not in ELIGIBLE_STATES:
                continue
            counts["requests"] += 1
            row = {"created_ms": attempt["created_ms"], "expiry_ms": attempt["expiry_ms"],
                   "direction": attempt["direction"], "amount": attempt["amount"],
                   "proxy_status": attempt["proxy_status"] or "PENDING",
                   "proxy_outcome": attempt["proxy_outcome"], "submitted_ratio": None,
                   "estimated_pnl": None, "period_minutes": None}
            matched = _matched_payload(bridge, attempt)
            if matched is not None:
                amount, ratio, minutes = matched
                row.update(submitted_ratio=str(ratio), period_minutes=minutes)
            status = row["proxy_status"]
            if matched is None:
                counts["unverifiable"] += 1
                row["proxy_status"] = "UNVERIFIABLE"
            elif status == "PENDING":
                counts["pending"] += 1
            elif status == "MISSING":
                counts["missing"] += 1
            elif status == "EQUAL":
                counts["equal"] += 1
            elif status == "VERIFIED" and row["proxy_outcome"] in ("WIN", "LOSS"):
                pnl = amount * ratio if row["proxy_outcome"] == "WIN" else -amount
                counts["scored"] += 1
                counts["wins" if row["proxy_outcome"] == "WIN" else "losses"] += 1
                row["estimated_pnl"] = str(pnl)
                scored.append(row)
            else:
                counts["unverifiable"] += 1
                row["proxy_status"] = "UNVERIFIABLE"
            rows.append(row)
    cumulative = Decimal("0")
    curve = []
    for row in scored:
        cumulative += Decimal(row["estimated_pnl"])
        row["cumulative_pnl"] = str(cumulative)
        curve.append({"expiry_ms": row["expiry_ms"], "amount": row["amount"],
                      "submitted_ratio": row["submitted_ratio"],
                      "outcome": row["proxy_outcome"],
                      "estimated_pnl": row["estimated_pnl"],
                      "cumulative_pnl": str(cumulative)})
    return {"start_ms": start_ms, **counts,
            "win_rate": str(Decimal(counts["wins"]) / counts["scored"]) if counts["scored"] else None,
            "estimated_pnl": str(cumulative), "curve": curve,
            "rows": list(reversed(rows[-200:])),
            "basis": "ACCEPTED_REQUEST_INDEX_PROXY_SUBMITTED_RATIO"}


def read_live_statistics_for_site(live_config: Path) -> dict:
    from .live_api import _settings
    settings = _settings(live_config)
    return read_live_statistics(
        Path(settings["control_db"]), Path(live_config).with_name("live-statistics.json"))
