"""Evidence contracts for the reverse-engineered Binance Event Contract Web endpoint.

A redacted source snapshot is now stored under interface_evidence/. It is never
imported or executed here. Binance documents Event Contracts as app-only.
Nothing in this module authorizes live execution.
"""
from __future__ import annotations

import hashlib
import json
import re
from decimal import Decimal, InvalidOperation

SYMBOL = "BTCUSDT"
HORIZONS = frozenset((30, 60))
DIRECTIONS = frozenset(("HIGHER", "LOWER"))
EXPECTED_NET_PAYOUT = Decimal("0.85")
HEX_SHA256 = re.compile(r"^[0-9a-f]{64}$")


class EvidenceError(ValueError):
    pass


def _int(x, name):
    if type(x) is not int or x < 0:
        raise EvidenceError("invalid "+name)
    return x


def _text(x, name):
    if not isinstance(x, str) or not x.strip() or len(x)>512:
        raise EvidenceError("invalid "+name)
    return x


def _money(x, name, *, positive=False):
    try:
        n=Decimal(str(x))
    except (ValueError, TypeError, InvalidOperation):
        raise EvidenceError("invalid "+name) from None
    if not n.is_finite() or (positive and n<=0) or (not positive and n<0):
        raise EvidenceError("invalid "+name)
    return n


def _sha(x, name):
    if not isinstance(x,str) or not HEX_SHA256.fullmatch(x):
        raise EvidenceError("invalid "+name)
    return x


def summarize_quote(quote, *, now_ms):
    """Normalize read-only quote evidence, but NEVER promote it to 'verified'.

    The future local Codex adapter must attest provenance and prove the event
    contract's duration enum. A JSON field claiming verification is not proof.
    """
    if not isinstance(quote,dict):
        raise EvidenceError("quote must be an object")
    now_ms=_int(now_ms,"now_ms")
    if quote.get("symbol")!=SYMBOL or quote.get("horizon_minutes") not in HORIZONS or quote.get("direction") not in DIRECTIONS:
        raise EvidenceError("unsupported event-contract profile")
    _text(quote.get("api_duration_enum"),"api_duration_enum")
    quote_id=_text(quote.get("quote_id"),"quote_id")
    source=_text(quote.get("source_ref"),"source_ref")
    raw_hash=_sha(quote.get("raw_sha256"),"raw_sha256")
    as_of=_int(quote.get("as_of_ms"),"as_of_ms")
    received=_int(quote.get("received_ms"),"received_ms")
    expiry=_int(quote.get("expires_ms"),"expires_ms")
    if as_of>received or received>now_ms or expiry<=received or now_ms>=expiry:
        raise EvidenceError("stale or future quote evidence")
    payout=_money(quote.get("net_payout_ratio"),"net_payout_ratio",positive=True)
    if payout>Decimal("10"):
        raise EvidenceError("implausible payout evidence")
    return {
        "symbol":SYMBOL,
        "horizon_minutes":quote["horizon_minutes"],
        "direction":quote["direction"],
        "api_duration_enum_unverified":quote["api_duration_enum"],
        "quote_id":quote_id,
        "source_ref":source,
        "raw_sha256":raw_hash,
        "observed_as_of_ms":as_of,
        "received_ms":received,
        "expires_ms":expiry,
        "net_payout_ratio_unverified":str(payout),
        "research_payout_difference":str(payout-EXPECTED_NET_PAYOUT),
        "trusted_live_quote":False,
        "official_programmatic_api_available":False,
        "payout_ratio_dynamic_per_official_rules":True,
        "order_submission_allowed":False,
        "reason":"Binance documents Event Contracts as app-only; the private Web endpoint is unsupported and no authenticated live quote or 30M/1H enum is available.",
    }


def classify_order_response(*, network_outcome, http_status=None, payload=None):
    """Classify external attempt records; NEVER initiate or retry a request.

    A 200/success=true/data={} response is ONLY API_ACCEPTED_UNVERIFIED.
    No rejection code is currently certified side-effect-free.
    """
    if network_outcome not in ("RESPONSE", "TIMEOUT", "DISCONNECTED", "MALFORMED"):
        raise EvidenceError("unknown network outcome")
    if network_outcome!="RESPONSE":
        return {"state":"UNKNOWN","safe_to_retry":False,
                "reason":"transport result cannot establish whether Binance processed order"}
    if type(http_status) is not int or not (100<=http_status<=599):
        raise EvidenceError("invalid HTTP response status")
    if not isinstance(payload,dict):
        return {"state":"UNKNOWN","safe_to_retry":False,"reason":"malformed response"}
    if http_status==200 and payload.get("success") is True:
        claimed=payload.get("data")
        order_id=(claimed.get("orderId") if isinstance(claimed,dict) else None)
        return {"state":"API_ACCEPTED_UNVERIFIED","safe_to_retry":False,
                "claimed_order_id":str(order_id) if order_id else None,
                "reason":"HTTP acceptance is not exchange order confirmation"}
    return {"state":"UNKNOWN","safe_to_retry":False,
            "reason":"response alone cannot prove no-side-effect rejection"}


def inspect_settlement_proof(proof, *, expected_order_id):
    """Validate completeness without claiming source authenticity.

    An index-price proxy is specifically NOT accepted as actual settlement.
    The adapter still has to authenticate the exchange order-history origin.
    """
    if not isinstance(proof,dict):
        raise EvidenceError("settlement must be an object")
    order_id=_text(expected_order_id,"expected_order_id")
    if proof.get("order_id")!=order_id or proof.get("symbol")!=SYMBOL or proof.get("horizon_minutes") not in HORIZONS:
        raise EvidenceError("settlement identity mismatch")
    if proof.get("source_kind")!="EXCHANGE_ORDER_HISTORY_READ_ONLY":
        raise EvidenceError("source must be actual order history, not an index proxy")
    _text(proof.get("source_ref"),"source_ref")
    _sha(proof.get("raw_sha256"),"raw_sha256")
    entry_ms=_int(proof.get("entry_ms"),"entry_ms")
    expiry_ms=_int(proof.get("expiry_ms"),"expiry_ms")
    if expiry_ms-entry_ms != proof["horizon_minutes"]*60000:
        raise EvidenceError("settlement duration mismatch")
    stake=_money(proof.get("stake"),"stake",positive=True)
    gross=_money(proof.get("gross_return"),"gross_return")
    direction=proof.get("direction")
    if direction not in DIRECTIONS:
        raise EvidenceError("invalid settlement side")
    return {
        "order_id":order_id,
        "symbol":SYMBOL,
        "horizon_minutes":proof["horizon_minutes"],
        "direction":direction,
        "stake":str(stake),
        "gross_return":str(gross),
        "gross_minus_stake":str(gross-stake),
        "source_ref":proof["source_ref"],
        "source_hash":proof["raw_sha256"],
        "externally_authenticated":False,
        "realized_pnl_verified":False,
        "reason":"Schema consistency cannot independently authenticate the exchange history or fees.",
    }


def live_readiness(evidence):
    """Expose explicit LIVE blockers. Deliberately cannot turn on trading."""
    if not isinstance(evidence,dict):
        raise EvidenceError("readiness evidence must be an object")
    missing=[]
    for horizon in (30,60):
        proof=evidence.get(str(horizon),{})
        for field in ("verified_duration_enum","authenticated_readonly_quote",
                      "quote_payout_semantics","authenticated_order_status_query",
                      "authenticated_settlement_source","verified_tie_rule"):
            if not isinstance(proof,dict) or proof.get(field) is not True:
                missing.append(f"{horizon}M:{field}")
    if not evidence.get("real_order_client_source_reviewed"):
        missing.append("CODEX_LOCAL_ORDER_CLIENT_SOURCE_NOT_REVIEWED")
    if not evidence.get("independent_forward_advantage"):
        missing.append("INDEPENDENT_FORWARD_ADVANTAGE_UNPROVEN")
    if not evidence.get("live_risk_authorization"):
        missing.append("LIVE_RISK_AUTHORIZATION_NOT_GRANTED")
    # These are hard product/API boundaries, not user-fillable evidence flags.
    hard_blockers = [
        "BINANCE_OFFICIAL_EVENT_CONTRACT_ACCESS_IS_APP_ONLY",
        "PRIVATE_WEB_ORDER_ENDPOINT_IS_UNOFFICIAL_AND_UNSUPPORTED",
        "NO_OFFICIAL_READ_ONLY_QUOTE_ORDER_OR_SETTLEMENT_API",
        "PAYOUT_RATIO_IS_DYNAMIC_AND_NOT_READ_FROM_A_VERIFIED_QUOTE",
        "NO_LIVE_ORDER_TRANSPORT_IN_THIS_REPOSITORY",
    ]
    return {
        "live_submission_enabled":False,
        "status":"BLOCKED",
        "blockers":missing+hard_blockers,
        "message":"The reverse-engineered private Web endpoint is not an officially supported Event Contract API. Only a separate, reviewed and explicitly authorized integration could change this gate.",
    }
