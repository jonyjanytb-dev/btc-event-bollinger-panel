"""Private cloud controls for a single user-armed, one-attempt live signal."""
from __future__ import annotations

import hmac
import json
import os
import sqlite3
from pathlib import Path
from urllib.parse import quote, urlsplit

from .live_bridge import BridgeClient, BridgeRejected, BridgeUnavailable
from .live_control import LiveControl, LiveControlError, STRATEGY, MULTI_STRATEGY, SUPPORTED_STRATEGIES, PERIOD_ENUMS, DIRECTION_MODES
from .live_worker import latest_observation_id
from .signal_periods import DEFAULT_PERIODS, periods_path, read_periods, save_periods


class LiveAPIError(RuntimeError):
    """A safe, fixed error code that contains no credentials."""


def _private_file(path: Path) -> Path:
    path = Path(path)
    if (path.is_symlink() or not path.is_file() or
            path.stat().st_uid != os.getuid() or path.stat().st_mode & 0o077):
        raise LiveAPIError("PRIVATE_FILE_UNAVAILABLE")
    return path


def _settings(config_path: Path) -> dict:
    try:
        raw = json.loads(_private_file(config_path).read_text())
        if set(raw) != {"control_db", "chart_db", "bridge_key_file", "max_amount", "public_origin"}:
            raise ValueError
        for name in ("control_db", "chart_db", "bridge_key_file"):
            if not isinstance(raw[name], str) or not Path(raw[name]).is_absolute():
                raise ValueError
        if not isinstance(raw["max_amount"], str):
            raise ValueError
        origin = urlsplit(raw["public_origin"])
        if (origin.scheme != "https" or not origin.hostname or origin.username
                or origin.password or origin.path or origin.query or origin.fragment):
            raise ValueError
        return raw
    except (OSError, ValueError, TypeError) as error:
        raise LiveAPIError("LIVE_CONFIG_INVALID") from None


def _safe_state(config: dict, attempt_limit: int = 20) -> dict:
    path = Path(config["control_db"])
    if path.is_symlink():
        raise LiveAPIError("LIVE_LEDGER_INVALID")
    if not path.is_file():
        return {"enabled": False, "strategy": STRATEGY, "horizon": 30, "reverse": False, "direction_mode": "SIGNAL", "cooldown_five_minutes": False,
                "amount": None, "attempts": [], "accepted_unverified_count": 0,
                "max_amount": config["max_amount"],
                "ladder": {"enabled": False, "stakes": ["5", "7", "15"], "step": 0, "next_amount": "5", "waiting_for_result": False}}
    try:
        uri = "file:" + quote(str(path.resolve(strict=True)), safe="/") + "?mode=ro"
        with sqlite3.connect(uri, uri=True, timeout=2) as db:
            db.row_factory = sqlite3.Row
            db.execute("PRAGMA query_only=ON")
            if db.execute("PRAGMA application_id").fetchone()[0] != 1162894410:
                raise LiveAPIError("LIVE_LEDGER_INVALID")
            columns = {r[1] for r in db.execute("PRAGMA table_info(config)")}
            fields = "enabled,strategy,horizon,amount,cursor,updated_ms"
            fields += ",reverse" if "reverse" in columns else ""
            fields += ",cooldown_five_minutes" if "cooldown_five_minutes" in columns else ""
            fields += ",direction_mode" if "direction_mode" in columns else ""
            row = db.execute("SELECT " + fields + " FROM config WHERE id=1").fetchone()
            has_proxy = db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='live_proxy_results'").fetchone() is not None
            has_buffer = "release_buffer_ms" in {r[1] for r in db.execute("PRAGMA table_info(attempts)")}
            buffer_field = ",a.release_buffer_ms" if has_buffer else ""
            proxy_fields = ",p.status AS proxy_status,p.entry_price AS proxy_entry_price,p.exit_price AS proxy_exit_price,p.outcome AS proxy_outcome" if has_proxy else ""
            proxy_join = " LEFT JOIN live_proxy_results p ON p.attempt_id=a.id" if has_proxy else ""
            attempts = [dict(r) for r in db.execute(
                "SELECT a.id,a.observation_id,a.direction,a.amount,a.state,a.created_ms,a.updated_ms,a.bridge_attempt_id,a.expiry_ms "
                + buffer_field + proxy_fields + " FROM attempts a" + proxy_join + " ORDER BY a.created_ms DESC LIMIT ?",
                (max(1, min(attempt_limit, 200)),))]
            accepted_count = db.execute(
                "SELECT COUNT(*) FROM attempts WHERE state IN ('API_ACCEPTED_UNVERIFIED','TIME_EXPIRED_UNVERIFIED')").fetchone()[0]
            has_ladder = db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='live_ladder'").fetchone() is not None
            ladder_row = db.execute("SELECT * FROM live_ladder WHERE id=1").fetchone() if has_ladder else None
            stakes = json.loads(ladder_row["stakes"]) if ladder_row else ["5", "7", "15"]
            ladder = {"enabled": bool(ladder_row["enabled"]) if ladder_row else False,
                      "stakes": stakes, "step": ladder_row["step"] if ladder_row else 0,
                      "next_amount": stakes[ladder_row["step"]] if ladder_row else stakes[0],
                      "waiting_for_result": bool(ladder_row and ladder_row["enabled"]
                                                 and ladder_row["last_attempt_id"]
                                                 and ladder_row["last_attempt_id"] != ladder_row["applied_attempt_id"])}
        return {"enabled": bool(row["enabled"]) if row else False,
                "strategy": row["strategy"] if row else STRATEGY,
                "horizon": row["horizon"] if row else 30,
                "reverse": bool(row["reverse"]) if row and "reverse" in columns else False,
                "direction_mode": row["direction_mode"] if row and "direction_mode" in columns else ("REVERSE" if row and "reverse" in columns and row["reverse"] else "SIGNAL"),
                "cooldown_five_minutes": bool(row["cooldown_five_minutes"]) if row and "cooldown_five_minutes" in columns else False,
                "amount": row["amount"] if row else None,
                "attempts": attempts, "accepted_unverified_count": accepted_count,
                "max_amount": config["max_amount"], "ladder": ladder}
    except sqlite3.Error:
        raise LiveAPIError("LIVE_LEDGER_UNAVAILABLE") from None


def read_live_state(config_path: Path, attempt_limit: int = 20) -> dict:
    config = _settings(config_path)
    state = _safe_state(config, attempt_limit)
    state["signal_periods"] = list(read_periods(periods_path(config["chart_db"])))
    return state


def change_signal_periods_from_site(config_path: Path, periods) -> dict:
    config = _settings(config_path)
    with LiveControl(Path(config["control_db"])) as control:
        if control.state() and control.state()["enabled"]:
            raise LiveAPIError("DISABLE_BEFORE_CHANGING_PERIODS")
        try:
            selected = save_periods(periods_path(config["chart_db"]), periods)
        except (OSError, ValueError):
            raise LiveAPIError("INVALID_SIGNAL_PERIODS") from None
    return {"signal_periods": list(selected), "strategy": STRATEGY if selected == DEFAULT_PERIODS else MULTI_STRATEGY}


def change_live_state_from_site(config_path: Path, body: dict, *, now_ms: int,
                                bridge_factory=BridgeClient) -> dict:
    """Use the private bridge key after the gateway and same-origin HTTP checks.

    The browser never receives or submits the key. Reject a browser-supplied
    authorization field so the trusted boundary stays explicit.
    """
    if not isinstance(body, dict) or "authorization" in body:
        raise LiveAPIError("INVALID_LIVE_REQUEST")
    if body.get("action") in ("enable", "review"):
        config = _settings(config_path)
        key = _private_file(Path(config["bridge_key_file"])).read_text().strip()
        body = body | {"authorization": key}
    return change_live_state(config_path, body, now_ms=now_ms,
                             bridge_factory=bridge_factory)


def live_public_origin(config_path: Path) -> str:
    return _settings(config_path)["public_origin"]


def change_live_state(config_path: Path, body: dict, *, now_ms: int,
                      bridge_factory=BridgeClient) -> dict:
    config = _settings(config_path)
    if not isinstance(body, dict) or type(now_ms) is not int:
        raise LiveAPIError("INVALID_LIVE_REQUEST")
    action = body.get("action")
    if action == "disable" and set(body) == {"action"}:
        with LiveControl(Path(config["control_db"])) as control:
            control.disable(now_ms)
        return _safe_state(config)
    if action == "review":
        if (set(body) != {"action", "attempt_id", "checked_in_binance", "authorization"}
                or body["checked_in_binance"] is not True
                or not isinstance(body["attempt_id"], str)):
            raise LiveAPIError("INVALID_LIVE_REVIEW")
        supplied = body["authorization"]
        if not isinstance(supplied, str) or len(supplied) > 256:
            raise LiveAPIError("LIVE_AUTH_REQUIRED")
        key = _private_file(Path(config["bridge_key_file"])).read_text().strip()
        if not hmac.compare_digest(supplied, key):
            raise LiveAPIError("LIVE_AUTH_REQUIRED")
        try:
            with LiveControl(Path(config["control_db"])) as control:
                local = control.attempt(body["attempt_id"])
                if not local or not local["bridge_attempt_id"]:
                    raise LiveAPIError("REVIEW_MATCH_UNAVAILABLE")
                bridge = bridge_factory("http://127.0.0.1:8876", key)
                history = bridge.status().get("history", [])
                match = next((item for item in history if item.get("id") == local["bridge_attempt_id"]), None)
                if not match or match.get("status") == "IN_FLIGHT":
                    raise LiveAPIError("REVIEW_MATCH_UNAVAILABLE")
                if match.get("acknowledged_ms") is None:
                    if bridge.acknowledge(local["bridge_attempt_id"]).get("acknowledged") is not True:
                        raise LiveAPIError("BRIDGE_RECONCILE_REQUIRED")
                control.review(local["id"], local["bridge_attempt_id"], now_ms)
        except LiveControlError as error:
            raise LiveAPIError(str(error)) from None
        except (BridgeUnavailable, BridgeRejected, OSError, sqlite3.Error):
            raise LiveAPIError("BRIDGE_UNAVAILABLE") from None
        return _safe_state(config)
    required = {"action", "strategy", "horizon", "amount", "authorization"}
    if action != "enable" or not required <= set(body) <= required | {"reverse", "cooldown_five_minutes", "direction_mode", "signal_periods", "ladder_stakes"}:
        raise LiveAPIError("INVALID_LIVE_REQUEST")
    if type(body.get("reverse", False)) is not bool or type(body.get("cooldown_five_minutes", False)) is not bool:
        raise LiveAPIError("INVALID_LIVE_CONFIGURATION")
    mode = body.get("direction_mode")
    if (mode is not None and (type(mode) is not str or mode not in DIRECTION_MODES
                             or (body.get("reverse", False) and mode != "REVERSE"))):
        raise LiveAPIError("INVALID_LIVE_CONFIGURATION")
    supplied = body["authorization"]
    if not isinstance(supplied, str) or len(supplied) > 256:
        raise LiveAPIError("LIVE_AUTH_REQUIRED")
    key = _private_file(Path(config["bridge_key_file"])).read_text().strip()
    if not hmac.compare_digest(supplied, key):
        raise LiveAPIError("LIVE_AUTH_REQUIRED")
    selected = read_periods(periods_path(config["chart_db"]))
    if (body["strategy"] == MULTI_STRATEGY and body.get("signal_periods") != list(selected)):
        raise LiveAPIError("SIGNAL_PERIODS_CHANGED")
    if ("signal_periods" in body and body["signal_periods"] != list(selected)):
        raise LiveAPIError("SIGNAL_PERIODS_CHANGED")
    if (body["strategy"] not in SUPPORTED_STRATEGIES
            or (selected != DEFAULT_PERIODS and body["strategy"] != MULTI_STRATEGY)
            or (selected == DEFAULT_PERIODS and body["strategy"] == MULTI_STRATEGY)
            or type(body["horizon"]) is not int or body["horizon"] not in PERIOD_ENUMS):
        raise LiveAPIError("STRATEGY_UNAVAILABLE")
    if not isinstance(body["amount"], str):
        raise LiveAPIError("INVALID_AMOUNT")
    try:
        current = _safe_state(config)
        if current["enabled"]:
            raise LiveAPIError("ALREADY_ENABLED")
        bridge = bridge_factory("http://127.0.0.1:8876", key)
        status = bridge.status()
        unresolved = [x for x in status.get("history", []) if x.get("acknowledged_ms") is None and x.get("auto_released_ms") is None] if isinstance(status.get("history"), list) else []
        local_accepted = {x["bridge_attempt_id"] for x in current["attempts"] if x["state"] == "API_ACCEPTED_UNVERIFIED"}
        if (status.get("parameter_mode") != "configured"
                or status.get("periods", {}).get(str(body["horizon"])) is not True
                or not isinstance(status.get("history"), list)
                or len(unresolved) > 1
                or any(x.get("status") != "ACCEPTED_UNVERIFIED" or x.get("id") not in local_accepted
                       for x in unresolved)):
            raise LiveAPIError("BRIDGE_RECONCILE_REQUIRED")
        cursor = latest_observation_id(Path(config["chart_db"]))
        with LiveControl(Path(config["control_db"])) as control:
            control.enable(strategy=body["strategy"], horizon=body["horizon"],
                           amount=body["amount"], max_amount=config["max_amount"],
                           current_observation_id=cursor, now_ms=now_ms,
                           reverse=body.get("reverse", False),
                           cooldown_five_minutes=body.get("cooldown_five_minutes", False),
                           direction_mode=mode, ladder_stakes=body.get("ladder_stakes"))
    except LiveControlError as error:
        raise LiveAPIError(str(error)) from None
    except (BridgeUnavailable, BridgeRejected):
        raise LiveAPIError("BRIDGE_UNAVAILABLE") from None
    except (OSError, sqlite3.Error, ValueError):
        raise LiveAPIError("LIVE_SOURCE_UNAVAILABLE") from None
    return _safe_state(config)
