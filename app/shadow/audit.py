"""Independent, read-only evidence audit for BTC shadow research.

No exchange requests, OAuth credentials, orders, model fitting, or DB mutations.
The report describes *index-price proxies*, never real event-contract settlement.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import sqlite3
import time
from collections import Counter
from decimal import Decimal, InvalidOperation
from pathlib import Path

APP_ID = 1397244983
DAY_MS = 86_400_000
HORIZONS = (30, 60)
MECHANISMS = ('FIXED', 'ROLLING')
MAX_ISSUES = 100
EXPECTED_PAYOUT = Decimal('0.85')


def _json(value):
    try:
        result = json.loads(value)
    except (TypeError, ValueError):
        return None
    return result if isinstance(result, dict) else None


def _finite_decimal(value):
    try:
        number = Decimal(str(value))
        return number if number.is_finite() else None
    except (InvalidOperation, TypeError, ValueError):
        return None


def _is_int(value):
    return type(value) is int


def _prediction_issues(row, max_lag_ms, tick_exists=None):
    """Return public issue *codes*, never raw feature values or credentials."""
    errors = []
    def error(code):
        if code not in errors:
            errors.append(code)

    if row.get('symbol') != 'BTCUSDT' or row.get('horizon_minutes') not in HORIZONS or row.get('mechanism') not in MECHANISMS:
        error('INVALID_PROFILE')
        return errors

    horizon = row['horizon_minutes']
    mechanism = row['mechanism']
    step = horizon * 60_000 if mechanism == 'FIXED' else 60_000
    times = ('decision_ms', 'recorded_ms', 'entry_ms', 'expiry_ms')
    if not all(_is_int(row.get(k)) for k in times):
        error('INVALID_TIMESTAMP')
        return errors
    if row['entry_ms'] % step or row['expiry_ms'] != row['entry_ms'] + horizon * 60_000:
        error('INVALID_ENTRY_EXPIRY')
    if row['decision_ms'] > row['recorded_ms'] or row['recorded_ms'] >= row['entry_ms']:
        error('LATE_OR_BACKDATED_PREDICTION')

    metadata = _json(row.get('metadata'))
    if metadata is None:
        error('INVALID_METADATA_JSON')
        return errors
    for k in ('symbol', 'horizon_minutes', 'mechanism', 'decision_ms', 'entry_ms', 'expiry_ms', 'candidate_direction', 'probability_up', 'model_version', 'screen_version', 'execution_decision'):
        if metadata.get(k) != row.get(k):
            error('METADATA_MISMATCH')
            break
    if metadata.get('classification') != 'shadow_v1' or row.get('classification') != 'shadow_v1':
        error('UNTRUSTED_CLASSIFICATION')
    screen_status = row.get('screen_passed')
    if screen_status not in (0, 1) or metadata.get('screen_passed') is not bool(screen_status):
        error('SCREEN_STATUS_MISMATCH')
    try:
        db_reasons = json.loads(row.get('no_trade_reasons'))
    except (ValueError, TypeError):
        db_reasons = None
    if db_reasons != metadata.get('no_trade_reasons'):
        error('NO_TRADE_REASONS_MISMATCH')
    if _json(row.get('features')) != metadata.get('features'):
        error('FEATURE_EVIDENCE_MISMATCH')
    if _finite_decimal(metadata.get('research_payout_ratio')) != EXPECTED_PAYOUT:
        error('WRONG_RESEARCH_PAYOUT')
    if metadata.get('live_quote_verified') is not False:
        error('UNVERIFIED_QUOTE_CLAIM')
    if row.get('execution_decision') != 'NO TRADE' or metadata.get('execution_decision') != 'NO TRADE':
        error('NON_SHADOW_EXECUTION')
    reasons = metadata.get('no_trade_reasons')
    if not isinstance(reasons, list) or 'verified_live_quote_unavailable' not in reasons:
        error('NO_TRADE_REASON_MISSING')
    if metadata.get('screen_passed') is False and (not isinstance(reasons, list) or 'historical_screen_failed' not in reasons):
        error('FAILED_SCREEN_REASON_MISSING')

    prob = _finite_decimal(row.get('probability_up'))
    if prob is None or not Decimal(0) <= prob <= Decimal(1):
        error('INVALID_PROBABILITY')
    elif row.get('candidate_direction') != ('HIGHER' if prob >= Decimal('0.5') else 'LOWER'):
        error('DIRECTION_PROBABILITY_MISMATCH')

    ident = hashlib.sha256(f"BTCUSDT:{mechanism}:{horizon}:{row['entry_ms']}".encode()).hexdigest()
    if row.get('id') != ident:
        error('INVALID_SLOT_IDENTIFIER')
    version = row.get('model_version')
    if not isinstance(version, str) or not version.startswith('sha256:') or len(version) != 71 or any(c not in '0123456789abcdef' for c in version[7:]):
        error('INVALID_MODEL_HASH')
    if not isinstance(row.get('screen_version'), str) or not row['screen_version']:
        error('INVALID_SCREEN_VERSION')

    feature_end = metadata.get('feature_end_ms')
    feature_available = metadata.get('feature_available_ms')
    bound = metadata.get('clock_uncertainty_ms')
    if not _is_int(feature_end) or feature_end != row['entry_ms'] - 60_001 or not _is_int(feature_available) or feature_available > row['decision_ms']:
        error('INVALID_FEATURE_BOUNDARY')
    if not isinstance(bound, (int, float)) or isinstance(bound, bool) or not math.isfinite(bound) or bound < 0 or row['recorded_ms'] + math.ceil(bound) >= row['entry_ms']:
        error('INVALID_CLOCK_DEADLINE')
    if metadata.get('training_spacing_minutes') != (horizon if mechanism == 'FIXED' else 1):
        error('INVALID_TRAINING_MECHANISM')

    windows = metadata.get('windows')
    if not isinstance(windows, dict):
        error('MISSING_WALK_WINDOWS')
    else:
        stages = ('train', 'calibration', 'screen')
        durations = (30, 4, 3)
        for key, days in zip(stages, durations):
            win = windows.get(key)
            if not isinstance(win, dict) or not all(_is_int(win.get(x)) for x in ('start_ms','end_ms','last_exit_ms','samples')) or win['end_ms'] - win['start_ms'] != days * DAY_MS or win['last_exit_ms'] >= win['end_ms'] or win['samples'] < 1:
                error('INVALID_WALK_WINDOW')
                break
        else:
            tr, ca, sc = [windows[x] for x in stages]
            if tr['end_ms'] != ca['start_ms'] or ca['end_ms'] != sc['start_ms'] or sc['end_ms'] > row['decision_ms'] or not sc['end_ms'] <= row['entry_ms'] < sc['end_ms'] + 7 * DAY_MS:
                error('INVALID_WALK_BOUNDARY')
            cutoff = windows.get('training_cutoff_ms')
            if not _is_int(cutoff) or not sc['end_ms'] <= cutoff <= row['decision_ms']:
                error('INVALID_TRAINING_CUTOFF')

    features = metadata.get('features')
    if not isinstance(features, dict) or not features:
        error('MISSING_FEATURE_EVIDENCE')
    else:
        for value in features.values():
            if not isinstance(value, dict) or not _is_int(value.get('event_ms')) or not _is_int(value.get('available_ms')) or not _is_int(feature_available) or not value['event_ms'] <= value['available_ms'] <= feature_available or not value.get('source') or _finite_decimal(value.get('value')) is None:
                error('INVALID_FEATURE_PROVENANCE')
                break
            if value.get('source') == 'OPENMARKET_BINANCE_FUTURES' and value.get('used_by_model') is True:
                error('UNVERIFIED_ENHANCED_MODEL')

    status = row.get('result_status')
    if status not in ('PENDING', 'MISSING', 'VERIFIED'):
        error('UNKNOWN_RESULT_STATUS')
    if status == 'VERIFIED':
        entry, expiry = _finite_decimal(row.get('entry_price')), _finite_decimal(row.get('exit_price'))
        if entry is None or expiry is None or entry <= 0 or expiry <= 0:
            error('MISSING_VERIFIED_PRICES')
        else:
            derived = 'HIGHER' if expiry > entry else 'LOWER' if expiry < entry else 'EQUAL'
            if row.get('direction_result') != derived:
                error('INCORRECT_PROXY_OUTCOME')
        for stem, target in (('entry', row['entry_ms']), ('exit', row['expiry_ms'])):
            event, received = row.get(stem + '_event_ms'), row.get(stem + '_received_ms')
            # Local receipt clocks may be slightly behind exchange event timestamps;
            # allow only the prediction's already-recorded uncertainty bound.
            clock_slack = math.ceil(bound) if isinstance(bound, (int, float)) and not isinstance(bound, bool) and math.isfinite(bound) and bound >= 0 else 0
            if not _is_int(event) or not _is_int(received) or not target <= event <= target + max_lag_ms or received + clock_slack < event or received > target + max_lag_ms:
                error('INVALID_PROXY_TIME_WINDOW')
            elif tick_exists is not None and not tick_exists(event, received, row.get(stem + '_price')):
                # A consistent-looking prediction row does not prove its index
                # snapshot was ever received. Cross-check the immutable tick journal.
                error('RAW_' + stem.upper() + '_TICK_NOT_FOUND')
    elif status in ('PENDING', 'MISSING') and row.get('direction_result') is not None:
        error('UNVERIFIED_HAS_DIRECTION_RESULT')
    return errors


def audit_rows(rows, now_ms, max_lag_ms=5000, *, tick_exists=None, tick_first=None):
    if not _is_int(now_ms) or now_ms < 0 or not _is_int(max_lag_ms) or max_lag_ms < 0:
        raise ValueError('invalid audit time or lag')
    profiles = {f'BTCUSDT:{h}:{m}': {'predictions':0,'pending':0,'verified':0,'missing':0,'scored':0,'correct':0,'distinct_entry_days':0,'nonoverlap_candidates':0,'nonoverlap_scored':0} for h in HORIZONS for m in MECHANISMS}
    groups = {key: [] for key in profiles}
    findings = []
    totals = Counter()
    reviews = []
    review_totals = Counter()
    slot_seen = set()

    def finding(code, row):
        totals[code] += 1
        if len(findings) < MAX_ISSUES:
            findings.append({'code': code, 'prediction_id': str(row.get('id', '<missing>'))[:16]})

    def review(code, row):
        review_totals[code] += 1
        if len(reviews) < MAX_ISSUES:
            reviews.append({'code': code, 'prediction_id': str(row.get('id', '<missing>'))[:16]})

    legacy = 0
    for row in rows:
        r = dict(row)
        if r.get('classification') != 'shadow_v1':
            legacy += 1
            if r.get('classification') not in ('legacy_development_test', 'legacy_forward_baseline'):
                finding('UNKNOWN_RECORD_CLASSIFICATION', r)
            continue
        key = f"{r.get('symbol')}:{r.get('horizon_minutes')}:{r.get('mechanism')}"
        if key not in profiles:
            finding('INVALID_PROFILE', r)
            continue
        errors = _prediction_issues(r, max_lag_ms, tick_exists=tick_exists)
        if _is_int(r.get('decision_ms')) and _is_int(r.get('recorded_ms')) and (r['decision_ms'] > now_ms or r['recorded_ms'] > now_ms):
            errors.append('PREDICTION_IN_FUTURE')
        if r.get('result_status') in ('VERIFIED', 'MISSING') and _is_int(r.get('expiry_ms')):
            bound = _json(r.get('metadata'))
            uncertainty = bound.get('clock_uncertainty_ms') if bound else None
            clock_extra = math.ceil(uncertainty) if isinstance(uncertainty, (int, float)) and not isinstance(uncertainty, bool) and math.isfinite(uncertainty) and uncertainty >= 0 else 0
            if now_ms < r['expiry_ms'] + max_lag_ms + clock_extra:
                errors.append('PREMATURE_RESOLVED')
        if r.get('result_status') == 'VERIFIED' and tick_first is not None:
            for stem, target_name in (('entry', 'entry_ms'), ('exit', 'expiry_ms')):
                event, received, target = r.get(stem+'_event_ms'), r.get(stem+'_received_ms'), r.get(target_name)
                if all(_is_int(x) for x in (event, received, target)):
                    first = tick_first(target, max_lag_ms)
                    # Current DB has no tick ingestion timestamp. An earlier tick
                    # might have arrived after resolve; this is a review, not proof
                    # of historical manipulation or an automatic invalidation.
                    if first is not None and (event, received) > first:
                        review('PROXY_'+stem.upper()+'_SELECTION_REVIEW', r)
        valid = not errors
        groups[key].append({
            'entry_ms': r.get('entry_ms'), 'expiry_ms': r.get('expiry_ms'),
            'result_status': r.get('result_status'),
            'direction_result': r.get('direction_result'),
            'candidate_direction': r.get('candidate_direction'),
            'valid': valid,
            'id': r.get('id'),
        })
        slot = (key, r.get('entry_ms'))
        if slot in slot_seen:
            finding('DUPLICATE_SLOT', r)
        slot_seen.add(slot)
        for code in errors:
            finding(code, r)
        status = r.get('result_status')
        if status == 'PENDING' and _is_int(r.get('expiry_ms')) and now_ms > r['expiry_ms'] + max_lag_ms + 1_000:
            finding('OVERDUE_PENDING', r)
        if status == 'MISSING' and _is_int(r.get('expiry_ms')) and now_ms < r['expiry_ms'] + max_lag_ms:
            finding('PREMATURE_MISSING', r)

    for key, group in groups.items():
        stat = profiles[key]
        group.sort(key=lambda r: (r.get('entry_ms') or 0, str(r.get('id'))))
        stat['predictions'] = len(group)
        last_expiry = None
        days = set()
        for r in group:
            status = r.get('result_status')
            if status == 'PENDING':stat['pending'] += 1
            elif status == 'VERIFIED':stat['verified'] += 1
            elif status == 'MISSING':stat['missing'] += 1
            if _is_int(r.get('entry_ms')):days.add(r['entry_ms']//DAY_MS)
            scored = r.get('valid') and status == 'VERIFIED' and r.get('direction_result') in ('HIGHER','LOWER')
            if scored:
                stat['scored'] += 1
                stat['correct'] += int(r.get('candidate_direction') == r['direction_result'])
            if _is_int(r.get('entry_ms')) and _is_int(r.get('expiry_ms')) and (last_expiry is None or r['entry_ms'] >= last_expiry):
                stat['nonoverlap_candidates'] += 1
                stat['nonoverlap_scored'] += int(scored)
                last_expiry = r['expiry_ms']
        stat['distinct_entry_days'] = len(days)
        stat['proxy_accuracy'] = stat['correct']/stat['scored'] if stat['scored'] else None
    return {'audit_version':'shadow_readonly_v2','generated_at_ms':now_ms,'formal_predictions':sum(len(v) for v in groups.values()),'excluded_legacy':legacy,'issue_count':sum(totals.values()),'issue_codes':dict(sorted(totals.items())),'issues':findings,'issues_truncated':sum(totals.values())>len(findings),'review_count':sum(review_totals.values()),'review_codes':dict(sorted(review_totals.items())),'reviews':reviews,'reviews_truncated':sum(review_totals.values())>len(reviews),'profiles':profiles,'scope':'Offline index-price proxy integrity only; not real event-contract settlement, executed trades, or profitability.'}


def audit_database(path, now_ms=None, max_lag_ms=5000):
    """Read SQLite with mode=ro: never create/migrate/change WAL or user data."""
    path = Path(path).expanduser().resolve()
    if not path.is_file():
        raise FileNotFoundError('shadow database does not exist')
    uri = path.as_uri() + '?mode=ro'
    with sqlite3.connect(uri, uri=True, timeout=5.0) as db:
        db.row_factory = sqlite3.Row
        db.execute('PRAGMA query_only=ON')
        if db.execute('PRAGMA application_id').fetchone()[0] != APP_ID or db.execute('PRAGMA user_version').fetchone()[0] != 2:
            raise ValueError('not an approved shadow v2 database')
        db.execute('BEGIN')
        try:
            rows = db.execute('SELECT * FROM predictions ORDER BY entry_ms,id')
            def recorded_tick(event_ms, received_ms, price):
                # Raw event and observed-receipt timestamps must match exactly.
                # Do not infer settlement prices from minute bars.
                return db.execute(
                    'SELECT 1 FROM ticks WHERE event_ms=? AND received_ms=? AND index_price=? LIMIT 1',
                    (event_ms, received_ms, str(price)),
                ).fetchone() is not None
            def first_tick_key(target_ms, lag_ms):
                row = db.execute(
                    'SELECT event_ms,received_ms FROM ticks '
                    'WHERE event_ms>=? AND event_ms<=? AND received_ms<=? '
                    'ORDER BY event_ms,received_ms LIMIT 1',
                    (target_ms, target_ms+lag_ms, target_ms+lag_ms),
                ).fetchone()
                return (row['event_ms'], row['received_ms']) if row else None
            result = audit_rows(rows, int(time.time()*1000) if now_ms is None else now_ms, max_lag_ms=max_lag_ms, tick_exists=recorded_tick, tick_first=first_tick_key)
            result['unresolved_second_gaps'] = db.execute("SELECT COUNT(*) FROM gaps WHERE status='OPEN'").fetchone()[0]
            result['candidate_skipped_events'] = db.execute("SELECT COUNT(*) FROM events WHERE kind='CANDIDATE_SKIPPED'").fetchone()[0]
        finally:
            db.execute('ROLLBACK')
    return result


def main(argv=None):
    parser = argparse.ArgumentParser(description='Offline read-only shadow evidence audit; no orders, no credentials')
    parser.add_argument('--database', required=True, type=Path)
    parser.add_argument('--output', type=Path, help='Optional JSON report path; do not use a path inside the live database directory')
    parser.add_argument('--now-ms', type=int)
    parser.add_argument('--proxy-max-lag-ms', type=int, default=5000)
    args = parser.parse_args(argv)
    report = audit_database(args.database, now_ms=args.now_ms, max_lag_ms=args.proxy_max_lag_ms)
    rendered = json.dumps(report, ensure_ascii=False, indent=2, allow_nan=False) + '\n'
    if args.output:
        # Do not resolve the *leaf* path: doing so follows a symlink and can
        # bypass a check intended to reject links. Parent path resolution still
        # protects the live database directory through parent symlinks.
        target = args.output.expanduser().absolute()
        live_dir = args.database.expanduser().resolve().parent
        if target.parent.resolve().is_relative_to(live_dir):
            parser.error('write report outside the live database directory')
        if target.is_symlink() or target.exists():
            parser.error('report output must be a new file, not an existing path or link')
        flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, 'O_NOFOLLOW', 0)
        try:
            fd = os.open(str(target), flags, 0o600)
        except OSError:
            parser.error('unable to exclusively create report output')
        try:
            with os.fdopen(fd, 'w', encoding='utf-8') as stream:
                stream.write(rendered)
                stream.flush()
                os.fsync(stream.fileno())
        except BaseException:
            # The fdopen context owns the descriptor. No other pre-existing file
            # can have been truncated because creation was exclusive.
            try:
                target.unlink()
            except OSError:
                pass
            raise
    else:
        print(rendered, end='')
    return 2 if report['issue_count'] else 0


if __name__ == '__main__':
    raise SystemExit(main())
