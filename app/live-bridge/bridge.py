"""Local, authenticated manual-order bridge. No scheduler and no automatic order retries.

The transport implements the existing BAPI contract. Configured-parameter mode
uses explicit per-period request values, without claiming a live quote. The
optional live-quote adapter fails closed unless its source is independently verified.
"""
import argparse
from contextlib import contextmanager
from decimal import Decimal, InvalidOperation
import hmac
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import re
import secrets
import sqlite3
import time
from urllib.error import HTTPError
from urllib.parse import urlsplit
from urllib.request import build_opener, HTTPRedirectHandler, Request

ORDER_URL = 'https://www.binance.com/bapi/futures/v2/private/future/event-contract/place-order'


class PanelError(Exception):
    """Only this fixed error code may cross the API/log boundary."""


def now_ms():
    return int(time.time() * 1000)


def exact_keys(value, keys):
    if not isinstance(value, dict) or set(value) != set(keys):
        raise PanelError('INVALID_INPUT')


def decimal_value(value):
    if not isinstance(value, str) or not re.fullmatch(r'\d{1,12}(?:\.\d{1,8})?', value):
        raise PanelError('INVALID_NUMBER')
    try:
        n = Decimal(value)
        if not n.is_finite() or n <= 0:
            raise PanelError('INVALID_NUMBER')
        return n
    except InvalidOperation:
        raise PanelError('INVALID_NUMBER') from None


def binance_url(url):
    if not isinstance(url, str):
        raise PanelError('SOURCE_INVALID')
    u = urlsplit(url)
    if u.scheme != 'https' or u.netloc != 'www.binance.com' or not u.path.startswith('/bapi/') or u.fragment:
        raise PanelError('SOURCE_INVALID')
    return u


class NoRedirects(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


class Transport:
    def __init__(self, credentials_path, timeout, read_retries):
        self.path = Path(credentials_path)
        self.timeout = timeout
        self.read_retries = read_retries
        self.opener = build_opener(NoRedirects())

    def request(self, url, payload=None):
        binance_url(url)
        try:
            auth = json.loads(self.path.read_text())
            # Never return these values or exception bodies to the browser/logs.
            if not all(isinstance(auth.get(k), str) and auth[k] and '\n' not in auth[k] and '\r' not in auth[k]
                       for k in ('p20t', 'csrftoken')):
                raise ValueError()
            headers = {'content-type':'application/json', 'clienttype':'web',
                       'csrftoken':auth['csrftoken'], 'cookie':'p20t=' + auth['p20t']}
        except (OSError, ValueError, TypeError):
            raise PanelError('LOGIN_REQUIRED') from None
        req = Request(url, headers=headers, data=None if payload is None else json.dumps(payload).encode(),
                      method='GET' if payload is None else 'POST')
        try:
            with self.opener.open(req, timeout=self.timeout) as response:
                raw = response.read(2_000_001)
                if len(raw) > 2_000_000:
                    raise PanelError('UPSTREAM_INVALID')
                return response.status, json.loads(raw)
        except HTTPError as error:
            if error.code in (401,403):
                raise PanelError('LOGIN_REQUIRED') from None
            raise PanelError('UPSTREAM_HTTP_ERROR') from None
        except (OSError, ValueError):
            raise PanelError('UPSTREAM_UNAVAILABLE') from None

    def get_json(self, url):
        # Read-only transport may retry; order POST never does.
        for attempt in range(self.read_retries + 1):
            try:
                return self.request(url)[1]
            except PanelError as error:
                if str(error) != 'UPSTREAM_UNAVAILABLE' or attempt == self.read_retries:
                    raise
                time.sleep(0.25)

    def post_order(self, payload):
        return self.request(ORDER_URL, payload)


class SchemaQuotes:
    """Explicit schema mapping only. Never recursively guess payout fields.

    Source must provide a millisecond server quote timestamp, a symbol, direction,
    period enum and ratio in each row. Providers with a different shape need a
    separately verified adapter. No such source is configured in the shipped files.
    """
    def __init__(self, config, enums, get_json, clock=now_ms):
        self.config, self.enums, self.get_json, self.clock = config, enums, get_json, clock

    def get(self, minutes, direction):
        c = self.config
        if not c or c.get('verified') is not True or not c.get('evidence'):
            raise PanelError('QUOTE_SOURCE_UNVERIFIED')
        u = binance_url(c.get('url'))
        if u.path == urlsplit(ORDER_URL).path:
            raise PanelError('SOURCE_INVALID')
        enum = self.enums.get(str(minutes))
        if not enum:
            raise PanelError('PERIOD_UNVERIFIED')
        raw = self.get_json(c['url'])
        received = self.clock()
        try:
            if raw.get('success') is not True or raw.get('code') != '000000':
                raise PanelError('QUOTE_UNAVAILABLE')
            rows = raw
            for key in c['rows_path']:
                rows = rows[key]
            if not isinstance(rows, list):
                raise PanelError('QUOTE_SCHEMA_INVALID')
            f = c['fields']
            matches = [r for r in rows if isinstance(r, dict) and r.get(f['symbol']) == 'BTCUSDT'
                       and r.get(f['enum']) == enum and r.get(f['direction']) == direction]
            if len(matches) != 1:
                raise PanelError('QUOTE_UNAVAILABLE')
            r = matches[0]
            return dict(symbol=r[f['symbol']], minutes=minutes, direction=r[f['direction']], enum=r[f['enum']],
                        ratio=r[f['ratio']], quoted_ms=r[f['quoted_ms']], received_ms=received, source=c['url'])
        except (KeyError, TypeError, AttributeError):
            raise PanelError('QUOTE_SCHEMA_INVALID') from None


class Engine:
    def __init__(self, db, quotes, transport, config, clock=now_ms):
        self.db, self.quotes, self.transport, self.config, self.clock = db, quotes, transport, config, clock
        with self.transaction() as c:
            c.execute('CREATE TABLE IF NOT EXISTS previews(id TEXT PRIMARY KEY, body TEXT NOT NULL, expires_ms INTEGER NOT NULL)')
            c.execute('CREATE TABLE IF NOT EXISTS attempts(id TEXT PRIMARY KEY, status TEXT NOT NULL, created_ms INTEGER NOT NULL, '
                      'updated_ms INTEGER NOT NULL, acknowledged_ms INTEGER, payload TEXT NOT NULL, auto_released_ms INTEGER)')
            if 'auto_released_ms' not in {row[1] for row in c.execute('PRAGMA table_info(attempts)')}:
                c.execute('ALTER TABLE attempts ADD COLUMN auto_released_ms INTEGER')

    @contextmanager
    def transaction(self):
        c = sqlite3.connect(self.db, timeout=5)
        c.row_factory = sqlite3.Row
        try:
            c.execute('BEGIN IMMEDIATE')
            yield c
            c.commit()
        except BaseException:
            c.rollback()
            raise
        finally:
            c.close()

    def parameters(self, minutes, direction):
        if type(minutes) is not int or minutes not in (10,30,60) or direction not in ('LONG','SHORT'):
            raise PanelError('INVALID_INPUT')
        if not self.config['verified_enums'].get(str(minutes)):
            raise PanelError('PERIOD_UNVERIFIED')

    def quote(self, minutes, direction):
        self.parameters(minutes, direction)
        if self.config.get('parameter_mode') == 'configured':
            ratio = self.config.get('configured_ratios', {}).get(str(minutes))
            if decimal_value(ratio) > 1:
                raise PanelError('QUOTE_INVALID')
            return dict(symbol='BTCUSDT', minutes=minutes, direction=direction,
                        enum=self.config['verified_enums'][str(minutes)], ratio=ratio,
                        source='local_period_parameters', kind='configured_parameter')
        q = self.quotes.get(minutes, direction)
        try:
            current = self.clock()
            if (q['symbol'] != 'BTCUSDT' or q['minutes'] != minutes or q['direction'] != direction
                    or q['enum'] != self.config['verified_enums'][str(minutes)]):
                raise PanelError('QUOTE_MISMATCH')
            if decimal_value(q['ratio']) > 1:
                raise PanelError('QUOTE_INVALID')
            binance_url(q['source'])
            if (type(q['quoted_ms']) is not int or type(q['received_ms']) is not int
                    or not 0 <= current - q['quoted_ms'] <= self.config['quote_max_age_ms']
                    or not q['quoted_ms'] <= q['received_ms'] <= current):
                raise PanelError('QUOTE_STALE')
            return q
        except (TypeError,KeyError):
            raise PanelError('QUOTE_INVALID') from None

    def preview(self, request):
        exact_keys(request, ['minutes','direction','amount'])
        if decimal_value(request['amount']) < Decimal(self.config['minimum_amount']):
            raise PanelError('AMOUNT_TOO_SMALL')
        q = self.quote(request['minutes'], request['direction'])
        p = dict(id=secrets.token_hex(16), quote=q, amount=request['amount'],
                 expires_ms=(self.clock()+self.config['preview_ttl_ms'] if q.get('kind') == 'configured_parameter' else min(q['quoted_ms'] + self.config['quote_max_age_ms'], self.clock()+self.config['preview_ttl_ms'])))
        with self.transaction() as c:
            if c.execute('SELECT 1 FROM attempts WHERE acknowledged_ms IS NULL AND auto_released_ms IS NULL').fetchone():
                raise PanelError('RECONCILE_REQUIRED')
            c.execute('INSERT INTO previews VALUES (?,?,?)', (p['id'],json.dumps(p),p['expires_ms']))
        return p

    def submit(self, request):
        exact_keys(request, ['preview_id'])
        pid = request['preview_id']
        if not isinstance(pid,str) or not re.fullmatch('[0-9a-f]{32}',pid):
            raise PanelError('INVALID_INPUT')
        with self.transaction() as c:
            prior = c.execute('SELECT * FROM attempts WHERE id=?',(pid,)).fetchone()
            if prior:
                return self.public_attempt(prior)
            row = c.execute('SELECT body FROM previews WHERE id=?',(pid,)).fetchone()
            if not row:
                raise PanelError('PREVIEW_NOT_FOUND')
            p = json.loads(row['body'])
        if self.clock() >= p['expires_ms']:
            raise PanelError('PREVIEW_EXPIRED')
        q = self.quote(p['quote']['minutes'], p['quote']['direction'])
        if q.get('kind') != p['quote'].get('kind') or any(q[k] != p['quote'][k] for k in ('symbol','minutes','direction','enum','ratio','source')):
            raise PanelError('QUOTE_CHANGED')
        payload = dict(orderAmount=p['amount'],timeIncrements=q['enum'],symbolName=q['symbol'],payoutRatio=q['ratio'],direction=q['direction'])
        with self.transaction() as c:
            prior = c.execute('SELECT * FROM attempts WHERE id=?',(pid,)).fetchone()
            if prior:
                return self.public_attempt(prior)
            if c.execute('SELECT 1 FROM attempts WHERE acknowledged_ms IS NULL AND auto_released_ms IS NULL').fetchone():
                raise PanelError('RECONCILE_REQUIRED')
            if self.clock() >= p['expires_ms'] or (q.get('kind') != 'configured_parameter' and self.clock()-q['quoted_ms'] > self.config['quote_max_age_ms']):
                raise PanelError('PREVIEW_EXPIRED')
            # Commit the attempt BEFORE the one and only external POST. A crash
            # anywhere after this point is unresolved, never automatically retried.
            c.execute('INSERT INTO attempts(id,status,created_ms,updated_ms,acknowledged_ms,payload,auto_released_ms) VALUES (?,?,?,?,NULL,?,NULL)',
                      (pid,'IN_FLIGHT',self.clock(),self.clock(),json.dumps(payload)))
        status = 'UNKNOWN'
        try:
            http, body = self.transport.post_order(payload)
            if isinstance(body,dict):
                if 200 <= http < 300 and body.get('success') is True and body.get('code') in (None,'000000'):
                    status = 'ACCEPTED_UNVERIFIED'
                elif body.get('success') is False:
                    status = 'REJECTED_UNVERIFIED'
        except Exception:
            # Intentional sanitization: exception text may contain account/session data.
            # Persist UNKNOWN; never retry or claim that no order exists.
            status = 'UNKNOWN'
        with self.transaction() as c:
            c.execute('UPDATE attempts SET status=?,updated_ms=? WHERE id=?',(status,self.clock(),pid))
            return self.public_attempt(c.execute('SELECT * FROM attempts WHERE id=?',(pid,)).fetchone())

    @staticmethod
    def public_attempt(row):
        return {k:row[k] for k in ('id','status','created_ms','updated_ms','acknowledged_ms','auto_released_ms')} | {'order':json.loads(row['payload'])}

    def history(self):
        with self.transaction() as c:
            return [self.public_attempt(r) for r in c.execute('SELECT * FROM attempts ORDER BY created_ms DESC LIMIT 30')]

    def acknowledge(self, request):
        exact_keys(request, ['id','checked_in_binance'])
        if request['checked_in_binance'] is not True or not isinstance(request['id'],str):
            raise PanelError('INVALID_INPUT')
        with self.transaction() as c:
            row = c.execute('SELECT * FROM attempts WHERE id=?',(request['id'],)).fetchone()
            if not row:
                raise PanelError('ATTEMPT_NOT_FOUND')
            # Never unlock a concurrent request. IN_FLIGHT after a crash needs a
            # separate manual incident review, not an automatic age-based reset.
            if row['status']=='IN_FLIGHT':
                raise PanelError('REQUEST_PENDING')
            c.execute('UPDATE attempts SET acknowledged_ms=? WHERE id=?',(self.clock(),request['id']))
        return {'acknowledged':True}

    def release_expired(self, request):
        """Release an accepted 30M request after expiry; never assert a fill.

        UNKNOWN and IN_FLIGHT still require an incident review. This is only a
        serialisation release, not an order-status or settlement result.
        """
        exact_keys(request, ['id'])
        ident = request['id']
        if not isinstance(ident, str) or not re.fullmatch('[0-9a-f]{32}', ident):
            raise PanelError('INVALID_INPUT')
        buffer_ms = self.config.get('auto_release_buffer_ms', 60000)
        if type(buffer_ms) is not int or not 60000 <= buffer_ms <= 600000:
            raise PanelError('AUTO_RELEASE_CONFIG_INVALID')
        with self.transaction() as c:
            row = c.execute('SELECT * FROM attempts WHERE id=?', (ident,)).fetchone()
            if not row:
                raise PanelError('ATTEMPT_NOT_FOUND')
            if row['status'] != 'ACCEPTED_UNVERIFIED':
                raise PanelError('RECONCILE_REQUIRED')
            try:
                payload = json.loads(row['payload'])
                period_map = {v: int(k) for k, v in self.config['verified_enums'].items() if k in ('10', '30', '60') and isinstance(v, str)}
                minutes = period_map.get(payload['timeIncrements'])
                if payload['symbolName'] != 'BTCUSDT' or minutes not in (10, 30, 60):
                    raise PanelError('RECONCILE_REQUIRED')
            except (ValueError, KeyError, TypeError):
                raise PanelError('RECONCILE_REQUIRED') from None
            if row['acknowledged_ms'] is not None or row['auto_released_ms'] is not None:
                return self.public_attempt(row)
            if self.clock() < row['created_ms'] + minutes * 60000 + buffer_ms:
                raise PanelError('NOT_EXPIRED')
            c.execute('UPDATE attempts SET auto_released_ms=? WHERE id=? AND auto_released_ms IS NULL',
                      (self.clock(), ident))
            return self.public_attempt(c.execute('SELECT * FROM attempts WHERE id=?', (ident,)).fetchone())


def authorize(headers, key, port):
    if headers.get('Host') != f'127.0.0.1:{port}':
        raise PanelError('HOST_DENIED')
    if headers.get('Origin') not in (None,'https://openmarket.xyz'):
        raise PanelError('ORIGIN_DENIED')
    if not hmac.compare_digest(headers.get('Authorization',''), 'Bearer '+key):
        raise PanelError('AUTH_REQUIRED')


def handler(engine, key, port):
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *_args):
            # No request headers, raw URLs, bodies or upstream responses in logs.
            return

        def respond(self, status, data):
            encoded = json.dumps(data).encode()
            self.send_response(status)
            self.send_header('Content-Type','application/json; charset=utf-8')
            self.send_header('Cache-Control','no-store')
            self.send_header('Content-Length',str(len(encoded)))
            self.end_headers()
            try:
                self.wfile.write(encoded)
            except (BrokenPipeError,ConnectionResetError):
                # Client disconnect never causes resubmission.
                return

        def process(self, post=False):
            try:
                self.connection.settimeout(5)
                authorize(self.headers,key,port)
                body = None
                if post:
                    if self.headers.get('Content-Type','').split(';')[0] != 'application/json':
                        raise PanelError('INVALID_CONTENT_TYPE')
                    size = int(self.headers.get('Content-Length','0'))
                    if not 0 < size <= 2048:
                        raise PanelError('INVALID_BODY')
                    body = json.loads(self.rfile.read(size))
                if not post and self.path=='/status':
                    mode = engine.config.get('parameter_mode', 'live_quote')
                    periods = {}
                    for m in ('10','30','60'):
                        available = bool(engine.config['verified_enums'].get(m))
                        if mode == 'configured':
                            ratio = engine.config.get('configured_ratios', {}).get(m)
                            try:
                                available = available and decimal_value(ratio) <= Decimal('1')
                            except PanelError:
                                available = False
                        else:
                            available = available and bool(engine.config.get('quote_source'))
                        periods[m] = available
                    result={'history':engine.history(), 'quote_source_configured':bool(engine.config.get('quote_source')), 'parameter_mode':mode,
                            'periods':periods}
                elif post and self.path=='/quote':
                    exact_keys(body,['minutes','direction'])
                    result=engine.quote(body['minutes'],body['direction'])
                elif post and self.path=='/preview': result=engine.preview(body)
                elif post and self.path=='/submit': result=engine.submit(body)
                elif post and self.path=='/acknowledge': result=engine.acknowledge(body)
                elif post and self.path=='/release-expired': result=engine.release_expired(body)
                else: raise PanelError('NOT_FOUND')
                self.respond(200,{'ok':True,'data':result})
            except PanelError as error:
                self.respond(400,{'ok':False,'error':{'code':str(error)}})
            except (ValueError,TypeError):
                self.respond(400,{'ok':False,'error':{'code':'INVALID_INPUT'}})
            except Exception:
                self.respond(500,{'ok':False,'error':{'code':'LOCAL_FAILURE'}})

        def do_GET(self): self.process()
        def do_POST(self): self.process(True)
    return Handler


def main():
    p=argparse.ArgumentParser()
    p.add_argument('--config',type=Path,default=Path(__file__).with_name('config.json'))
    p.add_argument('--serve',action='store_true')
    args=p.parse_args()
    config=json.loads(args.config.read_text())
    if not args.serve:
        print('未启动服务。当前实时报价来源：'+('已配置，需验证' if config['quote_source'] else '未接通'))
        return
    os.umask(0o077)
    state=Path(config['state_dir']).expanduser()
    state.mkdir(parents=True,exist_ok=True,mode=0o700)
    key_path=state/'bridge.key'
    try:
        with key_path.open('x') as f: f.write(secrets.token_urlsafe(32))
    except FileExistsError:
        if key_path.stat().st_mode & 0o077: raise SystemExit('bridge.key 权限必须为 600')
    key=key_path.read_text().strip()
    if len(key)<32: raise SystemExit('本机配对密钥无效')
    transport=Transport(Path(config['credentials_path']).expanduser(),config['request_timeout_seconds'],config['read_retries'])
    quotes=SchemaQuotes(config['quote_source'],config['verified_enums'],transport.get_json)
    engine=Engine(state/'ledger.sqlite',quotes,transport,config)
    port=config['port']
    server=ThreadingHTTPServer(('127.0.0.1',port),handler(engine,key,port))
    print(f'本机面板服务：127.0.0.1:{port}；配对密钥保存在 {key_path}（不要上传）',flush=True)
    if config.get('parameter_mode') == 'configured': print('使用按周期配置的请求参数；未连接实时报价。',flush=True)
    elif not config['quote_source']: print('报价未接通，订单请求将被阻止。',flush=True)
    try: server.serve_forever()
    except KeyboardInterrupt: print('已停止本机面板服务。')
    finally: server.server_close()


if __name__=='__main__': main()
