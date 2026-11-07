"""Configured read-only collectors, with no order endpoints."""
import json,time
from pathlib import Path
from .binance import BinanceHistory,collect_stream
from .openmarket import normalise_points,normalise_metrics,normalise_books
from .transport import ReadOnlyMCP,binance_get

def now_ms(): return time.time_ns()//1_000_000

def load_token(path):
 store=json.loads(Path(path).expanduser().read_text())
 entries=[x for x in store.values() if x.get('server_name')=='openmarket' and x.get('server_url')=='https://mcp.openmarket.xyz/']
 if len(entries)!=1:raise ValueError('OpenMarket OAuth not uniquely configured')
 e=entries[0];expires=e.get('expires_at')
 if expires is not None and expires<=now_ms():raise ValueError('OAuth expired: use Codex login to renew')
 return e['access_token']

def collect_index_history(store,config,interval,start,end):
 fetch=lambda params:binance_get(config['binance_rest'],'/fapi/v1/indexPriceKlines',params,config['timeout_seconds'],config['read_retries'])
 return BinanceHistory(store,fetch,config['page_limit']).run(interval,start,end,now_ms())

def collect_rest_snapshot(store,config):
 response=binance_get(config['binance_rest'],'/fapi/v1/premiumIndex',{'symbol':'BTCUSDT'},config['timeout_seconds'],config['read_retries'])
 received=now_ms()
 tick={'symbol':response['symbol'],'event_ms':int(response['time']),'received_ms':received,'index_price':str(response['indexPrice']),'mark_price':str(response['markPrice'])}
 store.record_tick(tick,{'source':'rest_snapshot','response':response})
 return tick

def collect_openmarket(store,config,token):
 client=ReadOnlyMCP(config['openmarket_mcp'],token,timeout=config['timeout_seconds'],retries=config['read_retries']);client.initialise()
 base={'rawSymbol':'BTCUSDT','interval':config['openmarket_interval'],'lookback':config['openmarket_lookback']}
 queries=[]
 for venue in ('BINANCE','BINANCE_FUTURES'):
  queries.append(('points',dict(base,type='TRADE_SIDE_AGNOSTIC_AGG',exchanges=[venue])))
 for typ in ('OPEN_INTEREST_AGG','FUNDING_RATE_AGG','LIQUIDATION_AGG'):
  queries.append(('points',dict(base,type=typ,exchanges=['BINANCE_FUTURES'])))
 for side in ('BUY','SELL'):
  queries.append(('points',dict(base,type='TRADE_AGG',side=side,exchanges=['BINANCE_FUTURES'])))
 queries.append(('points',dict(base,type='BLOCK_BOOK_SNAPSHOT_AGG',blockSize=config['openmarket_book_block_size'],exchanges=['BINANCE_FUTURES'])))
 queries.append(('metric_get',{'selector':{'symbol':'BTCUSDT','exchange':'BINANCE_FUTURES','interval':config['openmarket_interval']},'queries':[{'metric':'cvd'},{'metric':'book_imbalance'}]}))
 count=0
 for tool,args in queries:
  try:
   data=client.tool(tool,args);received=now_ms()
   expected={'exchange':args['exchanges'][0],'rawSymbol':args['rawSymbol'],'interval':args['interval'],'type':args['type']} if tool=='points' else args['selector']
   if args.get('side'):expected['side']=args['side']
   is_book=args.get('type')=='BLOCK_BOOK_SNAPSHOT_AGG'
   points=normalise_points(data,received,expected) if tool=='points' and not is_book else []
   metrics=normalise_metrics(data,received,expected) if tool=='metric_get' else []
   books=normalise_books(data,received,'BTCUSDT','BINANCE_FUTURES',expected) if is_book else []
   store.write_openmarket({'tool':tool,'arguments':args,'response':data,'transport_body':client.last_response_body},points,metrics,received,books,args.get('blockSize'))
   if not (points or metrics or books):
    store.event('EMPTY_RESULT','openmarket',{'tool':tool,'type':args.get('type')},received);continue
   if tool=='metric_get':
    missing=[x['metric'] for x in args['queries'] if x['metric'] not in {m['kind'] for m in metrics if m['event_ms'] is not None}]
    if missing:
     store.event('PARTIAL_RESULT','openmarket',{'tool':tool,'missing':missing},received);continue
   count+=1
  except (ValueError,KeyError,TypeError,IndexError,TimeoutError,OSError) as e:
   store.event('INVALID_SOURCE_DATA' if isinstance(e,(ValueError,KeyError,TypeError,IndexError)) else 'COLLECTION_ERROR','openmarket',{'tool':tool,'type':args.get('type'),'error':type(e).__name__},now_ms())
 return count,len(queries)

def backfill_openmarket(store,client,market,interval,start_ms,end_ms,window_ms,now=now_ms,kind='TRADE_SIDE_AGNOSTIC_AGG',side=None):
 steps={'MINUTE':60000,'FIVE_MINUTES':300000,'FIFTEEN_MINUTES':900000,'THIRTY_MINUTES':1800000,'HOUR':3600000,'FOUR_HOURS':14400000}
 if market not in ('BINANCE','BINANCE_FUTURES') or interval not in steps:raise ValueError('Unsupported market or interval')
 if kind not in ('TRADE_SIDE_AGNOSTIC_AGG','TRADE_AGG','OPEN_INTEREST_AGG','FUNDING_RATE_AGG','LIQUIDATION_AGG'):raise ValueError('Unsupported historical type')
 if (kind=='TRADE_AGG') != (side in ('BUY','SELL')):raise ValueError('BUY or SELL side required only for TRADE_AGG')
 step=steps[interval]
 if start_ms%step or end_ms%step or window_ms<step or window_ms%step:raise ValueError('Window must align to interval')
 key=f'openmarket:backfill:{market}:BTCUSDT:{interval}:{kind}:{side or "all"}:{start_ms}:{end_ms}'
 saved=store.checkpoint(key);cursor=max(start_ms,saved if saved is not None else start_ms)
 while cursor<end_ms:
  span=min(window_ms,end_ms-cursor);args={'rawSymbol':'BTCUSDT','exchanges':[market],'type':kind,'interval':interval,'from':cursor,'lookback':str(span//60000)+'m'}
  if side:args['side']=side
  data=client.tool('points',args);received=now()
  expected={'exchange':market,'rawSymbol':'BTCUSDT','interval':interval,'type':kind}
  if side:expected['side']=side
  try:rows=normalise_points(data,received,expected)
  except (ValueError,KeyError,TypeError,IndexError):
   store.event('INVALID_SOURCE_DATA',key,{'reason':'invalid_response'},received);raise
  rows=[x for x in rows if cursor<=x['bucket_ms']<end_ms and x['bucket_ms']+step<=received]
  if not rows:
   store.event('EMPTY_PAGE',key,{'cursor':cursor},received);break
  nxt=max(x['bucket_ms'] for x in rows)+step
  if nxt<=cursor:raise ValueError('OpenMarket pagination did not advance')
  missing=[t for t in range(cursor,nxt,step) if t not in {x['bucket_ms'] for x in rows}]
  if missing and kind not in ('LIQUIDATION_AGG','TRADE_AGG'):raise ValueError('OpenMarket returned a gap; checkpoint not advanced')
  store.write_openmarket({'tool':'points','arguments':args,'response':data,'transport_body':getattr(client,'last_response_body',None)},rows,[],received,checkpoint=(key,nxt));cursor=nxt
 return cursor

def collect_websocket(store,config):
 try:import websocket
 except ImportError:raise RuntimeError('Install websocket-client to collect stream') from None
 from urllib.parse import urlsplit
 parsed=urlsplit(config['binance_index_stream'])
 if parsed.scheme!='wss' or parsed.netloc!='fstream.binance.com' or parsed.path!='/market/ws/btcusdt@markPrice@1s' or parsed.query or parsed.fragment:raise ValueError('Invalid index stream endpoint')
 def connect():
  try:
   ws=websocket.create_connection(config['binance_index_stream'],timeout=config['timeout_seconds'])
   try:
    while True:
     message=ws.recv()
     if not message:raise ConnectionError('WebSocket closed')
     yield json.loads(message)
   finally:ws.close()
  except websocket.WebSocketException as e:raise ConnectionError(type(e).__name__) from None
 return collect_stream(connect,store,config['max_websocket_connections'],gap_threshold_ms=config['stream_gap_threshold_ms'])
