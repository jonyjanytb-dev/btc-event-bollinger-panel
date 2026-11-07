import time
from decimal import Decimal,InvalidOperation
from .quality import closed
INTERVALS={'1m':60000,'5m':300000,'15m':900000,'30m':1800000,'1h':3600000,'4h':14400000}
class BinanceHistory:
 def __init__(self,store,fetch,limit,now=lambda:int(time.time()*1000)): self.store,self.fetch,self.limit,self.now=store,fetch,limit,now
 def run(self,interval,start_ms,end_ms,received_ms):
  step=INTERVALS[interval]
  if start_ms<0 or start_ms>=end_ms or start_ms%step or end_ms%step:raise ValueError('History window must be UTC interval-aligned')
  dataset=f'binance:index:BTCUSDT:{interval}';checkpoint=f'{dataset}:range:{start_ms}:{end_ms}';current=self.store.reconcile_history_checkpoint(dataset,checkpoint,start_ms,end_ms);cursor=max(start_ms,current if current is not None else start_ms)
  while cursor<end_ms:
   raw=self.fetch({'pair':'BTCUSDT','interval':interval,'startTime':cursor,'endTime':end_ms-1,'limit':self.limit})
   if not isinstance(raw,list):raise ValueError('Unexpected Binance response')
   if not raw: self.store.event('EMPTY_PAGE',dataset,{'cursor':cursor},received_ms);break
   observed_ms=self.now();bars=[]
   for row in raw:
    t=int(row[0]);close_ms=int(row[6])
    if t<cursor or t>=end_ms or not closed(close_ms,received_ms):continue
    try:
     prices=[Decimal(str(x)) for x in row[1:5]]
     if len(prices)!=4 or not all(x.is_finite() and x>0 for x in prices):raise ValueError('Invalid index prices')
     op,high,low,cl=prices
     if t%step or close_ms!=t+step-1 or low>min(op,cl) or high<max(op,cl) or low>high:raise ValueError('Invalid index candle')
    except (InvalidOperation,IndexError,TypeError):raise ValueError('Invalid index candle') from None
    bars.append(dict(source='binance',market='index',symbol='BTCUSDT',interval=interval,open_ms=t,close_ms=close_ms,available_ms=observed_ms,open=str(row[1]),high=str(row[2]),low=str(row[3]),close=str(row[4]),volume=None))
   if not bars: self.store.event('NO_CLOSED_DATA',dataset,{'cursor':cursor},received_ms);break
   nxt=max(b['open_ms'] for b in bars)+step
   if nxt<=cursor:raise ValueError('Pagination did not advance')
   observed={b['open_ms'] for b in bars}; missing=[t for t in range(cursor,nxt,step) if t not in observed]
   quality=[{'kind':'GAP','missing_opens':missing,'checkpoint':checkpoint}] if missing else []
   contiguous=missing[0] if missing else nxt
   self.store.write_batch(dataset,bars,contiguous,raw,observed_ms,quality,checkpoint);cursor=contiguous
   if missing:break  # Next invocation resumes at the first unfilled interval.
  return cursor

def parse_tick(payload,received_ms):
 if payload.get('e')!='markPriceUpdate' or payload.get('s')!='BTCUSDT':raise ValueError('Unexpected stream event')
 try:
  prices=[Decimal(str(payload[k])) for k in ('i','p')]
  if not all(p.is_finite() and p>0 for p in prices):raise ValueError('Invalid stream price')
 except (InvalidOperation,KeyError):raise ValueError('Invalid stream price') from None
 return {'event_ms':int(payload['E']),'received_ms':received_ms,'symbol':payload['s'],'index_price':str(payload['i']),'mark_price':str(payload['p'])}

def collect_stream(connect,store,max_connections,sleep=time.sleep,now=lambda:int(time.time()*1000),gap_threshold_ms=1500):
 count=0;previous=store.db.execute("SELECT MAX(event_ms) FROM ticks WHERE source='websocket' AND symbol='BTCUSDT'").fetchone()[0]
 for attempt in range(max_connections):
  first=True
  try:
   for payload in connect():
    try:t=parse_tick(payload,now())
    except (ValueError,KeyError,TypeError):
     store.event('INVALID_TICK','binance:index:BTCUSDT',{'reason':'invalid_payload'},now());raise
    events=[]
    if first and attempt>0:events.append({'kind':'RECONNECTED','attempt':attempt+1})
    first=False
    if previous is not None and t['event_ms']-previous>gap_threshold_ms:events.append({'kind':'GAP','previous':previous,'next':t['event_ms']})
    if previous is not None and t['event_ms']<previous:events.append({'kind':'OUT_OF_ORDER','previous':previous,'next':t['event_ms']})
    store.record_tick(t,payload,events);previous=max(previous,t['event_ms']) if previous is not None else t['event_ms'];count+=1
  except (ConnectionError,TimeoutError,OSError) as e:store.event('DISCONNECT','binance:index:BTCUSDT',{'error':type(e).__name__},now())
  if attempt+1<max_connections:sleep(min(2**attempt,30))
 return count
