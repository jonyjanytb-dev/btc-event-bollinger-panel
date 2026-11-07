"""Read-only Binance Futures candles for the 1m/5m Bollinger strategy."""
import argparse,json,os,signal,sqlite3,time
from decimal import Decimal,InvalidOperation
from pathlib import Path
import requests

RULES=json.loads(Path(__file__).with_name('chart_rules.json').read_text())
SOURCE=RULES['source_id']
PERIOD=RULES['five_min']['period_ms']

def reconstructed_band(bars, index, period):
 length=RULES['bb_length'];start=index-length+1
 if start<0:raise ValueError('insufficient bollinger history')
 closes=[];previous=None
 for bar in bars[start:index+1]:
  t=int(bar[0]);o,h,l,c=(Decimal(str(bar[j])) for j in (1,2,3,4))
  if (previous is not None and t!=previous+period) or not all(v.is_finite() and v>0 for v in (o,h,l,c)) or not l<=min(o,c)<=max(o,c)<=h:
   raise ValueError('invalid or noncontiguous bollinger candle')
  previous=t;closes.append(c)
 middle=sum(closes)/Decimal(length)
 deviation=(sum((value-middle)**2 for value in closes)/Decimal(length)).sqrt()
 spread=Decimal(RULES['bb_stddev_multiplier'])*deviation
 return middle+spread,middle,middle-spread

def closed_chart(data, key, received_ms):
 spec=RULES[key];period=spec['period_ms'];bars=data['candles']
 if (data['status']!='ok' or data.get('source')!=SOURCE or
     (data['symbol'],data['interval'])!=(RULES['symbol'],spec['interval'])):
  return None,'SOURCE_MISMATCH'
 if len(bars)<RULES['minimum_indicator_bars']:
  return None,'INDICATOR_HISTORY_UNAVAILABLE'
 closed=[i for i,b in enumerate(bars) if int(b[0])+period<=received_ms-RULES['minimum_close_lag_ms']]
 if not closed or closed[-1]<RULES['minimum_indicator_bars']-1:return None,'INSUFFICIENT_CLOSED_BARS'
 i=closed[-1];bar=bars[i];t=int(bar[0]);end=t+period
 if i!=len(bars)-1 or not RULES['minimum_close_lag_ms']<=received_ms-end<RULES['maximum_close_lag_ms']:
  return None,'STALE_OR_NONCONTIGUOUS'
 for j in range(i-RULES['minimum_indicator_bars']+1,i+1):
  current=bars[j]
  if type(current[0]) is not int or (j>i-RULES['minimum_indicator_bars']+1
                                  and current[0]!=int(bars[j-1][0])+period):
   return None,'STALE_OR_NONCONTIGUOUS'
  o0,h0,l0,c0=(Decimal(str(current[k])) for k in (1,2,3,4))
  volume=Decimal(str(current[5]))
  if (not all(v.is_finite() and v>0 for v in (o0,h0,l0,c0))
      or not volume.is_finite() or volume<0 or not l0<=min(o0,c0)<=max(o0,c0)<=h0):
   return None,'INVALID_OR_MISSING_DATA'
 upper,middle,lower=reconstructed_band(bars,i,period)
 o,h,l,c=(Decimal(str(bar[j])) for j in (1,2,3,4))
 if not all(v.is_finite() and v>0 for v in (o,h,l,c,upper,middle,lower)) or not l<=min(o,c)<=max(o,c)<=h or not lower<=middle<=upper:
  raise ValueError('invalid candle or band')
 return {'open_ms':t,'close_ms':end,'touch_lower':l<=lower,'touch_upper':h>=upper},None

def evaluate(data,received_ms):
 def unavailable(reason):return {'direction':'UNAVAILABLE','reason':reason}
 try:
  five,reason=closed_chart(data['five_min'],'five_min',received_ms)
  if reason:return unavailable(reason)
  one,reason=closed_chart(data['one_min'],'one_min',received_ms)
  if reason:return unavailable(reason)
  if one['close_ms']!=five['close_ms'] or one['open_ms']!=five['close_ms']-RULES['one_min']['period_ms']:
   return unavailable('CHARTS_NOT_ALIGNED')
  lower=five['touch_lower'] and one['touch_lower'];upper=five['touch_upper'] and one['touch_upper']
  if lower and upper:direction,reason='NO_SIGNAL','AMBIGUOUS_TOUCH'
  elif lower:direction,reason='HIGHER','BOTH_LOWER_TOUCHED'
  elif upper:direction,reason='LOWER','BOTH_UPPER_TOUCHED'
  else:direction,reason='NO_SIGNAL','CONDITIONS_NOT_MET'
  entry=(received_ms//RULES['entry_step_ms']+1)*RULES['entry_step_ms']
  return {'direction':direction,'reason':reason,'bar_open_ms':five['open_ms'],'bar_close_ms':five['close_ms'],
          'entry_ms':entry,'expiry_ms':entry+RULES['horizon_ms'],'strategy_version':RULES['version'],
          'source':SOURCE}
 except (KeyError,TypeError,IndexError,ValueError,InvalidOperation,OverflowError):return unavailable('INVALID_OR_MISSING_DATA')

class BinanceFuturesClient:
 def __init__(self,session=None,clock=None,sleep=None):
  self.session=session or requests.Session();self.session.trust_env=False
  self.clock=clock or (lambda:time.time_ns()//1000000)
  self.sleep=sleep or time.sleep
 def _bars(self,key,asof_ms):
  spec=RULES[key];period=spec['period_ms']
  end_ms=(asof_ms//period)*period-1
  params={'symbol':RULES['symbol'],'interval':spec['interval'],
          'endTime':end_ms,'limit':RULES['market_history_bars']}
  for attempt in range(RULES['market_request_attempts']):
   try:
    response=self.session.get(RULES['market_data_url'],params=params,
                              timeout=RULES['market_request_timeout_seconds'],
                              allow_redirects=False)
    response.raise_for_status()
    raw=response.json()
    break
   except requests.RequestException:
    if attempt+1==RULES['market_request_attempts']:raise
    self.sleep(RULES['market_retry_delay_seconds'])
  if not isinstance(raw,list) or len(raw)<RULES['minimum_indicator_bars'] or len(raw)>RULES['market_history_bars']:
   raise ValueError('MARKET_HISTORY_UNAVAILABLE')
  bars=[]
  for item in raw:
   if (not isinstance(item,list) or len(item)<7 or type(item[0]) is not int
       or type(item[6]) is not int or item[6]!=item[0]+period-1
       or item[0]>end_ms or (bars and item[0]!=bars[-1][0]+period)):
    raise ValueError('MARKET_CANDLE_INVALID')
   bars.append([item[0],*(str(item[i]) for i in (1,2,3,4,5))])
  return {'status':'ok','source':SOURCE,'symbol':RULES['symbol'],
          'interval':spec['interval'],'truncated':False,'candles':bars}
 def snapshot(self):
  asof_ms=self.clock()
  result={key:self._bars(key,asof_ms) for key in ('five_min','one_min')}
  received=self.clock()
  return result,received

def open_db(path):
 db=sqlite3.connect(path,timeout=10);db.execute('PRAGMA journal_mode=WAL');db.execute('CREATE TABLE IF NOT EXISTS observations(id INTEGER PRIMARY KEY,received_ms INTEGER NOT NULL,workspace TEXT NOT NULL,payload TEXT NOT NULL,decision TEXT NOT NULL,bar_open_ms INTEGER,entry_ms INTEGER,expiry_ms INTEGER,execution TEXT NOT NULL)');db.execute('CREATE UNIQUE INDEX IF NOT EXISTS unique_signal ON observations(bar_open_ms) WHERE bar_open_ms IS NOT NULL');db.execute("CREATE TABLE IF NOT EXISTS proxy_results(observation_id INTEGER PRIMARY KEY REFERENCES observations(id),status TEXT NOT NULL,entry_price TEXT,entry_event_ms INTEGER,entry_received_ms INTEGER,exit_price TEXT,exit_event_ms INTEGER,exit_received_ms INTEGER,direction_result TEXT,resolved_ms INTEGER NOT NULL)");db.commit();return db

def resolve_proxy(db,tick_path,now_ms,max_lag_ms=5000):
 uri=Path(tick_path).resolve().as_uri()+'?mode=ro'; ticks=sqlite3.connect(uri,uri=True,timeout=10)
 try:
  pending=db.execute("select id,decision,entry_ms,expiry_ms from observations where entry_ms is not null and id not in (select observation_id from proxy_results where status!='PENDING')").fetchall()
  for ident,decision,entry,expiry in pending:
   direction=json.loads(decision).get('direction')
   if direction not in ('HIGHER','LOWER') or now_ms<entry+max_lag_ms:continue
   values=[]
   for target in (entry,expiry):
    values.append(ticks.execute('select index_price,event_ms,received_ms from ticks where event_ms>=? and event_ms<=? and received_ms<=? order by event_ms,received_ms limit 1',(target,target+max_lag_ms,target+max_lag_ms)).fetchone())
   a,b=values;status=('VERIFIED' if a and b else 'MISSING') if now_ms>=expiry+max_lag_ms else 'PENDING';result=None
   if status=='VERIFIED':
    first,last=Decimal(a[0]),Decimal(b[0]);result='HIGHER' if last>first else 'LOWER' if last<first else 'EQUAL'
   with db:db.execute('insert or replace into proxy_results values(?,?,?,?,?,?,?,?,?,?)',(ident,status,*(a or (None,None,None)),*(b or (None,None,None)),result,now_ms))
 finally:ticks.close()

def require_healthy_clock(path, now_ms, max_age_ms, max_error_ms):
 db=sqlite3.connect(Path(path).resolve().as_uri()+'?mode=ro',uri=True,timeout=5)
 try:row=db.execute("select status,checked_ms,detail from health where source='clock'").fetchone()
 finally:db.close()
 try:
  if not row or row[0]!='OK' or not 0<=now_ms-row[1]<=max_age_ms:raise ValueError('CLOCK_UNAVAILABLE')
  bound=float(json.loads(row[2])['bound_ms'])
  if not 0<=bound<=max_error_ms:raise ValueError('CLOCK_UNAVAILABLE')
 except (KeyError,TypeError,ValueError):raise ValueError('CLOCK_UNAVAILABLE') from None

def require_healthy_index(path, now_ms, max_age_ms):
 """A chart signal needs a current, actually received index tick."""
 db=sqlite3.connect(Path(path).resolve().as_uri()+'?mode=ro',uri=True,timeout=5)
 try:row=db.execute("select status,checked_ms,detail from health where source='index'").fetchone()
 finally:db.close()
 try:
  if not row or row[0]!='OK' or not 0<=now_ms-row[1]<=max_age_ms:
   raise ValueError('INDEX_UNAVAILABLE')
  event_ms=json.loads(row[2])['event_ms']
  if type(event_ms) is not int or abs(now_ms-event_ms)>max_age_ms:
   raise ValueError('INDEX_UNAVAILABLE')
 except (KeyError,TypeError,ValueError):raise ValueError('INDEX_UNAVAILABLE') from None

def capture(client,db,clock_check=None):
 data,received=client.snapshot()
 if clock_check:clock_check(received)
 result=evaluate(data,received)
 with db:
  if result['direction'] in ('HIGHER','LOWER'):
   current=db.execute("SELECT decision,expiry_ms FROM observations WHERE entry_ms IS NOT NULL AND expiry_ms>?",(result['entry_ms'],)).fetchall()
   if any(json.loads(row[0]).get('direction') in ('HIGHER','LOWER') for row in current):result['direction']='NO_SIGNAL';result['reason']='OVERLAPPING_POSITION'
  db.execute('INSERT OR IGNORE INTO observations(received_ms,workspace,payload,decision,bar_open_ms,entry_ms,expiry_ms,execution) VALUES(?,?,?,?,?,?,?,?)',(received,SOURCE,json.dumps(data,ensure_ascii=False,default=str),json.dumps(result),result.get('bar_open_ms'),result.get('entry_ms'),result.get('expiry_ms'),'NO TRADE'))
 return result

def main():
 p=argparse.ArgumentParser();p.add_argument('--config',required=True);p.add_argument('--database',required=True);p.add_argument('--once',action='store_true');p.add_argument('--ticks-db');a=p.parse_args()
 config=json.loads(Path(a.config).read_text());path=Path(a.database).expanduser();path.parent.mkdir(mode=0o700,parents=True,exist_ok=True);os.umask(0o077);db=open_db(path);client=BinanceFuturesClient();stopped=False
 def stop(*_):
  nonlocal stopped;stopped=True
 signal.signal(signal.SIGTERM,stop);signal.signal(signal.SIGINT,stop)
 def health_check(received):
  require_healthy_clock(a.ticks_db,received,config['health_max_age_ms'],config['clock_max_error_ms'])
  require_healthy_index(a.ticks_db,received,config['tick_max_delay_ms'])
 try:
  first=True
  while not stopped:
   now=time.time_ns()//1000000
   try:
    if a.ticks_db:resolve_proxy(db,a.ticks_db,now-1000)
    if first or now%PERIOD<55000:
     print(json.dumps(capture(client,db,health_check if a.ticks_db else None),ensure_ascii=False),flush=True)
   except (ValueError,KeyError,TypeError,requests.RequestException,OSError,sqlite3.Error) as exc:
    code=str(exc) if isinstance(exc,ValueError) and str(exc) in ('INDEX_UNAVAILABLE','CLOCK_UNAVAILABLE') else None
    print(json.dumps({'status':'UNAVAILABLE','error_type':type(exc).__name__,'error_code':code}),flush=True)
   first=False
   if a.once:break
   for _ in range(10):
    if stopped:break
    time.sleep(1)
 finally:db.close();client.session.close()
if __name__=='__main__':main()
