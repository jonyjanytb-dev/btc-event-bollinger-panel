from .quality import closed

def export_dataset(store,decision_ms,horizon,enhanced=False,*,interval='FIVE_MINUTES',max_age_ms=None,required_metrics=None,metric_max_age_ms=None):
 if horizon not in ('30m','1h'):raise ValueError('Unsupported horizon')
 b=store.db.execute('SELECT * FROM bars WHERE dataset=? AND close_ms<? AND available_ms>close_ms AND available_ms<=? ORDER BY open_ms DESC,available_ms DESC,id DESC LIMIT 1',('binance:index:BTCUSDT:1m',decision_ms,decision_ms)).fetchone()
 if b is None:return []
 if max_age_ms is not None and decision_ms-b['close_ms']>max_age_ms:return []
 out={'decision_ms':decision_ms,'horizon':horizon,'index_close':b['close'],'index_bar_open_ms':b['open_ms'],'index_bar':{k:b[k] for k in ('open','high','low','close','open_ms','close_ms','available_ms')},'available_ms':b['available_ms'],'mode':'index_only'}
 if not enhanced:return [out]
 # A feature set must be declared; never silently return a partial enhanced dataset.
 if max_age_ms is None or not required_metrics:return []
 bars={}
 for market in ('BINANCE','BINANCE_FUTURES'):
  key=f'openmarket:{market}:BTCUSDT:{interval}'
  row=store.db.execute('SELECT * FROM bars WHERE dataset=? AND source_verified=1 AND close_ms<? AND available_ms>close_ms AND available_ms<=? AND close_ms>=? ORDER BY open_ms DESC,available_ms DESC,id DESC LIMIT 1',(key,decision_ms,decision_ms,decision_ms-max_age_ms)).fetchone()
  if row is None:return []
  bars[market]=row
 if bars['BINANCE']['open_ms']!=bars['BINANCE_FUTURES']['open_ms']:return []
 data={}
 for row in store.db.execute('SELECT kind,value,unit,event_ms,end_ms,available_ms,market,symbol,source_asof_ms,data_age_seconds FROM metrics WHERE source_verified=1 AND symbol=? AND market=? AND event_ms<=? AND available_ms<=? AND (end_ms IS NULL OR (end_ms<? AND end_ms<available_ms)) ORDER BY event_ms,available_ms,id',('BTCUSDT','BINANCE_FUTURES',decision_ms,decision_ms,decision_ms)):
  threshold=(metric_max_age_ms or {}).get(row['kind'],max_age_ms)
  if threshold<0:raise ValueError('Invalid feature age threshold')
  if row['value'] is None or decision_ms-row['event_ms']>threshold:continue
  if row['end_ms'] is None and (row['source_asof_ms'] is None or row['data_age_seconds'] is None or row['source_asof_ms']>decision_ms):continue
  if row['end_ms'] is not None and (row['event_ms']!=bars['BINANCE_FUTURES']['open_ms'] or row['end_ms']!=bars['BINANCE_FUTURES']['close_ms']):continue
  data[row['kind']]={'value':row['value'],'unit':row['unit'],'event_ms':row['event_ms'],'source_asof_ms':row['source_asof_ms'],'data_age_seconds':row['data_age_seconds'],'available_ms':row['available_ms'],'market':row['market']}
 if any(k not in data for k in required_metrics):return []
 out.update({'mode':'enhanced','market_bars':{k:{field:v[field] for field in ('open_ms','close_ms','open','high','low','close','volume','available_ms')} for k,v in bars.items()},'metrics':data})
 if 'TRADE_AGG:BUY' in data and 'TRADE_AGG:SELL' in data:
  from .openmarket import volume_delta
  if data['TRADE_AGG:BUY']['event_ms']!=data['TRADE_AGG:SELL']['event_ms']:return []
  out['volume_delta']=volume_delta(data['TRADE_AGG:BUY']['value'],data['TRADE_AGG:SELL']['value'])
 return [out]

def export_series(store,start_ms,end_ms,step_ms,horizons=('30m','1h'),enhanced=False,**options):
 if step_ms<=0 or start_ms>=end_ms:raise ValueError('Invalid dataset time window')
 for decision_ms in range(start_ms,end_ms,step_ms):
  for horizon in horizons:
   yield from export_dataset(store,decision_ms,horizon,enhanced,**options)
