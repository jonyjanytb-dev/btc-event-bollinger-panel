import hashlib
import json
import sqlite3
import time
from pathlib import Path

SCHEMA = '''
CREATE TABLE IF NOT EXISTS raw_events(id INTEGER PRIMARY KEY,source TEXT NOT NULL,kind TEXT NOT NULL,received_ms INTEGER NOT NULL,available_ms INTEGER NOT NULL,source_event_ms INTEGER,payload TEXT NOT NULL,sha256 TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS bars(id INTEGER PRIMARY KEY,dataset TEXT NOT NULL,source TEXT NOT NULL,market TEXT NOT NULL,symbol TEXT NOT NULL,interval TEXT NOT NULL,open_ms INTEGER NOT NULL,close_ms INTEGER NOT NULL,received_ms INTEGER NOT NULL,available_ms INTEGER NOT NULL,raw_id INTEGER REFERENCES raw_events(id),open TEXT NOT NULL,high TEXT NOT NULL,low TEXT NOT NULL,close TEXT NOT NULL,volume TEXT,hash TEXT NOT NULL,source_verified INTEGER NOT NULL DEFAULT 0,UNIQUE(dataset,open_ms,hash));
CREATE INDEX IF NOT EXISTS bars_asof ON bars(dataset,open_ms,available_ms);
CREATE TABLE IF NOT EXISTS metrics(id INTEGER PRIMARY KEY,source TEXT NOT NULL,market TEXT NOT NULL,symbol TEXT NOT NULL,kind TEXT NOT NULL,unit TEXT,event_ms INTEGER,end_ms INTEGER,received_ms INTEGER NOT NULL,available_ms INTEGER NOT NULL,raw_id INTEGER REFERENCES raw_events(id),value TEXT,payload TEXT NOT NULL,fingerprint TEXT NOT NULL UNIQUE,source_asof_ms INTEGER,data_age_seconds TEXT,source_verified INTEGER NOT NULL DEFAULT 0);
CREATE INDEX IF NOT EXISTS metrics_asof ON metrics(symbol,market,event_ms,available_ms);
CREATE TABLE IF NOT EXISTS ticks(id INTEGER PRIMARY KEY,source TEXT NOT NULL,event_ms INTEGER NOT NULL,received_ms INTEGER NOT NULL,available_ms INTEGER NOT NULL,symbol TEXT NOT NULL,index_price TEXT NOT NULL,mark_price TEXT NOT NULL,raw_id INTEGER REFERENCES raw_events(id),payload TEXT NOT NULL,UNIQUE(source,event_ms,symbol,index_price,mark_price));
CREATE TABLE IF NOT EXISTS orderbooks(id INTEGER PRIMARY KEY,source TEXT NOT NULL,market TEXT NOT NULL,symbol TEXT NOT NULL,event_ms INTEGER NOT NULL,received_ms INTEGER NOT NULL,available_ms INTEGER NOT NULL,raw_id INTEGER NOT NULL REFERENCES raw_events(id),block_size TEXT NOT NULL,fingerprint TEXT NOT NULL UNIQUE);
CREATE TABLE IF NOT EXISTS book_levels(book_id INTEGER NOT NULL REFERENCES orderbooks(id),side TEXT NOT NULL,level INTEGER NOT NULL,price TEXT NOT NULL,quantity TEXT NOT NULL,PRIMARY KEY(book_id,side,level));
CREATE TABLE IF NOT EXISTS quality(id INTEGER PRIMARY KEY,kind TEXT NOT NULL,dataset TEXT NOT NULL,observed_ms INTEGER NOT NULL,detail TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS checkpoints(dataset TEXT PRIMARY KEY,next_ms INTEGER NOT NULL);
'''
class Store:
 def __init__(self,path):
  self.db=sqlite3.connect(str(path));self.db.row_factory=sqlite3.Row
  self.db.execute('PRAGMA foreign_keys=ON')
  version=self.db.execute('PRAGMA user_version').fetchone()[0]
  app_id=self.db.execute('PRAGMA application_id').fetchone()[0]
  tables=self.db.execute("SELECT count(*) FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'").fetchone()[0]
  if version not in (0,1,2) or (version==0 and tables) or (version in (1,2) and app_id!=1112818481):
   self.db.close();raise ValueError('Unsupported existing database schema; migration required')
  if version==0:
   self.db.executescript('BEGIN;\n'+SCHEMA+'\nPRAGMA application_id=1112818481;\nPRAGMA user_version=2;\nCOMMIT;')
  elif version==1:
   with self.db:
    self.db.execute('ALTER TABLE metrics ADD COLUMN source_asof_ms INTEGER')
    self.db.execute('ALTER TABLE metrics ADD COLUMN data_age_seconds TEXT')
    self.db.execute('ALTER TABLE metrics ADD COLUMN source_verified INTEGER NOT NULL DEFAULT 0')
    self.db.execute('ALTER TABLE bars ADD COLUMN source_verified INTEGER NOT NULL DEFAULT 0')
    self.db.execute('PRAGMA user_version=2')
 def close(self):self.db.close()
 def checkpoint(self,dataset):
  r=self.db.execute('SELECT next_ms FROM checkpoints WHERE dataset=?',(dataset,)).fetchone();return r[0] if r else None
 def reconcile_history_checkpoint(self,dataset,key,start_ms,end_ms):
  """Repair checkpoints written by older versions that skipped recorded gaps."""
  with self.db:
   cursor=self.checkpoint(key)
   if cursor is None:return None
   unresolved=[]
   for row in self.db.execute("SELECT detail FROM quality WHERE dataset=? AND kind='GAP'",(dataset,)):
    detail=json.loads(row['detail'])
    if detail.get('checkpoint') not in (None,key):continue
    for t in detail.get('missing_opens',[]):
     if start_ms<=t<min(cursor,end_ms) and self.db.execute('SELECT 1 FROM bars WHERE dataset=? AND open_ms=? AND available_ms>close_ms LIMIT 1',(dataset,t)).fetchone() is None:unresolved.append(t)
   if unresolved:
    new_cursor=min(unresolved)
    self.db.execute('UPDATE checkpoints SET next_ms=? WHERE dataset=?',(new_cursor,key))
    self.db.execute('INSERT INTO quality(kind,dataset,observed_ms,detail) VALUES(?,?,?,?)',('CHECKPOINT_REWOUND',dataset,int(time.time()*1000),json.dumps({'checkpoint':key,'old':cursor,'new':new_cursor})))
    return new_cursor
   return cursor
 def write_batch(self,dataset,bars,next_ms,raw=None,received_ms=None,quality_events=(),checkpoint_dataset=None):
  with self.db:
   raw_id=None
   if raw is not None:
    payload=json.dumps(raw,ensure_ascii=False,separators=(',',':'),default=str); ts=received_ms if received_ms is not None else int(time.time()*1000)
    raw_id=self.db.execute('INSERT INTO raw_events(source,kind,received_ms,available_ms,payload,sha256) VALUES(?,?,?,?,?,?)',('binance','index_klines',ts,ts,payload,hashlib.sha256(payload.encode()).hexdigest())).lastrowid
   for b in bars:
    keys=('source','market','symbol','interval','open_ms','close_ms','available_ms','open','high','low','close','volume'); vals=[b[k] for k in keys]
    signature=hashlib.sha256(json.dumps([*[b[k] for k in keys if k!='available_ms'],b['available_ms']>b['close_ms']],separators=(',',':')).encode()).hexdigest()
    insert=self.db.execute('INSERT OR IGNORE INTO bars(dataset,source,market,symbol,interval,open_ms,close_ms,available_ms,open,high,low,close,volume,hash,received_ms,raw_id) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',[dataset,*vals,signature,received_ms if received_ms is not None else b['available_ms'],raw_id])
    if insert.rowcount==0:self.db.execute('INSERT INTO quality(kind,dataset,observed_ms,detail) VALUES(?,?,?,?)',('DUPLICATE',dataset,b['available_ms'],json.dumps({'open_ms':b['open_ms']})))
   for item in quality_events:self.db.execute('INSERT INTO quality(kind,dataset,observed_ms,detail) VALUES(?,?,?,?)',(item['kind'],dataset,received_ms,json.dumps(item,default=str)))
   self.db.execute('INSERT INTO checkpoints VALUES(?,?) ON CONFLICT(dataset) DO UPDATE SET next_ms=MAX(checkpoints.next_ms,excluded.next_ms)',(checkpoint_dataset or dataset,next_ms))
 def bars(self,dataset):return [dict(x) for x in self.db.execute('SELECT * FROM bars WHERE dataset=? ORDER BY open_ms,available_ms,id',(dataset,))]
 def record_tick(self,t,payload,quality_events=()):
  raw=json.dumps(payload,separators=(',',':'),default=str)
  with self.db:
   source=payload.get('source','websocket');raw_id=self.db.execute('INSERT INTO raw_events(source,kind,received_ms,available_ms,source_event_ms,payload,sha256) VALUES(?,?,?,?,?,?,?)',('binance',source,t['received_ms'],t['received_ms'],t['event_ms'],raw,hashlib.sha256(raw.encode()).hexdigest())).lastrowid
   self.db.execute('INSERT OR IGNORE INTO ticks(source,event_ms,received_ms,available_ms,symbol,index_price,mark_price,raw_id,payload) VALUES(?,?,?,?,?,?,?,?,?)',(source,t['event_ms'],t['received_ms'],t['received_ms'],t['symbol'],t['index_price'],t['mark_price'],raw_id,raw))
   for item in quality_events:self.db.execute('INSERT INTO quality(kind,dataset,observed_ms,detail) VALUES(?,?,?,?)',(item['kind'],'binance:index:BTCUSDT',t['received_ms'],json.dumps(item,default=str)))
 def event(self,kind,dataset,detail,now):
  with self.db:self.db.execute('INSERT INTO quality(kind,dataset,observed_ms,detail) VALUES(?,?,?,?)',(kind,dataset,now,json.dumps(detail,default=str)))
 def write_openmarket(self,raw,points,metrics,received_ms,books=(),block_size=None,checkpoint=None):
  payload=json.dumps(raw,ensure_ascii=False,separators=(',',':'),default=str)
  steps={'MINUTE':60000,'FIVE_MINUTES':300000,'FIFTEEN_MINUTES':900000,'THIRTY_MINUTES':1800000,'HOUR':3600000,'FOUR_HOURS':14400000}
  with self.db:
   raw_id=self.db.execute('INSERT INTO raw_events(source,kind,received_ms,available_ms,payload,sha256) VALUES(?,?,?,?,?,?)',('openmarket','mcp',received_ms,received_ms,payload,hashlib.sha256(payload.encode()).hexdigest())).lastrowid
   for row in points:
    kind=row['kind'];market=row.get('market') or 'unknown';symbol=row.get('symbol') or 'unknown'; t=row['bucket_ms']; avail=row['available_ms']
    if kind=='TRADE_SIDE_AGNOSTIC_AGG' and all(row.get(k) is not None for k in ('open','high','low','close')):
     step=steps[row['interval']];ds=f'openmarket:{market}:{symbol}:{row["interval"]}';vals=[str(row.get(x)) if row.get(x) is not None else None for x in ('open','high','low','close','volume')];signature=hashlib.sha256(json.dumps([kind,market,symbol,t,vals,avail>t+step-1,row.get('source_verified') is True]).encode()).hexdigest()
     insert=self.db.execute('INSERT OR IGNORE INTO bars(dataset,source,market,symbol,interval,open_ms,close_ms,received_ms,available_ms,raw_id,open,high,low,close,volume,hash,source_verified) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',(ds,'openmarket',market,symbol,row['interval'],t,t+step-1,received_ms,avail,raw_id,*vals,signature,int(row.get('source_verified') is True)))
     if insert.rowcount==0:self.db.execute('INSERT INTO quality(kind,dataset,observed_ms,detail) VALUES(?,?,?,?)',('DUPLICATE',ds,received_ms,json.dumps({'open_ms':t})))
    else:
     field={'OPEN_INTEREST_AGG':'close','FUNDING_RATE_AGG':'rateClose','LIQUIDATION_AGG':'liquidations','TRADE_AGG':'volume'}.get(kind)
     if field:
      label=kind+(':'+row['side'] if row.get('side') else '')
      end_ms=t+steps[row['interval']]-1
      fingerprint=hashlib.sha256(json.dumps([{k:v for k,v in row.items() if k!='available_ms'},avail>end_ms],sort_keys=True,default=str).encode()).hexdigest()
      self.db.execute('INSERT OR IGNORE INTO metrics(source,market,symbol,kind,unit,event_ms,end_ms,received_ms,available_ms,raw_id,value,payload,fingerprint,source_verified) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)',('openmarket',market,symbol,label,row.get('unit'),t,end_ms,received_ms,avail,raw_id,row.get(field),json.dumps(row,default=str),fingerprint,int(row.get('source_verified') is True)))
   for metric in metrics:
    fingerprint=hashlib.sha256(json.dumps({k:v for k,v in metric.items() if k not in ('received_ms','available_ms')},sort_keys=True,default=str).encode()).hexdigest()
    self.db.execute('INSERT OR IGNORE INTO metrics(source,market,symbol,kind,unit,event_ms,end_ms,received_ms,available_ms,raw_id,value,payload,fingerprint,source_asof_ms,data_age_seconds,source_verified) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',('openmarket',metric.get('market','unknown'),metric.get('symbol','BTCUSDT'),metric['kind'],metric.get('unit'),metric['event_ms'],None,received_ms,metric['available_ms'],raw_id,metric.get('value'),json.dumps(metric,default=str),fingerprint,metric.get('source_asof_ms'),metric.get('data_age_seconds'),int(metric.get('source_verified') is True)))
   for book in books:
    if block_size is None:raise ValueError('Block size required')
    fingerprint=hashlib.sha256(json.dumps([book['market'],book['symbol'],book['event_ms'],str(block_size),book['bids'],book['asks']],separators=(',',':')).encode()).hexdigest()
    insert=self.db.execute('INSERT OR IGNORE INTO orderbooks(source,market,symbol,event_ms,received_ms,available_ms,raw_id,block_size,fingerprint) VALUES(?,?,?,?,?,?,?,?,?)',('openmarket',book['market'],book['symbol'],book['event_ms'],received_ms,book['available_ms'],raw_id,str(block_size),fingerprint))
    if insert.rowcount==0:
     self.db.execute('INSERT INTO quality(kind,dataset,observed_ms,detail) VALUES(?,?,?,?)',('DUPLICATE','openmarket:orderbook',received_ms,json.dumps({'event_ms':book['event_ms']})));continue
    book_id=insert.lastrowid
    for side in ('bids','asks'):
     for level,(price,quantity) in enumerate(book[side]):
      self.db.execute('INSERT INTO book_levels VALUES(?,?,?,?,?)',(book_id,side,level,price,quantity))
   if checkpoint:
    dataset,next_ms=checkpoint
    self.db.execute('INSERT INTO checkpoints VALUES(?,?) ON CONFLICT(dataset) DO UPDATE SET next_ms=MAX(checkpoints.next_ms,excluded.next_ms)',(dataset,next_ms))
