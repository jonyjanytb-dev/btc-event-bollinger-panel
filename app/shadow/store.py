"""Durable single-host SQLite journal. Never creates orders."""
import hashlib,json,math,sqlite3
from decimal import Decimal,InvalidOperation


def _price(value):
 try:
  p=Decimal(str(value))
  if not p.is_finite() or p<=0:raise ValueError('invalid price')
  return str(value)
 except (InvalidOperation,TypeError):raise ValueError('invalid price') from None

def validate_candidate(p,recorded_ms):
 required=('classification','symbol','mechanism','training_spacing_minutes','horizon_minutes','decision_ms','entry_ms','expiry_ms','feature_end_ms','feature_available_ms','features','model_version','screen_version','screen_passed','screen_metrics','windows','probability_up','candidate_direction','execution_decision','no_trade_reasons','clock_uncertainty_ms','research_payout_ratio','live_quote_verified')
 if any(k not in p for k in required):raise ValueError('missing candidate fields')
 if p['classification']!='shadow_v1' or p['symbol']!='BTCUSDT' or p['horizon_minutes'] not in (30,60) or p['mechanism'] not in ('FIXED','ROLLING'):raise ValueError('unsupported profile')
 step=(p['horizon_minutes'] if p['mechanism']=='FIXED' else 1)*60000
 if p['entry_ms']%step or p['expiry_ms']!=p['entry_ms']+p['horizon_minutes']*60000:raise ValueError('invalid entry or expiry')
 if p['training_spacing_minutes']!=step//60000 or p['feature_end_ms']!=p['entry_ms']-60001 or p['feature_end_ms']>=p['decision_ms']:raise ValueError('invalid strategy sampling or feature boundary')
 if p['research_payout_ratio']!='0.85' or p['live_quote_verified'] is not False:raise ValueError('invalid research payout or quote status')
 windows=p['windows'];names=('train','calibration','screen');durations=(30,4,3);day=86400000
 if not isinstance(windows,dict) or any(not isinstance(windows.get(k),dict) for k in names):raise ValueError('missing protocol windows')
 for name,duration in zip(names,durations):
  w=windows[name]
  if any(k not in w for k in ('start_ms','end_ms','last_exit_ms','samples')) or w['end_ms']-w['start_ms']!=duration*day or w['last_exit_ms']>=w['end_ms'] or w['samples']<=0:raise ValueError('invalid historical window')
 if windows['train']['end_ms']!=windows['calibration']['start_ms'] or windows['calibration']['end_ms']!=windows['screen']['start_ms'] or windows['screen']['end_ms']>windows.get('training_cutoff_ms',-1) or windows.get('training_cutoff_ms',-1)>p['decision_ms'] or not windows['screen']['end_ms']<=p['entry_ms']<windows['screen']['end_ms']+7*day:raise ValueError('historical windows cross decision or test fold')
 uncertainty=p['clock_uncertainty_ms']
 if not isinstance(uncertainty,(int,float)) or not math.isfinite(uncertainty) or uncertainty<0:raise ValueError('invalid clock uncertainty')
 if not p['feature_available_ms']<=p['decision_ms']<=recorded_ms or recorded_ms+math.ceil(uncertainty)>=p['entry_ms']:raise ValueError('candidate is late or uses future features')
 if not p['features'] or not isinstance(p['features'],dict):raise ValueError('missing features')
 for feature in p['features'].values():
  if not isinstance(feature,dict) or not feature.get('source') or feature.get('value') is None or feature.get('event_ms') is None or feature.get('available_ms') is None or not feature['event_ms']<=feature['available_ms']<=p['feature_available_ms']<=p['decision_ms']:raise ValueError('invalid feature availability')
  try:
   if not Decimal(str(feature['value'])).is_finite():raise ValueError('invalid feature value')
  except (InvalidOperation,TypeError):raise ValueError('invalid feature value') from None
 if not p['model_version'] or not p['screen_version'] or not isinstance(p['screen_passed'],bool):raise ValueError('missing model or screen provenance')
 if not isinstance(p['screen_metrics'],dict) or p['screen_metrics'].get('passed')!=p['screen_passed']:raise ValueError('screen metrics mismatch')
 prob=p['probability_up']
 if not isinstance(prob,(int,float)) or not math.isfinite(prob) or not 0<=prob<=1 or p['candidate_direction']!=('HIGHER' if prob>=.5 else 'LOWER'):raise ValueError('invalid probability or direction')
 if p['execution_decision']!='NO TRADE' or 'verified_live_quote_unavailable' not in p['no_trade_reasons']:raise ValueError('shadow execution must remain NO TRADE')
 if not p['screen_passed'] and 'historical_screen_failed' not in p['no_trade_reasons']:raise ValueError('missing failed-screen reason')

SCHEMA='''
CREATE TABLE IF NOT EXISTS predictions(id TEXT PRIMARY KEY,symbol TEXT NOT NULL,mechanism TEXT NOT NULL,horizon_minutes INTEGER NOT NULL,decision_ms INTEGER NOT NULL,recorded_ms INTEGER NOT NULL,entry_ms INTEGER NOT NULL,expiry_ms INTEGER NOT NULL,features TEXT NOT NULL,model_version TEXT NOT NULL,screen_version TEXT NOT NULL,screen_passed INTEGER NOT NULL,probability_up REAL NOT NULL,candidate_direction TEXT NOT NULL,execution_decision TEXT NOT NULL,no_trade_reasons TEXT NOT NULL,metadata TEXT NOT NULL,entry_price TEXT,entry_event_ms INTEGER,entry_received_ms INTEGER,exit_price TEXT,exit_event_ms INTEGER,exit_received_ms INTEGER,result_status TEXT NOT NULL DEFAULT 'PENDING',direction_result TEXT,classification TEXT NOT NULL DEFAULT 'legacy_development_test',UNIQUE(symbol,mechanism,horizon_minutes,entry_ms));
CREATE TABLE IF NOT EXISTS ticks(event_ms INTEGER NOT NULL,received_ms INTEGER NOT NULL,index_price TEXT NOT NULL,mark_price TEXT NOT NULL,PRIMARY KEY(event_ms,index_price,mark_price));
CREATE TABLE IF NOT EXISTS health(source TEXT PRIMARY KEY,status TEXT NOT NULL,checked_ms INTEGER NOT NULL,detail TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS events(id INTEGER PRIMARY KEY,kind TEXT NOT NULL,observed_ms INTEGER NOT NULL,detail TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS gaps(id INTEGER PRIMARY KEY,previous_ms INTEGER NOT NULL,next_ms INTEGER NOT NULL,observed_ms INTEGER NOT NULL,status TEXT NOT NULL DEFAULT 'OPEN',UNIQUE(previous_ms,next_ms));
'''
class ShadowStore:
 def __init__(self,path):
  self.db=sqlite3.connect(str(path),timeout=15);self.db.row_factory=sqlite3.Row
  version=self.db.execute('PRAGMA user_version').fetchone()[0];app=self.db.execute('PRAGMA application_id').fetchone()[0];tables=self.db.execute("SELECT count(*) FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'").fetchone()[0]
  if (version==0 and tables) or version not in (0,1,2) or (version in (1,2) and app!=1397244983):self.db.close();raise ValueError('unrelated or unsupported SQLite database')
  self.db.execute('PRAGMA journal_mode=WAL');self.db.execute('PRAGMA synchronous=FULL')
  if version==0:self.db.executescript('BEGIN;\n'+SCHEMA+'\nPRAGMA application_id=1397244983;\nPRAGMA user_version=2;\nCOMMIT;')
  elif version==1:
   with self.db:
    self.db.execute("ALTER TABLE predictions ADD COLUMN classification TEXT NOT NULL DEFAULT 'legacy_development_test'")
    self.db.execute('PRAGMA user_version=2')
 def close(self):self.db.close()
 def record_candidate(self,p,recorded_ms,max_health_age_ms,source_max_age_ms=None):
  validate_candidate(p,recorded_ms);payload=json.dumps(p,sort_keys=True,separators=(',',':'),allow_nan=False);ident=hashlib.sha256(f"{p['symbol']}:{p['mechanism']}:{p['horizon_minutes']}:{p['entry_ms']}".encode()).hexdigest()
  with self.db:
   self.db.execute('BEGIN IMMEDIATE')
   if not self.health_ok(recorded_ms,max_health_age_ms,source_max_age_ms):raise ValueError('data health unavailable or stale')
   try:current_bound=json.loads(self.get_health('clock')['detail'])['bound_ms']
   except (KeyError,TypeError,ValueError):raise ValueError('clock uncertainty unavailable') from None
   if not isinstance(current_bound,(int,float)) or not math.isfinite(current_bound) or current_bound<0 or current_bound>p['clock_uncertainty_ms']:raise ValueError('clock uncertainty increased during preparation')
   old=self.db.execute('SELECT metadata FROM predictions WHERE id=?',(ident,)).fetchone()
   if old:
    if old['metadata']!=payload:raise ValueError('conflicting prediction for already recorded slot')
    return False
   self.db.execute('INSERT INTO predictions(id,symbol,mechanism,horizon_minutes,decision_ms,recorded_ms,entry_ms,expiry_ms,features,model_version,screen_version,screen_passed,probability_up,candidate_direction,execution_decision,no_trade_reasons,metadata,classification) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',(ident,p['symbol'],p['mechanism'],p['horizon_minutes'],p['decision_ms'],recorded_ms,p['entry_ms'],p['expiry_ms'],json.dumps(p['features'],sort_keys=True),p['model_version'],p['screen_version'],int(p['screen_passed']),p['probability_up'],p['candidate_direction'],p['execution_decision'],json.dumps(p['no_trade_reasons']),payload,p['classification']))
  return True
 def record_tick(self,t):
  if not isinstance(t.get('event_ms'),int) or not isinstance(t.get('received_ms'),int):raise ValueError('invalid tick times')
  with self.db:self.db.execute('INSERT OR IGNORE INTO ticks VALUES(?,?,?,?)',(t['event_ms'],t['received_ms'],_price(t['index_price']),_price(t['mark_price'])))
 def resolve(self,now_ms,max_lag_ms):
  if max_lag_ms<0:raise ValueError('invalid lag')
  with self.db:
   for p in self.db.execute("SELECT * FROM predictions WHERE result_status='PENDING'").fetchall():
    prices=[]
    for target in (p['entry_ms'],p['expiry_ms']):
     row=self.db.execute('SELECT * FROM ticks WHERE event_ms>=? AND event_ms<=? AND received_ms<=? ORDER BY event_ms,received_ms LIMIT 1',(target,target+max_lag_ms,target+max_lag_ms)).fetchone();prices.append(row)
    entry,exit=prices
    if entry:self.db.execute('UPDATE predictions SET entry_price=?,entry_event_ms=?,entry_received_ms=? WHERE id=?',(entry['index_price'],entry['event_ms'],entry['received_ms'],p['id']))
    if now_ms>=p['expiry_ms']+max_lag_ms:
     if exit:self.db.execute('UPDATE predictions SET exit_price=?,exit_event_ms=?,exit_received_ms=? WHERE id=?',(exit['index_price'],exit['event_ms'],exit['received_ms'],p['id']))
     if entry and exit:
      a,b=Decimal(entry['index_price']),Decimal(exit['index_price']);direction='HIGHER' if b>a else 'LOWER' if b<a else 'EQUAL'
      self.db.execute("UPDATE predictions SET exit_price=?,exit_event_ms=?,exit_received_ms=?,result_status='VERIFIED',direction_result=? WHERE id=?",(exit['index_price'],exit['event_ms'],exit['received_ms'],direction,p['id']))
     else:self.db.execute("UPDATE predictions SET result_status='MISSING' WHERE id=?",(p['id'],))
 def count_predictions(self):return self.db.execute('SELECT count(*) FROM predictions').fetchone()[0]
 def predictions(self):return [dict(x) for x in self.db.execute('SELECT * FROM predictions ORDER BY entry_ms,id')]
 def set_health(self,source,status,checked_ms,detail):
  with self.db:
   self.db.execute('BEGIN IMMEDIATE')
   previous=self.db.execute('SELECT status FROM health WHERE source=?',(source,)).fetchone()
   self.db.execute('INSERT INTO health VALUES(?,?,?,?) ON CONFLICT(source) DO UPDATE SET status=excluded.status,checked_ms=excluded.checked_ms,detail=excluded.detail',(source,status,checked_ms,json.dumps(detail,sort_keys=True)))
   old=previous['status'] if previous else None
   if old!=status:self.db.execute('INSERT INTO events(kind,observed_ms,detail) VALUES(?,?,?)',('HEALTH_TRANSITION',checked_ms,json.dumps({'source':source,'previous':old,'status':status},sort_keys=True)))
 def health_ok(self,now_ms,max_age_ms,source_max_age_ms=None):
  rows={r['source']:r for r in self.db.execute('SELECT * FROM health')}
  return all(x in rows and rows[x]['status']=='OK' and 0<=now_ms-rows[x]['checked_ms']<=(source_max_age_ms or {}).get(x,max_age_ms) for x in ('index','openmarket','clock','auth','storage'))
 def get_health(self,source):
  row=self.db.execute('SELECT * FROM health WHERE source=?',(source,)).fetchone();return dict(row) if row else None
 def unresolved_gaps(self):return [dict(x) for x in self.db.execute("SELECT * FROM gaps WHERE status='OPEN' ORDER BY previous_ms")]
 def event(self,kind,observed_ms,detail):
  with self.db:self.db.execute('INSERT INTO events(kind,observed_ms,detail) VALUES(?,?,?)',(kind,observed_ms,json.dumps(detail,sort_keys=True)))
