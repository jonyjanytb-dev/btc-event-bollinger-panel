"""Health gates and durable gap handling for a read-only research process."""
import json,math
from .store import _price

def profiles_for_entry(entry_ms):
 if entry_ms%60000:raise ValueError('entry must be minute-aligned')
 return [(mechanism,horizon) for horizon in (30,60) for mechanism in ('FIXED','ROLLING') if mechanism=='ROLLING' or entry_ms%(horizon*60000)==0]

class ShadowCoordinator:
 def __init__(self,store,gap_threshold_ms):
  if gap_threshold_ms<=0:raise ValueError('invalid gap threshold')
  self.store=store;self.gap_threshold_ms=gap_threshold_ms
 def on_tick(self,t):
  if not isinstance(t.get('event_ms'),int) or not isinstance(t.get('received_ms'),int):raise ValueError('invalid tick times')
  index=_price(t['index_price']);mark=_price(t['mark_price']);db=self.store.db
  with db:
   db.execute('BEGIN IMMEDIATE');last=db.execute('SELECT max(event_ms) FROM ticks').fetchone()[0]
   insert=db.execute('INSERT OR IGNORE INTO ticks VALUES(?,?,?,?)',(t['event_ms'],t['received_ms'],index,mark))
   if insert.rowcount and last is not None and t['event_ms']-last>self.gap_threshold_ms:
    db.execute('INSERT OR IGNORE INTO gaps(previous_ms,next_ms,observed_ms) VALUES(?,?,?)',(last,t['event_ms'],t['received_ms']))
    db.execute('INSERT INTO events(kind,observed_ms,detail) VALUES(?,?,?)',('INDEX_GAP',t['received_ms'],json.dumps({'previous_ms':last,'next_ms':t['event_ms']})))
   if insert.rowcount and last is not None and t['event_ms']<last:
    db.execute('INSERT INTO events(kind,observed_ms,detail) VALUES(?,?,?)',('OUT_OF_ORDER',t['received_ms'],json.dumps({'previous_ms':last,'event_ms':t['event_ms']})))
   if insert.rowcount:
    gaps=db.execute("SELECT count(*) FROM gaps WHERE status='OPEN'").fetchone()[0]
    status='GAP' if gaps else 'OUT_OF_ORDER' if last is not None and t['event_ms']<last else 'OK'
    previous=db.execute("SELECT status FROM health WHERE source='index'").fetchone();old=previous['status'] if previous else None
    db.execute('INSERT INTO health VALUES(?,?,?,?) ON CONFLICT(source) DO UPDATE SET status=excluded.status,checked_ms=excluded.checked_ms,detail=excluded.detail',('index',status,t['received_ms'],json.dumps({'event_ms':t['event_ms']})))
    if old!=status:db.execute('INSERT INTO events(kind,observed_ms,detail) VALUES(?,?,?)',('HEALTH_TRANSITION',t['received_ms'],json.dumps({'source':'index','previous':old,'status':status},sort_keys=True)))
 def confirm_backfill(self,gap_id,bars):
  db=self.store.db;gap=db.execute("SELECT * FROM gaps WHERE id=? AND status='OPEN'",(gap_id,)).fetchone()
  if not gap:raise ValueError('gap not open')
  first=(gap['previous_ms']//60000)*60000;last=(gap['next_ms']//60000)*60000
  bytime={r['open_ms']:r for r in bars}
  for t in range(first,last+60000,60000):
   b=bytime.get(t)
   if not b or b['close_ms']!=t+59999 or b['available_ms']<=b['close_ms']:raise ValueError('backfill not complete and closed')
  with db:
   db.execute("UPDATE gaps SET status='BACKFILLED_BARS_ONLY' WHERE id=?",(gap_id,))
   db.execute('INSERT INTO events(kind,observed_ms,detail) VALUES(?,?,?)',('GAP_BACKFILLED_BARS_ONLY',max(b['available_ms'] for b in bars),json.dumps({'gap_id':gap_id,'seconds_remain_missing':True})))
 def update_clock(self,now_ms,offset_ms,dispersion_ms,max_error_ms):
  if not all(math.isfinite(x) for x in (offset_ms,dispersion_ms,max_error_ms)) or dispersion_ms<0 or max_error_ms<=0:raise ValueError('invalid clock measurement')
  bound=abs(offset_ms)+dispersion_ms;self.store.set_health('clock','OK' if bound<=max_error_ms else 'UNHEALTHY',now_ms,{'offset_ms':offset_ms,'dispersion_ms':dispersion_ms,'bound_ms':bound})
 def safe_stop(self,now_ms):self.store.event('SAFE_STOP',now_ms,{'pending_forecasts_remain_pending':True})
