"""User-configured boundary simulations; no network calls or exchange execution."""
import json,sqlite3,time
from decimal import Decimal,InvalidOperation
from pathlib import Path

class PaperSchedule:
 def __init__(self,path):
  self.db=sqlite3.connect(str(path),timeout=10);self.db.row_factory=sqlite3.Row
  self.db.execute('pragma journal_mode=WAL')
  self.db.executescript('''create table if not exists configs(horizon integer primary key,enabled integer not null,direction text not null,stake text not null,updated_ms integer not null);create table if not exists orders(id integer primary key,horizon integer not null,boundary_ms integer not null,entry_ms integer not null,expiry_ms integer not null,direction text not null,stake text not null,created_ms integer not null,execution text not null,status text not null,entry_price text,exit_price text,outcome text,unique(horizon,boundary_ms));''');self.db.execute('create table if not exists runtime(id integer primary key check(id=1),heartbeat_ms integer not null)')
  fields={r['name'] for r in self.db.execute('pragma table_info(orders)')}
  for name in ('previous_close','next_close','candle_outcome'):
   if name not in fields:self.db.execute('alter table orders add column '+name+' text')
  self.db.commit()
 def heartbeat(self,now_ms):
  with self.db:self.db.execute('insert into runtime values(1,?) on conflict(id) do update set heartbeat_ms=excluded.heartbeat_ms',(now_ms,))
 def close(self):self.db.close()
 def configure(self,horizon,enabled,direction,stake,updated_ms):
  try:value=Decimal(str(stake))
  except (InvalidOperation,ValueError):raise ValueError('invalid amount') from None
  if horizon not in (30,60) or type(enabled) is not bool or direction not in ('HIGHER','LOWER') or not value.is_finite() or value<=0 or not isinstance(updated_ms,int):raise ValueError('invalid configuration')
  with self.db:self.db.execute('insert into configs values(?,?,?,?,?) on conflict(horizon) do update set enabled=excluded.enabled,direction=excluded.direction,stake=excluded.stake,updated_ms=excluded.updated_ms',(horizon,int(enabled),direction,str(value),updated_ms))
 def configurations(self):return [dict(x) for x in self.db.execute('select * from configs order by horizon')]
 def due(self,now_ms):
  due=[]
  for c in self.configurations():
   step=c['horizon']*60000;boundary=(now_ms//step)*step
   if c['enabled'] and c['updated_ms']<boundary and 1000<=now_ms-boundary<=1900:due.append({'horizon':c['horizon'],'direction':c['direction'],'stake':c['stake'],'boundary_ms':boundary})
  return due
 def open(self,slot,now_ms):
  h=slot['horizon'];boundary=slot['boundary_ms'];entry=boundary+2000
  if h not in (30,60) or boundary%(h*60000) or not boundary+1000<=now_ms<=boundary+1900:raise ValueError('late or invalid slot')
  with self.db:
   self.db.execute('begin immediate')
   row=self.db.execute('select * from configs where horizon=?',(h,)).fetchone()
   if not row or not row['enabled'] or row['updated_ms']>=boundary or row['direction']!=slot['direction'] or row['stake']!=slot['stake']:raise ValueError('configuration changed')
   cur=self.db.execute('insert or ignore into orders(horizon,boundary_ms,entry_ms,expiry_ms,direction,stake,created_ms,execution,status) values(?,?,?,?,?,?,?,?,?)',(h,boundary,entry,entry+h*60000,slot['direction'],slot['stake'],now_ms,'PAPER_ONLY','PENDING'))
   return cur.rowcount==1
 def orders(self):return [dict(x) for x in self.db.execute('select * from orders order by boundary_ms desc,horizon limit 200')]
 def resolve(self,tick_path,now_ms,lag_ms=5000):
  tick=sqlite3.connect(Path(tick_path).resolve().as_uri()+'?mode=ro',uri=True,timeout=5)
  try:
   for p in self.db.execute("select * from orders where status='PENDING'").fetchall():
    if now_ms<p['entry_ms']+lag_ms:continue
    values=[]
    for t in (p['entry_ms'],p['expiry_ms']):values.append(tick.execute('select index_price from ticks where event_ms>=? and event_ms<=? and received_ms<=? order by event_ms,received_ms limit 1',(t,t+lag_ms,t+lag_ms)).fetchone())
    entry,exit_=values;entry=entry[0] if entry else None;exit_=exit_[0] if exit_ else None
    status='PENDING';outcome=None
    if now_ms>=p['expiry_ms']+lag_ms:
     status='VERIFIED' if entry and exit_ else 'MISSING'
     if status=='VERIFIED':
      a,b=Decimal(entry),Decimal(exit_);actual='HIGHER' if b>a else 'LOWER' if b<a else 'EQUAL';outcome='UNDETERMINED' if actual=='EQUAL' else 'WIN' if actual==p['direction'] else 'LOSS'
    def close_price(target):
     row=tick.execute('select index_price from ticks where event_ms>=? and event_ms<? and received_ms<=? order by event_ms desc,received_ms asc limit 1',(target-lag_ms,target,target+lag_ms)).fetchone()
     return row[0] if row else None
    previous=close_price(p['boundary_ms']);next_close=close_price(p['boundary_ms']+p['horizon']*60000) if status!='PENDING' else None
    candle_outcome=None
    if previous and next_close:
     first,last=Decimal(previous),Decimal(next_close);direction='HIGHER' if last>first else 'LOWER' if last<first else 'EQUAL'
     candle_outcome='UNDETERMINED' if direction=='EQUAL' else 'WIN' if direction==p['direction'] else 'LOSS'
    with self.db:self.db.execute('update orders set entry_price=?,exit_price=?,status=?,outcome=?,previous_close=?,next_close=?,candle_outcome=? where id=?',(entry,exit_,status,outcome,previous,next_close,candle_outcome,p['id']))
  finally:tick.close()

def health_ok(path,now_ms):
 try:
  db=sqlite3.connect(Path(path).resolve().as_uri()+'?mode=ro',uri=True,timeout=5);rows={x[0]:x for x in db.execute("select source,status,checked_ms,detail from health where source in ('index','clock')")};db.close()
  return all(name in rows and rows[name][1]=='OK' and 0<=now_ms-rows[name][2]<=age for name,age in [('index',5000),('clock',180000)]) and 0<=float(json.loads(rows['clock'][3])['bound_ms'])<=1000
 except (OSError,sqlite3.Error,KeyError,TypeError,ValueError):return False

def main():
 import argparse,signal,os
 p=argparse.ArgumentParser(description='只读 30M/1H 定时模拟；不会发送交易请求');p.add_argument('--database',required=True);p.add_argument('--shadow-db',required=True);args=p.parse_args();os.umask(0o077);stop=False
 def halt(*_):
  nonlocal stop;stop=True
 signal.signal(signal.SIGINT,halt);signal.signal(signal.SIGTERM,halt);s=PaperSchedule(args.database);last_resolve=0
 try:
  while not stop:
   now=time.time_ns()//1000000
   try:
    if now-last_resolve>=3000:s.resolve(args.shadow_db,now-1000);s.heartbeat(now);last_resolve=now
    due=s.due(now)
    if due and health_ok(args.shadow_db,now):
     for slot in due:s.open(slot,time.time_ns()//1000000)
   except (ValueError,sqlite3.Error,OSError):print('SCHEDULER_DATA_UNAVAILABLE',flush=True)
   time.sleep(.1)
 finally:s.close()
if __name__=='__main__':main()
