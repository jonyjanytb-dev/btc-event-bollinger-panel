"""Read-only health presentation with explicit stale/missing status."""
import fcntl,os
from pathlib import Path

def health_overview(rows,now_ms,max_age_ms,index_max_age_ms):
 byname={r['source']:dict(r) for r in rows};out={}
 for name in ('index','openmarket','clock','auth','storage'):
  row=byname.get(name)
  if row is None:out[name]={'effective_status':'MISSING','age_ms':None};continue
  age=now_ms-row['checked_ms'];limit=index_max_age_ms if name=='index' else max_age_ms
  row['age_ms']=age;row['effective_status']='STALE' if age<0 or age>limit else row['status'];out[name]=row
 return out

def runner_active(directory):
 p=Path(directory)/'runner.lock'
 if not p.exists():return False
 fd=os.open(str(p),os.O_RDONLY|getattr(os,'O_NOFOLLOW',0))
 try:
  try:fcntl.flock(fd,fcntl.LOCK_EX|fcntl.LOCK_NB)
  except BlockingIOError:return True
  fcntl.flock(fd,fcntl.LOCK_UN);return False
 finally:os.close(fd)
