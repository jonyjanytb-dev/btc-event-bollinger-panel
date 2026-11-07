"""Read-only shared credentials or isolated, safely rotating OAuth credentials."""
import fcntl,json,os,stat,tempfile,time
from pathlib import Path
import requests

ISSUER='https://mcp.openmarket.xyz'
SERVER=ISSUER+'/'
METADATA=ISSUER+'/.well-known/oauth-authorization-server'
TOKEN_ENDPOINT='https://openmarket.xyz/api/v1/oauth/token'

def _validate(e):
 if e.get('issuer')!=ISSUER or e.get('server_url')!=SERVER or set(e.get('scopes',[]))!={'mcp:read'}:raise ValueError('OAuth issuer, resource or scope invalid')
 if not isinstance(e.get('access_token'),str) or not e['access_token'] or not isinstance(e.get('expires_at'),(int,float)):raise ValueError('OAuth credentials incomplete')

class SharedCodexAccess:
 """Read only; deliberately never rotates the shared Codex refresh token."""
 def __init__(self,path):self.path=Path(path).expanduser()
 def access(self,now_ms=None,refresh_before_ms=0):
  now_ms=int(time.time()*1000) if now_ms is None else now_ms
  data=json.loads(self.path.read_text());entries=[x for x in data.values() if x.get('server_name')=='openmarket' and x.get('server_url')==SERVER]
  if len(entries)!=1:raise ValueError('shared OAuth entry unavailable')
  e=entries[0];_validate(e)
  if e['expires_at']<=now_ms+refresh_before_ms:raise ValueError('shared OAuth expired or near expiry; use dedicated authorization')
  return e['access_token']

class DedicatedOAuth:
 """Own credential file only. Ambiguous refresh failures require reauthorization."""
 def __init__(self,path,timeout=15):
  original=Path(path).expanduser()
  if original.is_symlink():raise ValueError('credential symlinks not allowed')
  self.path=original.resolve();shared=Path('~/.codex/.credentials.json').expanduser().resolve()
  if self.path==shared or (self.path.exists() and shared.exists() and os.path.samefile(self.path,shared)):raise ValueError('refusing to rotate shared credentials')
  self.timeout=timeout;self.last_access_refreshed=False
 def _read(self):
  if self.path.is_symlink():raise ValueError('credential symlinks not allowed')
  fd=os.open(str(self.path),os.O_RDONLY|getattr(os,'O_NOFOLLOW',0))
  with os.fdopen(fd) as f:
   st=os.fstat(f.fileno())
   if not stat.S_ISREG(st.st_mode) or st.st_uid!=os.getuid() or st.st_mode&0o077:raise ValueError('credential file must be private and owned by current user')
   e=json.load(f)
  _validate(e);return e
 def access(self,now_ms=None,refresh_before_ms=60000):
  self.last_access_refreshed=False
  now_ms=int(time.time()*1000) if now_ms is None else now_ms
  lock_path=str(self.path)+'.lock';fd=os.open(lock_path,os.O_CREAT|os.O_RDWR|getattr(os,'O_NOFOLLOW',0),0o600)
  with os.fdopen(fd,'r+') as lock:
   fcntl.flock(lock,fcntl.LOCK_EX);pending=Path(str(self.path)+'.refresh-pending')
   if pending.exists():raise ValueError('previous refresh outcome uncertain; reauthorization required')
   e=self._read()
   if e['expires_at']>now_ms+refresh_before_ms:return e['access_token']
   if not e.get('refresh_token') or not e.get('client_id'):raise ValueError('dedicated OAuth refresh unavailable')
   try:meta=requests.get(METADATA,timeout=self.timeout,allow_redirects=False)
   except requests.RequestException:raise ValueError('OAuth discovery unavailable; refresh not attempted') from None
   if meta.status_code!=200:raise ValueError('OAuth discovery failed')
   m=meta.json()
   if m.get('issuer')!=ISSUER or m.get('token_endpoint')!=TOKEN_ENDPOINT or 'refresh_token' not in m.get('grant_types_supported',[]):raise ValueError('OAuth metadata rejected')
   pending_fd=os.open(str(pending),os.O_WRONLY|os.O_CREAT|os.O_EXCL|getattr(os,'O_NOFOLLOW',0),0o600)
   with os.fdopen(pending_fd,'w') as marker:marker.write('refresh in progress\n');marker.flush();os.fsync(marker.fileno())
   dir_fd=os.open(str(self.path.parent),os.O_RDONLY)
   try:os.fsync(dir_fd)
   finally:os.close(dir_fd)
   try:
    response=requests.post(TOKEN_ENDPOINT,data={'grant_type':'refresh_token','refresh_token':e['refresh_token'],'client_id':e['client_id'],'scope':'mcp:read'},timeout=self.timeout,allow_redirects=False)
    if response.status_code!=200:raise ValueError('OAuth refresh rejected; reauthorization required')
    data=response.json()
   except requests.RequestException:raise RuntimeError('OAuth refresh outcome unknown; reauthorization may be required') from None
   scopes=data.get('scope','mcp:read')
   if set(scopes.split())!={'mcp:read'} or str(data.get('token_type','')).lower()!='bearer' or not isinstance(data.get('access_token'),str) or not data['access_token'] or not isinstance(data.get('expires_in'),(int,float)) or data['expires_in']<=0:raise ValueError('OAuth refresh response rejected')
   e.update(access_token=data['access_token'],refresh_token=data.get('refresh_token') or e['refresh_token'],expires_at=int(now_ms+data['expires_in']*1000))
   temp_fd,temp_path=tempfile.mkstemp(prefix='.oauth-',dir=str(self.path.parent))
   try:
    with os.fdopen(temp_fd,'w') as temp:
     os.fchmod(temp.fileno(),0o600);json.dump(e,temp,separators=(',',':'));temp.flush();os.fsync(temp.fileno())
    os.replace(temp_path,self.path)
    dir_fd=os.open(str(self.path.parent),os.O_RDONLY)
    try:os.fsync(dir_fd)
    finally:os.close(dir_fd)
    os.unlink(pending)
    dir_fd=os.open(str(self.path.parent),os.O_RDONLY)
    try:os.fsync(dir_fd)
    finally:os.close(dir_fd)
   finally:
    if os.path.exists(temp_path):os.unlink(temp_path)
   self.last_access_refreshed=True
   return e['access_token']
