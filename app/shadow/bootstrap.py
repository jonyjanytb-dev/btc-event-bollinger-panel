"""Interactive PKCE bootstrap for a separate read-only OpenMarket OAuth client."""
import argparse,base64,hashlib,json,os,secrets,time,webbrowser
from http.server import BaseHTTPRequestHandler,HTTPServer
from pathlib import Path
from urllib.parse import parse_qs,urlencode,urlsplit
import requests
from .oauth import ISSUER,SERVER,TOKEN_ENDPOINT,_validate

AUTH_ENDPOINT='https://openmarket.xyz/api/v1/oauth/authorize'
REGISTER_ENDPOINT='https://openmarket.xyz/api/v1/oauth/register'

def _redirect(uri):
 u=urlsplit(uri)
 if u.scheme!='http' or u.hostname!='127.0.0.1' or not u.port or u.path!='/callback' or u.query or u.fragment or u.username or u.password:raise ValueError('redirect must be local loopback callback')
 return uri

def register_client(redirect_uri,timeout=15):
 _redirect(redirect_uri)
 try:
  r=requests.post(REGISTER_ENDPOINT,json={'client_name':'BTC Event Shadow read-only','redirect_uris':[redirect_uri],'grant_types':['authorization_code','refresh_token'],'response_types':['code'],'token_endpoint_auth_method':'none','scope':'mcp:read'},timeout=timeout,allow_redirects=False)
 except requests.RequestException:raise RuntimeError('OAuth client registration unavailable') from None
 if r.status_code not in (200,201):raise ValueError('OAuth client registration rejected')
 data=r.json();client_id=data.get('client_id')
 if not isinstance(client_id,str) or not client_id or len(client_id)>512:raise ValueError('invalid OAuth client registration')
 return client_id

def authorization_url(client_id,redirect_uri,state,challenge):
 _redirect(redirect_uri)
 return AUTH_ENDPOINT+'?'+urlencode({'response_type':'code','client_id':client_id,'redirect_uri':redirect_uri,'state':state,'code_challenge':challenge,'code_challenge_method':'S256','scope':'mcp:read','resource':SERVER})

def exchange_code(client_id,code,verifier,redirect_uri,now_ms=None,timeout=15):
 _redirect(redirect_uri);now_ms=int(time.time()*1000) if now_ms is None else now_ms
 try:r=requests.post(TOKEN_ENDPOINT,data={'grant_type':'authorization_code','client_id':client_id,'code':code,'code_verifier':verifier,'redirect_uri':redirect_uri,'resource':SERVER},timeout=timeout,allow_redirects=False)
 except requests.RequestException:raise RuntimeError('OAuth exchange unavailable; start a new authorization') from None
 if r.status_code!=200:raise ValueError('OAuth exchange rejected')
 d=r.json();scope=d.get('scope','mcp:read')
 if not isinstance(scope,str) or set(scope.split())!={'mcp:read'} or str(d.get('token_type','')).lower()!='bearer' or not isinstance(d.get('expires_in'),(int,float)) or d['expires_in']<=0 or not d.get('refresh_token'):raise ValueError('OAuth exchange response rejected')
 e={'issuer':ISSUER,'server_url':SERVER,'client_id':client_id,'scopes':['mcp:read'],'access_token':d.get('access_token'),'refresh_token':d['refresh_token'],'expires_at':int(now_ms+d['expires_in']*1000)};_validate(e);return e

def write_credentials(path,credentials):
 _validate(credentials);p=Path(path).expanduser();p.parent.mkdir(mode=0o700,parents=True,exist_ok=True)
 flags=os.O_WRONLY|os.O_CREAT|os.O_EXCL|getattr(os,'O_NOFOLLOW',0);fd=os.open(str(p),flags,0o600)
 try:
  with os.fdopen(fd,'w') as f:json.dump(credentials,f,separators=(',',':'));f.flush();os.fsync(f.fileno())
 except BaseException:
  try:p.unlink()
  except OSError as cleanup_error:raise RuntimeError('Could not remove incomplete credentials file') from cleanup_error
  raise

def bootstrap(path,timeout=600):
 state=secrets.token_urlsafe(32);verifier=secrets.token_urlsafe(64);challenge=base64.urlsafe_b64encode(hashlib.sha256(verifier.encode()).digest()).rstrip(b'=').decode();result={}
 class Handler(BaseHTTPRequestHandler):
  def log_message(self,*args):
   # Request paths contain the authorization code; never print them.
   return
  def do_GET(self):
   q=urlsplit(self.path);params=parse_qs(q.query)
   good=q.path=='/callback' and secrets.compare_digest(params.get('state',[''])[0],state) and bool(params.get('code',[''])[0])
   if good:result['code']=params['code'][0]
   else:result['error']=True
   self.send_response(200 if good else 400);self.send_header('Content-Type','text/plain; charset=utf-8');self.end_headers();self.wfile.write(('Authorization received. You may close this window.' if good else 'Authorization failed.').encode())
 server=HTTPServer(('127.0.0.1',0),Handler);server.timeout=1;redirect=f'http://127.0.0.1:{server.server_port}/callback'
 try:
  client=register_client(redirect);url=authorization_url(client,redirect,state,challenge)
  if not webbrowser.open(url):raise RuntimeError('Could not open authorization page')
  print('Read-only authorization page opened; approve in browser. Do not share codes or tokens.',flush=True)
  deadline=time.monotonic()+timeout
  while not result and time.monotonic()<deadline:server.handle_request()
  if not result.get('code'):raise ValueError('OAuth authorization not completed')
  credentials=exchange_code(client,result['code'],verifier,redirect);write_credentials(path,credentials)
  print('Dedicated read-only credentials securely saved.',flush=True)
 finally:server.server_close()

def main():
 p=argparse.ArgumentParser();p.add_argument('--credentials',required=True,type=Path);p.add_argument('--timeout-seconds',type=int,default=600);a=p.parse_args();bootstrap(a.credentials,a.timeout_seconds)
if __name__=='__main__':main()
