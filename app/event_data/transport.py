"""HTTP/MCP transports. Only public Binance GET and OpenMarket read tools are allowed."""
import json
import time
from decimal import Decimal
from urllib.parse import urlencode, urlsplit
from urllib.error import URLError, HTTPError
import requests

class TransientHTTP(OSError): pass


def decode_mcp(content_type,body):
 if 'text/event-stream' in content_type:
  lines=[line[5:].strip() for line in body.splitlines() if line.startswith('data:')]
  if not lines:raise ValueError('Empty MCP event stream')
  body=lines[-1]
 return json.loads(body,parse_float=Decimal)

def default_request(url,headers,body,timeout):
 response=requests.request('POST' if body is not None else 'GET',url,headers=headers,data=body,timeout=timeout,allow_redirects=False)
 return response.status_code,dict(response.headers),response.text

class ReadOnlyMCP:
 def __init__(self,url,token,request=default_request,timeout=15,retries=2):
  u=urlsplit(url)
  if u.scheme!='https' or u.netloc!='mcp.openmarket.xyz' or u.path not in ('','/') or u.query or u.fragment:raise ValueError('Invalid MCP endpoint')
  if not token:raise ValueError('Missing OAuth token')
  self.url=url;self.request=request;self.timeout=timeout;self.retries=retries;self.session=None;self.protocol=None;self.last_response_body=None;self._token=token
  self.headers={'Authorization':'Bearer '+token,'Content-Type':'application/json','Accept':'application/json, text/event-stream'}
 def call(self,method,params,ident=1):
  if method!='initialize' and not (method=='tools/call' and isinstance(params,dict) and params.get('name') in ('points','metric_get')):raise ValueError('MCP method not allowed')
  body=json.dumps({'jsonrpc':'2.0','id':ident,'method':method,'params':params},separators=(',',':')).encode()
  for attempt in range(self.retries+1):
   try:
    status,headers,text=self.request(self.url,self.headers,body,self.timeout)
    if status==429 or status>=500:raise TransientHTTP(f'MCP HTTP {status}')
    if not 200<=status<300:raise ValueError(f'MCP HTTP {status}')
    if self._token in text:raise ValueError('Sensitive value appeared in response')
    self.last_response_body=text
    self.session=headers.get('Mcp-Session-Id') or headers.get('mcp-session-id') or self.session
    if self.session:self.headers['Mcp-Session-Id']=self.session
    parsed=decode_mcp(headers.get('Content-Type',headers.get('content-type','')),text)
    if parsed.get('error'):raise ValueError('MCP error '+str(parsed['error'].get('code')))
    return parsed['result']
   except (URLError,TimeoutError,OSError,requests.RequestException):
    if attempt==self.retries:raise
    time.sleep(min(2**attempt,8))
 def initialise(self):
  result=self.call('initialize',{'protocolVersion':'2025-03-26','capabilities':{},'clientInfo':{'name':'btc-event-data','version':'0.1'}})
  self.protocol=result['protocolVersion'];self.headers['MCP-Protocol-Version']=self.protocol
  notification=json.dumps({'jsonrpc':'2.0','method':'notifications/initialized'}).encode()
  for attempt in range(self.retries+1):
   try:
    status,_,_=self.request(self.url,self.headers,notification,self.timeout)
    if status==429 or status>=500:raise TransientHTTP(f'MCP notification HTTP {status}')
    if not 200<=status<300:raise ValueError(f'MCP notification HTTP {status}')
    break
   except (URLError,TimeoutError,OSError,requests.RequestException):
    if attempt==self.retries:raise
    time.sleep(min(2**attempt,8))
 def tool(self,name,arguments):
  if name not in ('points','metric_get'):raise ValueError('Tool not allowed')
  result=self.call('tools/call',{'name':name,'arguments':arguments})
  if result.get('isError'):raise ValueError('OpenMarket tool returned an error')
  output=result.get('structuredContent')
  if not isinstance(output,dict) or not isinstance(output.get('data'),dict):raise ValueError('Invalid OpenMarket result')
  if output['data'].get('error'):raise ValueError('OpenMarket: '+str(output['data']['error']))
  return output

def binance_get(base,path,params,timeout=15,retries=2,request=default_request):
 u=urlsplit(base)
 if u.scheme!='https' or u.netloc!='fapi.binance.com' or u.path not in ('','/') or path not in ('/fapi/v1/indexPriceKlines','/fapi/v1/premiumIndex'):raise ValueError('Only public Binance index endpoints allowed')
 url=base.rstrip('/')+path+'?'+urlencode(params)
 for attempt in range(retries+1):
  try:
   status,_,text=request(url,{'Accept':'application/json'},None,timeout)
   if status==429 or status>=500:raise TransientHTTP(f'Binance HTTP {status}')
   if not 200<=status<300:raise ValueError(f'Binance HTTP {status}')
   return json.loads(text,parse_float=Decimal)
  except (URLError,TimeoutError,OSError,requests.RequestException):
   if attempt==retries:raise
   time.sleep(min(2**attempt,8))
