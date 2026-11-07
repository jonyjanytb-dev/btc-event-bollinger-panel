from datetime import datetime
from decimal import Decimal,InvalidOperation,ROUND_CEILING

def parse_utc(value):
 if not isinstance(value,str):raise ValueError('Timestamp must be a string')
 d=datetime.fromisoformat(value.replace('Z','+00:00'))
 if d.tzinfo is None or d.utcoffset() is None:raise ValueError('Timestamp must include UTC offset')
 return int(d.timestamp()*1000)

def num(value):
 if value is None:return None
 if isinstance(value,float):raise ValueError('Parse JSON with parse_float=Decimal to preserve source precision')
 try:
  if not Decimal(str(value)).is_finite():raise ValueError('Nonfinite value')
 except InvalidOperation:raise ValueError('Invalid numeric value') from None
 return str(value)

def check_identity(actual,expected):
 if expected and any(actual.get(k)!=v for k,v in expected.items()):raise ValueError('OpenMarket response identity mismatch or missing')

def normalise_points(document,received_ms,expected=None):
 data=document.get('data',{})
 if data.get('error'): raise ValueError('OpenMarket data unavailable: '+str(data['error']))
 result=[]
 for series in data.get('series',[]):
  ident=series.get('id',{});columns=series.get('columns',[]);check_identity(ident,expected)
  for vals in series.get('rows',[]):
   if len(vals)!=len(columns):raise ValueError('Invalid OpenMarket row')
   if len(set(columns))!=len(columns):raise ValueError('Duplicate OpenMarket columns')
   record=dict(zip(columns,vals)); t=parse_utc(record.pop('t',None));row={'source':'openmarket','market':ident.get('exchange'),'symbol':ident.get('rawSymbol'),'kind':ident.get('type'),'side':ident.get('side'),'interval':ident.get('interval'),'unit':series.get('unit'),'bucket_ms':t,'available_ms':received_ms,'source_verified':bool(expected) and {'exchange','rawSymbol','interval','type'}<=expected.keys()}
   steps={'MINUTE':60000,'FIVE_MINUTES':300000,'FIFTEEN_MINUTES':900000,'THIRTY_MINUTES':1800000,'HOUR':3600000,'FOUR_HOURS':14400000}
   if row['interval'] not in steps or t%steps[row['interval']]:raise ValueError('Misaligned OpenMarket bucket')
   row.update({k:num(v) for k,v in record.items()})
   kind=row['kind']
   if kind in ('TRADE_SIDE_AGNOSTIC_AGG','TRADE_AGG','OPEN_INTEREST_AGG'):
    if any(row.get(k) is None for k in ('open','high','low','close')):raise ValueError('Missing OHLC value')
    op,high,low,cl=(Decimal(row[k]) for k in ('open','high','low','close'))
    if low<0 or (kind!='OPEN_INTEREST_AGG' and low<=0) or low>min(op,cl) or high<max(op,cl) or low>high:raise ValueError('Invalid OHLC value')
   for field in ('volume','liquidations'):
    if row.get(field) is not None and Decimal(row[field])<0:raise ValueError('Negative volume')
   required={'TRADE_SIDE_AGNOSTIC_AGG':'volume','TRADE_AGG':'volume','LIQUIDATION_AGG':'liquidations','FUNDING_RATE_AGG':'rateClose'}.get(kind)
   if required and row.get(required) is None:raise ValueError('Missing required aggregate')
   result.append(row)
 return result

def normalise_metrics(document,received_ms,expected=None):
 data=document.get('data',{})
 if data.get('error'):raise ValueError('OpenMarket data unavailable: '+str(data['error']))
 asof_ms=parse_utc(data.get('asOf'));check_identity(data.get('selector',{}),expected);result=[]
 for metric in data.get('values',[]):
  if metric.get('ok') is not True:continue
  if not isinstance(metric.get('metric'),str):raise ValueError('Missing metric name')
  kind='price_delta_abs' if metric['metric']=='delta_abs' else metric['metric']
  age=num(metric.get('data_age_seconds'))
  if age is not None and Decimal(age)<0:raise ValueError('Negative metric data age')
  event_ms=asof_ms-int((Decimal(age)*1000).to_integral_value(rounding=ROUND_CEILING)) if age is not None else None
  if event_ms is not None and event_ms<0:raise ValueError('Invalid metric observation time')
  result.append({'kind':kind,'value':num(metric.get('value')),'unit':metric.get('unit'),'market':data.get('selector',{}).get('exchange','unknown'),'symbol':data.get('selector',{}).get('symbol','unknown'),'event_ms':event_ms,'source_asof_ms':asof_ms,'data_age_seconds':age,'received_ms':received_ms,'available_ms':received_ms,'source_verified':bool(expected) and {'exchange','symbol','interval'}<=expected.keys()})
 return result

def volume_delta(buy,sell):
 if buy is None or sell is None: return None
 return str(Decimal(str(buy))-Decimal(str(sell)))

def normalise_books(document,received_ms,symbol,market,expected=None):
 result=[]
 for series in document.get('data',{}).get('series',[]):
  check_identity(series.get('id',{}),expected)
  for item in series.get('points',[]):
   p=item['Point'];stamp=p['timestamp'];ms=int(stamp['s'])*1000+int(stamp.get('nanos',0))//1000000
   def levels(values):
    if len(values)%2:raise ValueError('Invalid order-book price/quantity pairs')
    rows=[[num(values[i]),num(values[i+1])] for i in range(0,len(values),2)]
    if any(p is None or q is None or Decimal(p)<=0 or Decimal(q)<0 for p,q in rows):raise ValueError('Invalid order-book level')
    return rows
   result.append({'symbol':symbol,'market':market,'event_ms':ms,'received_ms':received_ms,'available_ms':received_ms,'bids':levels(p['bids']),'asks':levels(p['asks'])})
 return result
