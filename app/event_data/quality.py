def closed(close_ms,as_of_ms): return close_ms < as_of_ms

def gaps(opens,step):
 ordered=sorted(set(opens));return [t for a,b in zip(ordered,ordered[1:]) for t in range(a+step,b,step)]

def aggregate_bars(bars,window_ms,decision_ms):
 from decimal import Decimal
 if window_ms%60000 or window_ms<=0:raise ValueError('Window must be a multiple of one minute')
 # Resolve revisions as they were available at the decision time.
 by_time={}
 for b in bars:
  if b['available_ms']<=decision_ms and closed(b['close_ms'],decision_ms) and b['close_ms']<b['available_ms']:
   old=by_time.get(b['open_ms'])
   if old is None or b['available_ms']>old['available_ms']:by_time[b['open_ms']]=b
 result=[]
 for start in sorted({t//window_ms*window_ms for t in by_time}):
  keys=list(range(start,start+window_ms,60000))
  if start+window_ms>decision_ms or any(t not in by_time for t in keys):continue
  group=[by_time[t] for t in keys]
  result.append({'open_ms':start,'close_ms':start+window_ms-1,'available_ms':max(x['available_ms'] for x in group),'open':group[0]['open'],'high':str(max(Decimal(x['high']) for x in group)),'low':str(min(Decimal(x['low']) for x in group)),'close':group[-1]['close'],'volume':None if any(x['volume'] is None for x in group) else str(sum(Decimal(x['volume']) for x in group))})
 return result
