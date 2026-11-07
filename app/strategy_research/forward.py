"""Append-only local paper-observation journal; no order APIs."""
import json,math,os,time
from decimal import Decimal,InvalidOperation
from pathlib import Path


def _append(path,item):
 fd=os.open(str(path),os.O_WRONLY|os.O_CREAT|os.O_APPEND,0o600)
 with os.fdopen(fd,'a') as f:
  f.write(json.dumps(item,ensure_ascii=False,allow_nan=False,separators=(',',':'))+'\n');f.flush();os.fsync(f.fileno())

def _events(path):
 p=Path(path)
 if not p.exists():return []
 with p.open() as f:return [json.loads(line) for line in f if line.strip()]

def formal_predictions(records,classifications=None):
 classifications=classifications or {}
 return [r for r in records if r.get('type')=='prediction' and r.get('classification')=='protocol_forward' and r.get('strategy_mechanism') in ('FIXED','ROLLING') and classifications.get(r.get('id'))!='legacy_forward_baseline']

def record_prediction(path,prediction,now=lambda:time.time_ns()//1000000):
 required=('id','model_version','decision_ms','candidate_entry_ms','expiry_ms','probability_up','candidate_direction','decision','price_source','feature_available_ms','feature_end_ms','payout_source','classification','strategy_mechanism','training_spacing_minutes','horizon_minutes','windows','screen_passed','screen_metrics','no_trade_reasons','clock_uncertainty_bound_ms')
 if any(x not in prediction for x in required):raise ValueError('missing prediction field')
 if not prediction['id'] or not prediction['model_version'] or not prediction['price_source']:raise ValueError('missing provenance')
 if not (prediction['feature_available_ms']<=prediction['decision_ms']<prediction['candidate_entry_ms']<prediction['expiry_ms']):raise ValueError('invalid prediction time')
 if prediction['classification']!='protocol_forward' or prediction['strategy_mechanism'] not in ('FIXED','ROLLING') or prediction['horizon_minutes'] not in (30,60):raise ValueError('invalid protocol identity')
 spacing=prediction['horizon_minutes'] if prediction['strategy_mechanism']=='FIXED' else 1
 if prediction['training_spacing_minutes']!=spacing:raise ValueError('training spacing mismatch')
 if prediction['candidate_entry_ms']%(spacing*60000) or prediction['expiry_ms']!=prediction['candidate_entry_ms']+prediction['horizon_minutes']*60000:raise ValueError('invalid entry boundary')
 if prediction['feature_end_ms']!=prediction['candidate_entry_ms']-60001 or prediction['feature_end_ms']>=prediction['decision_ms']:raise ValueError('feature candle does not match research boundary')
 windows=prediction['windows'];names=('train','calibration','screen');durations=(30,4,3);day=86400000
 if not isinstance(windows,dict) or any(not isinstance(windows.get(k),dict) for k in names):raise ValueError('missing protocol windows')
 for key,duration in zip(names,durations):
  w=windows[key]
  if any(x not in w for x in ('start_ms','end_ms','last_exit_ms','samples')) or w['end_ms']-w['start_ms']!=duration*day or w['last_exit_ms']>=w['end_ms'] or w['samples']<=0:raise ValueError('invalid historical window')
 if windows['train']['end_ms']!=windows['calibration']['start_ms'] or windows['calibration']['end_ms']!=windows['screen']['start_ms'] or windows['screen']['end_ms']>windows.get('training_cutoff_ms',-1) or windows.get('training_cutoff_ms',-1)>prediction['decision_ms']:raise ValueError('historical windows cross decision')
 if not windows['screen']['end_ms']<=prediction['candidate_entry_ms']<windows['screen']['end_ms']+7*day:raise ValueError('candidate outside protocol test window')
 if not isinstance(prediction['screen_passed'],bool) or not isinstance(prediction['screen_metrics'],dict) or prediction['screen_metrics'].get('passed')!=prediction['screen_passed']:raise ValueError('invalid screen status')
 if not isinstance(prediction['no_trade_reasons'],list) or 'payout_unavailable' not in prediction['no_trade_reasons'] or (not prediction['screen_passed'] and 'historical_screen_failed' not in prediction['no_trade_reasons']):raise ValueError('missing no-trade reason')
 p=prediction['probability_up']
 if not isinstance(p,(float,int)) or not math.isfinite(p) or not 0<=p<=1:raise ValueError('invalid probability')
 if prediction['candidate_direction'] not in ('HIGHER','LOWER') or prediction['candidate_direction']!=('HIGHER' if p>=.5 else 'LOWER'):raise ValueError('invalid direction')
 # This recorder has no verified odds source and never emits a trade instruction.
 if prediction['decision']!='NO TRADE' or prediction['payout_source']!='unavailable':raise ValueError('only NO TRADE observations allowed')
 if any(x.get('id')==prediction['id'] for x in _events(path)):raise ValueError('duplicate prediction')
 recorded_ms=now()
 uncertainty=prediction['clock_uncertainty_bound_ms']
 if not isinstance(uncertainty,(int,float)) or not math.isfinite(uncertainty) or uncertainty<0:raise ValueError('invalid clock uncertainty')
 if not prediction['decision_ms']<=recorded_ms or recorded_ms+math.ceil(uncertainty)>=prediction['candidate_entry_ms']:raise ValueError('prediction must be recorded before entry including clock uncertainty')
 _append(path,dict(prediction,type='prediction',recorded_ms=recorded_ms))

def record_outcome(path,prediction,entry_tick,exit_tick,max_lag_ms=5000,now=lambda:time.time_ns()//1000000):
 existing=_events(path)
 if not any(x.get('id')==prediction['id'] and x.get('type')=='prediction' for x in existing):raise ValueError('prediction not in journal')
 if any(x.get('id')==prediction['id'] and x.get('type')=='outcome' for x in existing):raise ValueError('outcome already recorded')
 if now()<prediction['expiry_ms']:raise ValueError('outcome is not yet available')
 for tick,target in ((entry_tick,prediction['candidate_entry_ms']),(exit_tick,prediction['expiry_ms'])):
  if not (target<=tick['event_ms']<=target+max_lag_ms and tick['event_ms']<=tick['received_ms']):raise ValueError('price sample outside proxy window')
  try:
   price=Decimal(str(tick['index_price']))
   if not price.is_finite() or price<=0:raise ValueError('invalid price')
  except InvalidOperation:raise ValueError('invalid price') from None
 a=Decimal(str(entry_tick['index_price']));b=Decimal(str(exit_tick['index_price']))
 result='HIGHER' if b>a else 'LOWER' if b<a else 'EQUAL'
 _append(path,{'type':'outcome','id':prediction['id'],'entry':entry_tick,'exit':exit_tick,'result':result,'correct':None if result=='EQUAL' else result==prediction['candidate_direction'],'recorded_ms':now(),'interpretation':'index snapshot proxy, not contract settlement'})
