"""Bounded read-only forward observation. Never places orders or starts a service."""
import argparse,hashlib,json,math,re,threading,time,uuid
from datetime import datetime
from pathlib import Path
from urllib.parse import urlsplit
import joblib
import pandas as pd
import websocket
from sklearn.linear_model import LogisticRegression
from event_data.collect import collect_openmarket,load_token
from event_data.storage import Store
from event_data.transport import binance_get
from strategy_mvp.core import FEATURES,build_features,build_samples,load_index
from strategy_mvp.model import _logit,make_models
from .study import DAY,screen_gate
from .forward import _append,record_prediction,record_outcome


def now_ms():return time.time_ns()//1000000

def validate_prerequisites(summary,clock,now_ms,max_error_ms=1000,max_age_ms=300000):
 if summary.get('duration_seconds',0)<3600 or not summary.get('forced_reconnect') or summary.get('samples',0)<2:raise ValueError('60-minute WS test incomplete')
 if clock.get('clock_adjustment') is not False:raise ValueError('unexpected clock adjustment')
 samples=[]
 for item in clock.get('samples',[]):
  match=re.search(r'([+-]\d+\.\d+)\s+\+/-\s+(\d+\.\d+)',item.get('result') or '')
  if match:
   stamp=datetime.fromisoformat(item['start_utc']).timestamp()*1000
   if 0<=now_ms-stamp<=max_age_ms:samples.append((abs(float(match.group(1)))+float(match.group(2)))*1000)
 if len(samples)<2 or max(samples)>max_error_ms:raise ValueError('clock check stale or uncertainty too large')
 return max(samples)

def _load_recent(history,config):
 bars=load_index(history);start=int(bars.open_ms.iloc[-1])+60000;end=(now_ms()//60000)*60000
 added=[]
 while start<end:
  raw=binance_get(config['binance_rest'],'/fapi/v1/indexPriceKlines',{'pair':'BTCUSDT','interval':'1m','startTime':start,'endTime':end-1,'limit':config['page_limit']},config['timeout_seconds'],config['read_retries'])
  received=now_ms()
  if not raw:raise ValueError('missing live index bars')
  for r in raw:
   if r[0]>=start and r[6]<received:added.append({'open_ms':int(r[0]),'open':float(r[1]),'high':float(r[2]),'low':float(r[3]),'close':float(r[4]),'close_ms':int(r[6])})
  nxt=int(raw[-1][0])+60000
  if nxt<=start:raise ValueError('index pagination stalled')
  start=nxt
 if added:bars=pd.concat([bars,pd.DataFrame(added)],ignore_index=True)
 return bars,now_ms()

def training_spacing(mechanism,horizon):
 if mechanism not in ('FIXED','ROLLING') or horizon not in (30,60):raise ValueError('invalid strategy mechanism or horizon')
 return horizon if mechanism=='FIXED' else 1

def candidate_entry(mechanism,horizon,decision_ms,lead_ms=45000):
 step=training_spacing(mechanism,horizon)*60000
 return ((decision_ms+lead_ms+step-1)//step)*step

def protocol_windows(samples,cutoff_ms):
 if samples.empty:raise ValueError('no labeled history')
 valid=(samples.feature_end_ms<samples.decision_ms)&(samples.decision_ms<samples.entry_ms)&(samples.exit_ms<cutoff_ms)
 s=samples.loc[valid].sort_values('entry_ms')
 if s.empty:raise ValueError('no completed historical labels')
 origin=(int(s.entry_ms.min())//DAY+1)*DAY
 first_screen_end=origin+37*DAY
 if cutoff_ms<first_screen_end:raise ValueError('insufficient 30/4/3 day history')
 start=origin+((cutoff_ms-first_screen_end)//(7*DAY))*7*DAY
 b=start+30*DAY;c=b+4*DAY;d=c+3*DAY
 def part(lo,hi):return s[(s.decision_ms>=lo)&(s.decision_ms<hi)&(s.exit_ms<hi)]
 train=part(start,b);cal=part(b,c);screen=part(c,d)
 if min(len(train),len(cal),len(screen))==0:raise ValueError('incomplete protocol window')
 def meta(frame,lo,hi):return {'start_ms':int(lo),'end_ms':int(hi),'last_exit_ms':int(frame.exit_ms.max()),'samples':len(frame)}
 return train,cal,screen,{'train':meta(train,start,b),'calibration':meta(cal,b,c),'screen':meta(screen,c,d),'training_cutoff_ms':int(cutoff_ms)}

def _fit_predictions(bars,output,mechanism,cutoff_ms,horizons=(30,60),precomputed_features=None):
 # A SHADOW minute can train 2-4 models from the same immutable index bars.
 # Reuse the caller's complete feature matrix instead of rebuilding it per model.
 features=build_features(bars) if precomputed_features is None else precomputed_features
 if len(features)!=len(bars) or not features.index.equals(bars.reset_index(drop=True).index) or any(k not in features.columns for k in FEATURES):
  raise ValueError('precomputed features do not align with index bars')
 last=features.iloc[[-1]][FEATURES]
 if int(bars.close_ms.iloc[-1])>=cutoff_ms:raise ValueError('feature candle not closed by cutoff')
 if last.isna().any().any():raise ValueError('insufficient features')
 predictions=[]
 for horizon in horizons:
  samples=build_samples(bars,horizon,entry_spacing_minutes=training_spacing(mechanism,horizon),features=features);train,cal,screen,windows=protocol_windows(samples,cutoff_ms)
  if any(frame.label.nunique()!=2 for frame in (train,cal,screen)):raise ValueError('each training stage requires both directions')
  model=make_models()['logistic_regression'];model.fit(train[FEATURES],train.label)
  calibration=LogisticRegression(C=1.0,random_state=17).fit(_logit(model.predict_proba(cal[FEATURES])[:,1]),cal.label)
  screen_prob=calibration.predict_proba(_logit(model.predict_proba(screen[FEATURES])[:,1]))[:,1];gate=screen_gate(screen.label,screen_prob)
  probability=float(calibration.predict_proba(_logit(model.predict_proba(last)[:,1]))[0,1])
  path=output/f'model-{mechanism.lower()}-{horizon}m-{uuid.uuid4()}.joblib';joblib.dump({'model':model,'calibration':calibration,'features':FEATURES,'windows':windows,'screen_gate':gate,'mechanism':mechanism,'horizon_minutes':horizon},path)
  version='sha256:'+hashlib.sha256(path.read_bytes()).hexdigest();predictions.append((horizon,probability,version,windows,gate))
 return predictions

def _openmarket_loop(config,directory,stop,interval):
 while not stop.is_set():
  store=None
  try:
   store=Store(directory/'openmarket.sqlite');ok,total=collect_openmarket(store,config,load_token(config['openmarket_credentials_path']))
   _append(directory/'collection-events.jsonl',{'source':'openmarket','received_ms':now_ms(),'verified_queries':ok,'total_queries':total})
  except Exception as e:_append(directory/'collection-events.jsonl',{'source':'openmarket','received_ms':now_ms(),'error':type(e).__name__})
  finally:
   if store:store.close()
  if stop.wait(interval):break

def run(history,config,summary,clock,directory,mechanism,horizon,poll_seconds=300,max_lag_ms=5000):
 training_spacing(mechanism,horizon)
 directory=Path(directory);directory.mkdir(parents=True,exist_ok=True)
 error_bound=validate_prerequisites(json.loads(Path(summary).read_text()),json.loads(Path(clock).read_text()),now_ms())
 config=json.loads(Path(config).read_text());stream=urlsplit(config['binance_index_stream'])
 if stream.scheme!='wss' or stream.netloc!='fstream.binance.com' or stream.path!='/market/ws/btcusdt@markPrice@1s' or stream.query or stream.fragment:raise ValueError('invalid read-only index stream')
 entry=candidate_entry(mechanism,horizon,now_ms())
 while now_ms()<entry-45000:time.sleep(min(30,(entry-45000-now_ms())/1000))
 error_bound=validate_prerequisites(json.loads(Path(summary).read_text()),json.loads(Path(clock).read_text()),now_ms())
 bars,available_ms=_load_recent(history,config)
 if int(bars.close_ms.iloc[-1])!=entry-60001:raise ValueError('feature candle does not match research entry boundary')
 models=_fit_predictions(bars,directory,mechanism,available_ms,horizons=(horizon,))
 decision_lead_ms=max(1000,math.ceil(error_bound)+500)
 while now_ms()<entry-decision_lead_ms:time.sleep(min(.1,(entry-decision_lead_ms-now_ms())/1000))
 decision=now_ms()
 if decision>=entry:raise ValueError('decision deadline missed; no prediction recorded')
 journal=directory/'predictions.jsonl';predictions=[]
 for horizon,probability,version,windows,gate in models:
  reasons=['payout_unavailable']+([] if gate['passed'] else ['historical_screen_failed'])
  p={'id':str(uuid.uuid4()),'classification':'protocol_forward','strategy_mechanism':mechanism,'training_spacing_minutes':training_spacing(mechanism,horizon),'windows':windows,'screen_passed':gate['passed'],'screen_metrics':gate,'no_trade_reasons':reasons,'model_version':version,'decision_ms':decision,'candidate_entry_ms':entry,'expiry_ms':entry+horizon*60000,'horizon_minutes':horizon,'probability_up':probability,'candidate_direction':'HIGHER' if probability>=.5 else 'LOWER','decision':'NO TRADE','price_source':'binance_index_websocket','feature_end_ms':int(bars.close_ms.iloc[-1]),'feature_available_ms':available_ms,'payout_source':'unavailable','clock_uncertainty_bound_ms':error_bound,'mode':'read_only_forward_observation'}
  if p['feature_end_ms']>=decision:raise ValueError('unfinished feature bar')
  record_prediction(journal,p);predictions.append(p)
 _append(directory/'collection-events.jsonl',{'source':'forward','status':'started','recorded_ms':now_ms(),'entry_ms':[p['candidate_entry_ms'] for p in predictions],'expiry_ms':[p['expiry_ms'] for p in predictions]})
 stop=threading.Event();worker=threading.Thread(target=_openmarket_loop,args=(config,directory,stop,poll_seconds),daemon=True);worker.start()
 expiry=max(p['expiry_ms'] for p in predictions);entry_ticks={};done=set();ws=None;previous=None
 try:
  while now_ms()<=expiry+max_lag_ms:
   try:
    if ws is None:ws=websocket.create_connection(config['binance_index_stream'],timeout=config['timeout_seconds'])
    payload=json.loads(ws.recv());received=now_ms()
    if payload.get('e')!='markPriceUpdate' or payload.get('s')!='BTCUSDT':continue
    tick={'event_ms':int(payload['E']),'received_ms':received,'index_price':str(payload['i']),'mark_price':str(payload['p'])}
    if previous is not None and tick['event_ms']-previous>config['stream_gap_threshold_ms']:_append(directory/'collection-events.jsonl',{'source':'websocket','status':'gap','previous_ms':previous,'next_ms':tick['event_ms']})
    previous=tick['event_ms'];_append(directory/'index-ticks.jsonl',tick)
    for p in predictions:
     if p['id'] not in entry_ticks and p['candidate_entry_ms']<=tick['event_ms']<=p['candidate_entry_ms']+max_lag_ms:entry_ticks[p['id']]=tick
     if p['id'] not in done and p['id'] in entry_ticks and p['expiry_ms']<=tick['event_ms']<=p['expiry_ms']+max_lag_ms:
      try:record_outcome(journal,p,entry_ticks[p['id']],tick,max_lag_ms=max_lag_ms);done.add(p['id'])
      except ValueError as e:_append(directory/'collection-events.jsonl',{'source':'forward','status':'invalid_outcome','id':p['id'],'error':str(e)})
   except (websocket.WebSocketException,OSError,ValueError) as e:
    _append(directory/'collection-events.jsonl',{'source':'websocket','status':'disconnect','recorded_ms':now_ms(),'error':type(e).__name__})
    if ws:ws.close();ws=None
    time.sleep(2)
 finally:
  if ws:ws.close()
  stop.set()
  _append(directory/'collection-events.jsonl',{'source':'forward','status':'finished','recorded_ms':now_ms(),'outcomes_recorded':len(done),'expected':len(predictions)})

def main():
 p=argparse.ArgumentParser();p.add_argument('--history',required=True);p.add_argument('--config',required=True);p.add_argument('--summary',required=True);p.add_argument('--clock',required=True);p.add_argument('--directory',required=True);p.add_argument('--mechanism',required=True,choices=['FIXED','ROLLING']);p.add_argument('--horizon',required=True,type=int,choices=[30,60]);a=p.parse_args();run(a.history,a.config,a.summary,a.clock,a.directory,a.mechanism,a.horizon)
if __name__=='__main__':main()
