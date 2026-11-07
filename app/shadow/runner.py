"""Continuous, bounded-or-interruptible read-only shadow research runner."""
import fcntl,json,math,os,shutil,signal,socket,stat,struct,threading,time
from contextlib import contextmanager
from decimal import Decimal
from pathlib import Path
from urllib.parse import urlsplit
import websocket
from event_data.collect import collect_index_history,collect_openmarket
from event_data.storage import Store
from strategy_mvp.core import FEATURES,build_features
from strategy_research.observe import _fit_predictions,_load_recent
from .oauth import DedicatedOAuth,SharedCodexAccess
from .runtime import ShadowCoordinator,profiles_for_entry
from .stats import RESEARCH_BREAK_EVEN,RESEARCH_THRESHOLD
from .store import ShadowStore
from .power import sleep_guard


def now_ms():return time.time_ns()//1000000

def remaining_poll_seconds(interval,started,finished):return max(0,interval-(finished-started))

def conservative_resolution_time(local_ms,max_clock_error_ms):
 if max_clock_error_ms<0:raise ValueError('invalid clock uncertainty')
 return local_ms-max_clock_error_ms

def safe_error_code(error):
 codes={'clock uncertainty increased during preparation':'CLOCK_BOUND_CHANGED','data health unavailable or stale':'HEALTH_STALE','decision deadline or data health failed':'DECISION_OR_HEALTH','candidate is late or uses future features':'CANDIDATE_LATE','invalid feature availability':'FEATURE_UNAVAILABLE','last closed candle does not match target entry':'FEATURE_BOUNDARY','clock unavailable or stale':'CLOCK_UNAVAILABLE','clock uncertainty missing':'CLOCK_UNAVAILABLE','clock uncertainty unacceptable':'CLOCK_UNAVAILABLE','historical windows cross decision or test fold':'WINDOW_MISMATCH','safe stop requested':'SAFE_STOP'}
 codes.update({'CVD missing or stale':'CVD_STALE','spot or futures candle missing or stale':'MARKET_CANDLE_STALE','spot and futures candles misaligned':'MARKET_CANDLE_MISALIGNED','clock uncertainty unavailable':'CLOCK_UNAVAILABLE','missing live index bars':'INDEX_BARS_MISSING','index pagination stalled':'INDEX_PAGINATION_STALLED','feature warmup incomplete':'FEATURE_WARMUP','feature candle not closed by cutoff':'FEATURE_NOT_CLOSED','insufficient features':'INSUFFICIENT_FEATURES','no labeled history':'NO_LABELED_HISTORY','no completed historical labels':'NO_COMPLETED_LABELS','insufficient 30/4/3 day history':'HISTORY_INSUFFICIENT','incomplete protocol window':'PROTOCOL_WINDOW_INCOMPLETE','each training stage requires both directions':'LABEL_DIRECTION_MISSING','invalid historical window':'INVALID_HISTORICAL_WINDOW','invalid strategy sampling or feature boundary':'INVALID_SAMPLING_BOUNDARY','conflicting prediction for already recorded slot':'DUPLICATE_SLOT_CONFLICT'})
 return codes.get(str(error),type(error).__name__)

@contextmanager
def timed_stage(timings,name):
 started=time.monotonic_ns()
 try:yield
 finally:timings[name]=round((time.monotonic_ns()-started)/1000000,3)

def safe_health_snapshot(store,at_ms):
 return {row['source']:{'status':row['status'],'age_ms':at_ms-row['checked_ms']} for row in store.db.execute('SELECT source,status,checked_ms FROM health')}

def no_trade_reasons(screen_passed,probability):
 if not isinstance(screen_passed,bool) or not isinstance(probability,(int,float)) or not math.isfinite(probability) or not 0<=probability<=1:raise ValueError('invalid model signal')
 reasons=['verified_live_quote_unavailable']
 if not screen_passed:reasons.append('historical_screen_failed')
 if Decimal(str(max(probability,1-probability)))<=RESEARCH_THRESHOLD:reasons.append('research_probability_below_85pct_threshold')
 return reasons

def model_predictions_enabled(config):
 value=config.get('model_predictions_enabled',True)
 if not isinstance(value,bool):raise ValueError('invalid model_predictions_enabled configuration')
 return value

def validate_config(config):
 model_predictions_enabled(config)
 if config.get('symbol')!='BTCUSDT' or config.get('horizons')!=[30,60] or Decimal(str(config.get('research_payout_ratio'))) != Decimal('0.85') or Decimal(str(config.get('research_probability_margin'))) != Decimal('0.03'):raise ValueError('only approved BTCUSDT 30M/1H research parameters allowed')
 if not isinstance(config.get('prevent_idle_sleep',True),bool):raise ValueError('invalid prevent_idle_sleep configuration')
 if config.get('oauth_mode') not in ('shared_codex_readonly','dedicated'):raise ValueError('invalid OAuth mode')
 if config.get('clock_server')!='time.cloudflare.com':raise ValueError('unsupported clock source')
 for k in ('poll_seconds','health_max_age_ms','openmarket_max_age_ms','cvd_max_age_ms','clock_max_error_ms','tick_gap_threshold_ms','tick_max_delay_ms','proxy_price_window_ms','min_free_bytes','preparation_lead_ms'):
  if not isinstance(config.get(k),int) or config[k]<=0:raise ValueError('invalid positive configuration: '+k)
 if config['preparation_lead_ms']>=60000:raise ValueError('preparation lead must be less than one minute')
 if config['poll_seconds']>60:raise ValueError('health checks must run at least every minute when responsive')

def read_clock(server):
 if server!='time.cloudflare.com':raise ValueError('unsupported clock source')
 def stamp(unix_seconds):
  whole=int(unix_seconds+2208988800)
  fraction=int((unix_seconds%1)*2**32)
  return struct.pack('!II',whole,fraction)
 def unix(raw):
  whole,fraction=struct.unpack('!II',raw)
  return whole-2208988800+fraction/2**32
 try:
  with socket.socket(socket.AF_INET,socket.SOCK_DGRAM) as sock:
   sock.settimeout(3);sock.connect((server,123))
   request=bytearray(48);request[0]=0x23
   sent=time.time();request[40:48]=stamp(sent)
   sock.send(request);response=sock.recv(512);received=time.time()
  if len(response)<48 or response[0]&7!=4 or response[0]>>6==3 or not 1<=response[1]<=15 or response[24:32]!=request[40:48]:
   raise ValueError('NTP unavailable')
  server_received=unix(response[32:40]);server_sent=unix(response[40:48])
  if not 0<=server_sent-server_received<=3 or received<sent:raise ValueError('NTP unavailable')
  delay=(received-sent)-(server_sent-server_received)
  root_delay=struct.unpack('!i',response[4:8])[0]/2**16
  root_dispersion=struct.unpack('!I',response[8:12])[0]/2**16
  offset=((server_received-sent)+(server_sent-received))/2
  uncertainty=max(0,delay)/2+abs(root_delay)/2+root_dispersion+0.001
  if not all(math.isfinite(x) for x in (offset,uncertainty)):raise ValueError('NTP unavailable')
  return offset*1000,uncertainty*1000
 except (OSError,struct.error,OverflowError):raise ValueError('NTP unavailable') from None

def checked_clock_bound(store,now,max_age_ms,max_error_ms):
 row=store.get_health('clock')
 if not row or row['status']!='OK' or not 0<=now-row['checked_ms']<=max_age_ms:raise ValueError('clock unavailable or stale')
 try:bound=json.loads(row['detail'])['bound_ms']
 except (KeyError,TypeError,ValueError):raise ValueError('clock uncertainty missing') from None
 if not isinstance(bound,(int,float)) or not math.isfinite(bound) or not 0<=bound<=max_error_ms:raise ValueError('clock uncertainty unacceptable')
 return bound

def market_health(market,now,maximum_age,cvd_max_age,interval):
 # Only verified, actually received, closed source data; order book is deliberately excluded.
 cvd=market.db.execute("SELECT event_ms,available_ms,value FROM metrics WHERE source_verified=1 AND market='BINANCE_FUTURES' AND symbol='BTCUSDT' AND kind='cvd' AND data_age_seconds IS NOT NULL AND source_asof_ms<=available_ms AND event_ms<=? AND available_ms<=? ORDER BY event_ms DESC,available_ms DESC LIMIT 1",(now,now)).fetchone()
 if not cvd or now-cvd['event_ms']>cvd_max_age or cvd['value'] is None:raise ValueError('CVD missing or stale')
 market_opens=[]
 for market_name in ('BINANCE','BINANCE_FUTURES'):
  prefix='openmarket:'+market_name+':BTCUSDT:'
  row=market.db.execute('SELECT open_ms,close_ms,available_ms FROM bars WHERE dataset=? AND source_verified=1 AND close_ms<? AND available_ms>close_ms AND available_ms<=? ORDER BY close_ms DESC,available_ms DESC LIMIT 1',(prefix+interval,now,now)).fetchone()
  if not row or now-row['close_ms']>maximum_age:raise ValueError('spot or futures candle missing or stale')
  market_opens.append(row['open_ms'])
 if len(set(market_opens))!=1:raise ValueError('spot and futures candles misaligned')
 return {'cvd':{'value':cvd['value'],'source':'OPENMARKET_BINANCE_FUTURES','event_ms':cvd['event_ms'],'available_ms':cvd['available_ms'],'used_by_model':False}}

def clock_loop(config,shadow_path,stop):
 store=ShadowStore(shadow_path);coord=ShadowCoordinator(store,config['tick_gap_threshold_ms'])
 try:
  while not stop.is_set():
   started=time.monotonic()
   try:
    offset,uncertainty=read_clock(config['clock_server']);coord.update_clock(now_ms(),offset,uncertainty,config['clock_max_error_ms'])
   except (ValueError,OSError):store.set_health('clock','UNHEALTHY',now_ms(),{'reason':'independent clock unavailable'})
   stop.wait(remaining_poll_seconds(config['poll_seconds'],started,time.monotonic()))
 finally:store.close()

def market_loop(config,collector,shadow_path,market_path,stop):
 store=ShadowStore(shadow_path);provider=(DedicatedOAuth if config['oauth_mode']=='dedicated' else SharedCodexAccess)(config['oauth_credentials_path']);market=Store(market_path)
 try:
  while not stop.is_set():
   started=time.monotonic()
   try:
    token=provider.access(now_ms=now_ms(),refresh_before_ms=120000)
   except Exception as e:
    # Exception messages may contain credentials; record the type only.
    store.set_health('auth','UNHEALTHY',now_ms(),{'error_type':type(e).__name__});store.set_health('openmarket','UNHEALTHY',now_ms(),{'reason':'authorization unavailable'})
    store.event('OPENMARKET_AUTH_FAILED',now_ms(),{'error_type':type(e).__name__})
   else:
    store.set_health('auth','OK',now_ms(),{'mode':config['oauth_mode']})
    if config['oauth_mode']=='dedicated' and provider.last_access_refreshed:store.event('OAUTH_REFRESH_OK',now_ms(),{'mode':'dedicated'})
    try:
     ok,total=collect_openmarket(market,collector,token)
     details=market_health(market,now_ms(),config['openmarket_max_age_ms'],config['cvd_max_age_ms'],collector['openmarket_interval'])
     store.set_health('openmarket','OK',now_ms(),{'verified_queries':ok,'total_queries':total,'orderbook_excluded':True,'cvd_event_ms':details['cvd']['event_ms']})
     store.event('OPENMARKET_COLLECTION_OK',now_ms(),{'verified_queries':ok,'total_queries':total})
    except Exception as e:
     store.set_health('openmarket','UNHEALTHY',now_ms(),{'reason':'collection unavailable'})
     store.event('OPENMARKET_COLLECTION_FAILED',now_ms(),{'error_type':type(e).__name__})
    finally:del token
   stop.wait(remaining_poll_seconds(config['poll_seconds'],started,time.monotonic()))
 finally:market.close();store.close()

def stream_loop(config,collector,shadow_path,stop):
 store=ShadowStore(shadow_path);coord=ShadowCoordinator(store,config['tick_gap_threshold_ms']);ws=None
 u=urlsplit(collector['binance_index_stream'])
 if u.scheme!='wss' or u.netloc!='fstream.binance.com' or u.path!='/market/ws/btcusdt@markPrice@1s' or u.query or u.fragment:raise ValueError('invalid read-only stream')
 try:
  while not stop.is_set():
   try:
    if ws is None:ws=websocket.create_connection(collector['binance_index_stream'],timeout=5)
    p=json.loads(ws.recv());received=now_ms()
    if p.get('e')!='markPriceUpdate' or p.get('s')!='BTCUSDT':continue
    tick={'event_ms':int(p['E']),'received_ms':received,'index_price':str(p['i']),'mark_price':str(p['p'])};coord.on_tick(tick)
    if abs(received-tick['event_ms'])>config['tick_max_delay_ms']:store.set_health('index','STALE',received,{'event_ms':tick['event_ms']})
    store.resolve(conservative_resolution_time(received,config['clock_max_error_ms']),config['proxy_price_window_ms'])
   except (websocket.WebSocketException,OSError,ValueError,KeyError,TypeError) as e:
    store.set_health('index','DISCONNECTED',now_ms(),{'error_type':type(e).__name__});store.event('STREAM_DISCONNECT',now_ms(),{'error_type':type(e).__name__})
    if ws:ws.close();ws=None
    stop.wait(2)
 finally:
  if ws:ws.close()
  store.close()

def repair_gaps(config,collector,coord,market_path):
 gaps=coord.store.unresolved_gaps()
 if not gaps:return
 market=Store(market_path)
 try:
  for gap in gaps:
   start=(gap['previous_ms']//60000)*60000;end=(gap['next_ms']//60000+1)*60000
   if end>(now_ms()//60000)*60000:continue
   cursor=collect_index_history(market,collector,'1m',start,end)
   if cursor<end:continue
   bars=[b for b in market.bars('binance:index:BTCUSDT:1m') if start<=b['open_ms']<end]
   coord.confirm_backfill(gap['id'],bars)
 finally:market.close()

def prepare_candidates(config,collector,output,entry_ms,available_ms=None,timings=None):
 if timings is None:timings={}
 with timed_stage(timings,'load_ms'):bars,received=_load_recent(config['index_history_path'],collector)
 if int(bars.close_ms.iloc[-1])!=entry_ms-60001:raise ValueError('last closed candle does not match target entry')
 with timed_stage(timings,'features_ms'):features=build_features(bars);last=features.iloc[-1]
 if last[FEATURES].isna().any():raise ValueError('feature warmup incomplete')
 models=[]
 for mechanism,horizon in profiles_for_entry(entry_ms):
  with timed_stage(timings,f'fit_{mechanism}_{horizon}_ms'):models.extend((mechanism,x) for x in _fit_predictions(bars,output,mechanism,received,horizons=(horizon,),precomputed_features=features))
 return bars,received,last,models

@contextmanager
def runner_lock(directory):
 fd=os.open(str(Path(directory)/'runner.lock'),os.O_CREAT|os.O_RDWR|getattr(os,'O_NOFOLLOW',0),0o600)
 with os.fdopen(fd,'r+') as lock:
  try:fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
  except BlockingIOError:raise ValueError('another shadow runner owns this directory') from None
  yield

def private_directory(directory):
 path=Path(directory).expanduser()
 if path.is_symlink():raise ValueError('shadow directory cannot be a symlink')
 path.mkdir(mode=0o700,parents=True,exist_ok=True);info=path.stat()
 if not stat.S_ISDIR(info.st_mode) or info.st_uid!=os.getuid() or info.st_mode&0o077:raise ValueError('shadow directory must be private and owned by the current user')
 return path.resolve()

def request_safe_stop(directory):
 from .status import runner_active
 path=Path(directory).expanduser().resolve()
 if not runner_active(path):raise ValueError('no active runner in this directory')
 fd=os.open(str(path/'stop.request'),os.O_CREAT|os.O_EXCL|os.O_WRONLY|getattr(os,'O_NOFOLLOW',0),0o600)
 with os.fdopen(fd,'w') as f:f.write('stop\n');f.flush();os.fsync(f.fileno())

def _stop_watcher(directory,stop):
 while not stop.wait(1):
  if (directory/'stop.request').exists():stop.set();return

def ensure_not_stopping(directory,stop):
 if stop.is_set() or (directory/'stop.request').exists():raise ValueError('safe stop requested')

def run(config,collector,duration_seconds=None):
 validate_config(config)
 if duration_seconds is not None and duration_seconds<=0:raise ValueError('duration must be positive')
 directory=private_directory(config['data_directory'])
 with runner_lock(directory),sleep_guard(config.get('prevent_idle_sleep',True)) as guard:
  return _run(config,collector,duration_seconds,power_guard=guard)

def _run(config,collector,duration_seconds=None,power_guard=None):
 validate_config(config)
 directory=Path(config['data_directory']).expanduser().resolve();directory.mkdir(parents=True,exist_ok=True);(directory/'models').mkdir(exist_ok=True)
 if (directory/'stop.request').exists():raise ValueError('stale stop request; inspect before restarting')
 shadow_path=directory/'shadow.sqlite';market_path=directory/'market.sqlite';store=ShadowStore(shadow_path);coord=ShadowCoordinator(store,config['tick_gap_threshold_ms']);stop=threading.Event()
 # Pending outcomes from previous process are resolved or marked missing only from actually recorded ticks.
 store.resolve(conservative_resolution_time(now_ms(),config['clock_max_error_ms']),config['proxy_price_window_ms']);store.event('START',now_ms(),{'mode':'read-only-shadow'})
 workers=[threading.Thread(target=fn,args=args,daemon=True) for fn,args in [(clock_loop,(config,shadow_path,stop)),(market_loop,(config,collector,shadow_path,market_path,stop)),(stream_loop,(config,collector,shadow_path,stop)),(_stop_watcher,(directory,stop))]]
 for t in workers:t.start()
 start=time.monotonic();previous_handlers={}
 def request_stop(signum,frame):stop.set()
 for sig in (signal.SIGINT,signal.SIGTERM):previous_handlers[sig]=signal.signal(sig,request_stop)
 try:
  while not stop.is_set() and (duration_seconds is None or time.monotonic()-start<duration_seconds):
   if power_guard:power_guard.check()
   now=now_ms();store.resolve(conservative_resolution_time(now,config['clock_max_error_ms']),config['proxy_price_window_ms'])
   free=shutil.disk_usage(directory).free;store.set_health('storage','OK' if free>=config['min_free_bytes'] else 'UNHEALTHY',now,{'free_bytes':free})
   try:repair_gaps(config,collector,coord,market_path)
   except Exception as e:store.event('BACKFILL_ERROR',now_ms(),{'error_type':type(e).__name__})
   if not model_predictions_enabled(config):
    stop.wait(min(config['poll_seconds'],10));continue
   entry=((now+config['preparation_lead_ms']+59999)//60000)*60000;prepare_at=entry-config['preparation_lead_ms']
   wait=max(0,(prepare_at-now_ms())/1000)
   if duration_seconds is not None:wait=min(wait,max(0,duration_seconds-(time.monotonic()-start)))
   if stop.wait(wait):break
   if duration_seconds is not None and time.monotonic()-start>=duration_seconds:break
   if power_guard:power_guard.check()
   checked=now_ms()
   if not store.health_ok(checked,config['health_max_age_ms']):store.event('CANDIDATE_SKIPPED',checked,{'entry_ms':entry,'error_code':'HEALTH_STALE','health':safe_health_snapshot(store,checked)});continue
   timings={}
   try:
    with timed_stage(timings,'prepare_total_ms'):bars,available,last,models=prepare_candidates(config,collector,directory/'models',entry,timings=timings)
    market=Store(market_path)
    try:market_features=market_health(market,now_ms(),config['openmarket_max_age_ms'],config['cvd_max_age_ms'],collector['openmarket_interval'])
    finally:market.close()
    bound=checked_clock_bound(store,now_ms(),config['health_max_age_ms'],config['clock_max_error_ms']);decision_at=entry-max(1000,math.ceil(bound)+500)
    wait=max(0,(decision_at-now_ms())/1000)
    if duration_seconds is not None:wait=min(wait,max(0,duration_seconds-(time.monotonic()-start)))
    if stop.wait(wait):break
    if duration_seconds is not None and time.monotonic()-start>=duration_seconds:break
    if power_guard:power_guard.check()
    decision=now_ms()
    if decision>=entry or not store.health_ok(decision,config['health_max_age_ms'],{'index':config['tick_max_delay_ms']}) or decision-market_features['cvd']['event_ms']>config['cvd_max_age_ms']:raise ValueError('decision deadline or data health failed')
    features={k:{'value':str(float(last[k])),'source':'BINANCE_INDEX','event_ms':int(bars.close_ms.iloc[-1]),'available_ms':available,'used_by_model':True} for k in FEATURES};features.update(market_features)
    for mechanism,(horizon,prob,version,windows,gate) in models:
     ensure_not_stopping(directory,stop)
     reasons=no_trade_reasons(gate['passed'],prob)
     candidate={'classification':'shadow_v1','symbol':'BTCUSDT','mechanism':mechanism,'training_spacing_minutes':horizon if mechanism=='FIXED' else 1,'horizon_minutes':horizon,'decision_ms':decision,'entry_ms':entry,'expiry_ms':entry+horizon*60000,'feature_end_ms':int(bars.close_ms.iloc[-1]),'feature_available_ms':max(available,*[v['available_ms'] for v in market_features.values()]),'features':features,'model_version':version,'screen_version':'wf-30-4-3-v1','screen_passed':gate['passed'],'screen_metrics':gate,'windows':windows,'probability_up':prob,'candidate_direction':'HIGHER' if prob>=.5 else 'LOWER','execution_decision':'NO TRADE','no_trade_reasons':reasons,'clock_uncertainty_ms':bound,'research_payout_ratio':'0.85','research_probability_threshold':str(RESEARCH_THRESHOLD),'live_quote_verified':False}
     with timed_stage(timings,f'write_{mechanism}_{horizon}_ms'):store.record_candidate(candidate,now_ms(),config['health_max_age_ms'],{'index':config['tick_max_delay_ms']})
   except Exception as e:
    checked=now_ms();store.event('CANDIDATE_SKIPPED',checked,{'entry_ms':entry,'error_code':safe_error_code(e),'health':safe_health_snapshot(store,checked)})
   finally:store.event('PREPARATION_TIMINGS',now_ms(),{'entry_ms':entry,'durations_ms':timings})
 finally:
  stop.set();coord.safe_stop(now_ms())
  for t in workers:t.join(timeout=3)
  request=directory/'stop.request'
  if request.exists():request.unlink()
  for sig,handler in previous_handlers.items():signal.signal(sig,handler)
  store.close()
