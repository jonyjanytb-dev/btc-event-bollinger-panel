"""Strict chronological evaluation; calibration uses validation only."""
from pathlib import Path
import sqlite3
from urllib.parse import quote
import numpy as np
import pandas as pd
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import roc_auc_score
from .core import FEATURES,evaluate,sensitivity,align_enhanced,purged_split


def _logit(p):
 p=np.clip(np.asarray(p),1e-6,1-1e-6)
 return np.log(p/(1-p)).reshape(-1,1)

def bootstrap_days(test,probs,seed=77,repeats=1000):
 days=(test.entry_ms.to_numpy()//86400000);wins=((np.asarray(probs)>=.5)==test.label.to_numpy()).astype(float)
 unique=np.unique(days);rng=np.random.default_rng(seed)
 if len(unique)<2:return None
 blocks=[wins[days==d] for d in unique];estimates=[]
 for _ in range(repeats):
  indices=rng.integers(0,len(blocks),len(blocks));estimates.append(np.concatenate([blocks[i] for i in indices]).mean())
 return [float(v) for v in np.quantile(estimates,[.025,.975])]

def probability_distribution(probabilities):
 p=np.asarray(probabilities,dtype=float)
 if len(p)==0 or not np.isfinite(p).all() or (p<0).any() or (p>1).any():raise ValueError('invalid probabilities')
 return {'higher_fraction':float((p>=.5).mean()),'lower_fraction':float((p<.5).mean()),'mean':float(p.mean()),'std':float(p.std()),'quantiles':{str(q):float(np.quantile(p,q)) for q in (0,.1,.25,.5,.75,.9,1)}}

def baseline_results(labels):
 y=np.asarray(labels,dtype=int)
 return {name:evaluate(y,np.full(len(y),p)) for name,p in [('always_higher',1.0),('always_lower',0.0),('constant_50_percent',.5)]}

def evaluate_model(model,train,val,test,columns=FEATURES):
 for frame in (train,val,test):
  if 'decision_ms' not in frame or not ((frame.feature_end_ms<frame.decision_ms)&(frame.decision_ms<frame.entry_ms)).all():raise ValueError('invalid decision time')
 if any(x.label.nunique()!=2 for x in (train,val,test)):raise ValueError('each partition requires both directions')
 if not (train.exit_ms.max()<val.entry_ms.min() and val.exit_ms.max()<test.entry_ms.min()):raise ValueError('outcome overlaps a subsequent partition')
 model.fit(train[columns],train.label)
 pval=model.predict_proba(val[columns])[:,1];ptest=model.predict_proba(test[columns])[:,1]
 # The calibration fit never sees test labels or features.
 calibrator=LogisticRegression(C=1.0,random_state=17).fit(_logit(pval),val.label)
 calibrated=calibrator.predict_proba(_logit(ptest))[:,1]
 result={'uncalibrated':evaluate(test.label,ptest),'calibrated':evaluate(test.label,calibrated),'auc':float(roc_auc_score(test.label,calibrated)),'accuracy_day_block_bootstrap_95':bootstrap_days(test,calibrated),'hypothetical_payout_sensitivity':sensitivity(test.label,calibrated,[.7,.8,.85]),'regimes':{}}
 result['probability_distribution']={'uncalibrated':probability_distribution(ptest),'calibrated':probability_distribution(calibrated)}
 # Regime boundaries are learned exclusively from training data.
 low,high=train.rv30.quantile([.33,.67]);trend_low,trend_high=train.roc_60.quantile([.33,.67])
 definitions={'low_vol':test.rv30<=low,'medium_vol':(test.rv30>low)&(test.rv30<=high),'high_vol':test.rv30>high,'down_trend':test.roc_60<=trend_low,'sideways':(test.roc_60>trend_low)&(test.roc_60<=trend_high),'up_trend':test.roc_60>trend_high}
 for name,mask in definitions.items():
  if mask.any():
   stats=evaluate(test.label[mask],calibrated[np.asarray(mask)]);result['regimes'][name]={k:stats[k] for k in ('n','accuracy','brier','log_loss')}
 result['regime_thresholds_train']={'rv_low':float(low),'rv_high':float(high),'trend_low':float(trend_low),'trend_high':float(trend_high)}
 return result

def make_models():
 from lightgbm import LGBMClassifier
 from sklearn.pipeline import make_pipeline
 from sklearn.preprocessing import StandardScaler
 return {'logistic_regression':make_pipeline(StandardScaler(),LogisticRegression(max_iter=1000,C=1.0,random_state=17)),'lightgbm':LGBMClassifier(n_estimators=150,learning_rate=.03,num_leaves=7,max_depth=3,min_child_samples=50,reg_lambda=2,random_state=17,n_jobs=1,verbosity=-1)}

def read_observations(path,kind='cvd'):
 # Read-only connection; never creates, migrates, or modifies the collector DB.
 uri='file:'+quote(str(Path(path).resolve()))+'?mode=ro'
 with sqlite3.connect(uri,uri=True) as db:
  q="""SELECT event_ms,available_ms,value FROM metrics WHERE kind=? AND market='BINANCE_FUTURES' AND symbol='BTCUSDT' AND source_verified=1 AND end_ms IS NULL AND data_age_seconds IS NOT NULL AND source_asof_ms IS NOT NULL AND event_ms<=source_asof_ms AND source_asof_ms<=available_ms AND value IS NOT NULL ORDER BY event_ms,available_ms"""
  return pd.read_sql_query(q,db,params=(kind,))

def enhanced_comparison(samples,db_path,max_age_ms=300000,min_samples=500):
 if not db_path:return {'status':'unavailable','reason':'No verified real-time observation database supplied; no retrospective availability was invented.'}
 observations=read_observations(db_path)
 joined=samples.copy();joined['cvd']=align_enhanced(joined,observations,max_age_ms);joined=joined.dropna(subset=['cvd'])
 result={'status':'insufficient_data','observable_common_samples':len(joined),'source_observations':len(observations),'min_required':min_samples,'feature':'cvd'}
 if len(joined)<min_samples:return result
 train,val,test=purged_split(joined)
 result.update({'status':'evaluated','samples':{'train':len(train),'validation':len(val),'test':len(test)},'models':{}})
 for name,model in make_models().items():
  result['models'][name]={'price_only_same_common_window':evaluate_model(model,train,val,test),'price_plus_cvd':evaluate_model(make_models()[name],train,val,test,FEATURES+['cvd'])}
 return result
