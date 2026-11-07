"""Predeclared research rules, purged walks and day-block summaries."""
import numpy as np
import pandas as pd
from strategy_mvp.core import evaluate
DAY=86400000
THRESHOLDS=(.50,.52,.55,.60)

def signal(probability,payout_ratio,margin):
 if not np.isfinite(probability) or not 0<=probability<=1 or not np.isfinite(margin) or margin<0:raise ValueError('invalid probability or margin')
 if payout_ratio is None:return 'NO TRADE'
 if not np.isfinite(payout_ratio) or payout_ratio<=0:raise ValueError('invalid payout')
 threshold=1/(1+payout_ratio)+margin
 if probability>threshold:return 'HIGHER'
 if 1-probability>threshold:return 'LOWER'
 return 'NO TRADE'

def screen_gate(labels,probabilities,min_samples=50):
 y=np.asarray(labels,dtype=int);result=evaluate(y,probabilities);majority=float(max(y.mean(),1-y.mean()))
 return {'n':len(y),'accuracy':result['accuracy'],'majority_baseline':majority,'brier':result['brier'],'passed':bool(len(y)>=min_samples and result['accuracy']>majority and result['brier']<.25)}

def walk_windows(samples,train_days=30,cal_days=4,screen_days=3,test_days=7):
 if min(train_days,cal_days,screen_days,test_days)<=0:raise ValueError('window lengths must be positive')
 s=samples.sort_values('entry_ms');start=(int(s.entry_ms.min())//DAY+1)*DAY;last=int(s.entry_ms.max())
 while start+(train_days+cal_days+screen_days+test_days)*DAY<=last:
  b=start+train_days*DAY;c=b+cal_days*DAY;d=c+screen_days*DAY;e=d+test_days*DAY
  # Outcomes must be known strictly before the next decision interval begins.
  train=s[(s.decision_ms>=start)&(s.decision_ms<b)&(s.exit_ms<b)]
  cal=s[(s.decision_ms>=b)&(s.decision_ms<c)&(s.exit_ms<c)]
  screen=s[(s.decision_ms>=c)&(s.decision_ms<d)&(s.exit_ms<d)]
  test=s[(s.decision_ms>=d)&(s.decision_ms<e)&(s.exit_ms<e)]
  if min(len(train),len(cal),len(screen),len(test))>0:yield train.copy(),cal.copy(),screen.copy(),test.copy()
  start+=test_days*DAY

def _day_ci(times,correct,seed=17,repeats=1000):
 days=np.asarray(times,dtype=np.int64)//DAY;correct=np.asarray(correct,dtype=float);unique=np.unique(days)
 if len(unique)<2:return None
 counts=np.array([(days==d).sum() for d in unique]);wins=np.array([correct[days==d].sum() for d in unique]);rng=np.random.default_rng(seed)
 draws=rng.integers(0,len(unique),size=(repeats,len(unique)));vals=wins[draws].sum(axis=1)/counts[draws].sum(axis=1)
 return [float(x) for x in np.quantile(vals,[.025,.975])]

def summarize(samples,probabilities,thresholds=THRESHOLDS):
 y=samples.label.to_numpy();p=np.asarray(probabilities,dtype=float);evaluate(y,p);confidence=np.maximum(p,1-p);correct=((p>=.5)==y);out={}
 for threshold in thresholds:
  mask=confidence>=threshold;count=int(mask.sum())
  out[f'{threshold:.2f}']={'signals':count,'coverage':count/len(y),'higher':int(((p>=.5)&mask).sum()),'lower':int(((p<.5)&mask).sum()),'accuracy':float(correct[mask].mean()) if count else None,'accuracy_ci':_day_ci(samples.entry_ms.to_numpy()[mask],correct[mask]) if count else None,'distinct_days':len(np.unique(samples.entry_ms.to_numpy()[mask]//DAY)) if count else 0}
 return out

def compare_days(fixed,rolling):
 def daily(frame):return frame.assign(day=frame.entry_ms//DAY).groupby('day')['correct'].mean()
 pairs=pd.concat([daily(fixed).rename('fixed'),daily(rolling).rename('rolling')],axis=1,join='inner').dropna();diff=(pairs['rolling']-pairs['fixed']).to_numpy()
 if not len(diff):return {'common_days':0,'difference_rolling_minus_fixed':None,'paired_day_ci':None}
 rng=np.random.default_rng(17);means=[rng.choice(diff,size=len(diff),replace=True).mean() for _ in range(1000)] if len(diff)>1 else []
 return {'common_days':len(diff),'difference_rolling_minus_fixed':float(diff.mean()),'paired_day_ci':[float(x) for x in np.quantile(means,[.025,.975])] if means else None}
