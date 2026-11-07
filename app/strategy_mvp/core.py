"""Minute-bar approximations. No event contract settlement or order access."""
import json
import numpy as np
import pandas as pd

FEATURES=['ema20_ratio','ema50_ratio','ema200_ratio','rsi14','atr14_ratio','adx14','roc_5','roc_15','roc_30','roc_60','rv30','bbwidth20','ema20_5m_ratio','roc_5_5m']

def load_index(path):
 with open(path) as f:
  raw=[json.loads(line) for line in f if line.strip()]
 return pd.DataFrame([{'open_ms':int(r[0]),'open':float(r[1]),'high':float(r[2]),'low':float(r[3]),'close':float(r[4]),'close_ms':int(r[6])} for r in raw])

def build_features(bars):
 b=bars.reset_index(drop=True).copy()
 if len(b)==0 or not np.all(np.diff(b.open_ms.to_numpy())==60000) or not np.all(b.close_ms==b.open_ms+59999) or not np.all(b.open_ms%60000==0):raise ValueError('index minutes must be continuous and UTC aligned')
 p=b[['open','high','low','close']].to_numpy(dtype=float)
 if not np.isfinite(p).all() or (p<=0).any() or (b.low>b[['open','close']].min(axis=1)).any() or (b.high<b[['open','close']].max(axis=1)).any() or (b.low>b.high).any():raise ValueError('invalid index OHLC')
 c=b.close.astype(float);h=b.high.astype(float);lo=b.low.astype(float)
 out=pd.DataFrame(index=b.index)
 for n in (20,50,200):
  ema=c.ewm(span=n,adjust=False,min_periods=n).mean();out[f'ema{n}']=ema;out[f'ema{n}_ratio']=ema/c-1
 change=c.diff();gain=change.clip(lower=0).ewm(alpha=1/14,adjust=False,min_periods=14).mean();loss=(-change.clip(upper=0)).ewm(alpha=1/14,adjust=False,min_periods=14).mean()
 rs=gain/loss.replace(0,np.nan);out['rsi14']=100-100/(1+rs);out.loc[(gain>0)&(loss==0),'rsi14']=100;out.loc[(gain==0)&(loss==0),'rsi14']=50
 tr=pd.concat([h-lo,(h-c.shift()).abs(),(lo-c.shift()).abs()],axis=1).max(axis=1)
 atr=tr.ewm(alpha=1/14,adjust=False,min_periods=14).mean();out['atr14_ratio']=atr/c
 up=h.diff();down=-lo.diff();plus=up.where((up>down)&(up>0),0.0);minus=down.where((down>up)&(down>0),0.0)
 plus_di=100*plus.ewm(alpha=1/14,adjust=False,min_periods=14).mean()/atr.replace(0,np.nan);minus_di=100*minus.ewm(alpha=1/14,adjust=False,min_periods=14).mean()/atr.replace(0,np.nan)
 dx=100*(plus_di-minus_di).abs()/(plus_di+minus_di).replace(0,np.nan);out['adx14']=dx.ewm(alpha=1/14,adjust=False,min_periods=14).mean()
 for n in (5,15,30,60):out[f'roc_{n}']=c/c.shift(n)-1
 logret=np.log(c/c.shift(1));out['rv30']=logret.rolling(30,min_periods=30).std(ddof=0)
 mean=c.rolling(20,min_periods=20).mean();out['bbwidth20']=4*c.rolling(20,min_periods=20).std(ddof=0)/mean
 # Five-minute aggregates become visible only at the close of the fifth minute.
 groups=b.groupby(b.open_ms//300000,sort=True)
 five=groups.agg(close=('close','last'),count=('close','size'),last_index=('close_ms',lambda x:x.index[-1]))
 five=five[five['count']==5];fclose=five['close']
 five_features=pd.DataFrame({'ema20_5m_ratio':(fclose.ewm(span=20,adjust=False,min_periods=20).mean()/fclose-1).to_numpy(),'roc_5_5m':(fclose/fclose.shift(5)-1).to_numpy()},index=five.last_index.to_numpy())
 out=out.join(five_features)
 out[['ema20_5m_ratio','roc_5_5m']]=out[['ema20_5m_ratio','roc_5_5m']].ffill()
 return out.replace([np.inf,-np.inf],np.nan)

def build_samples(bars,horizon,entry_spacing_minutes=None,features=None):
 if horizon not in (30,60):raise ValueError('supported horizons: 30 or 60')
 spacing=horizon if entry_spacing_minutes is None else entry_spacing_minutes
 if not isinstance(spacing,int) or spacing<=0:raise ValueError('invalid entry spacing')
 b=bars.reset_index(drop=True);f=build_features(b) if features is None else features
 if len(f)!=len(b) or not f.index.equals(b.index):raise ValueError('features must align with bars')
 # A full minute after feature candle closes, entry at next minute open.
 indices=np.arange(1,len(b)-horizon-2,dtype=int);entry=indices+2;exit_=entry+horizon
 aligned=(b.open_ms.to_numpy()[entry]//60000)%spacing==0
 indices=indices[aligned];entry=entry[aligned];exit_=exit_[aligned]
 result=f.iloc[indices][FEATURES].copy().reset_index(drop=True)
 result['feature_end_ms']=b.close_ms.to_numpy()[indices];result['entry_ms']=b.open_ms.to_numpy()[entry];result['exit_ms']=b.open_ms.to_numpy()[exit_]
 result['decision_ms']=result['entry_ms']-1000
 result['entry_price']=b.open.to_numpy()[entry];result['exit_price']=b.open.to_numpy()[exit_]
 result['label']=(result.exit_price>result.entry_price).astype(int)
 result=result.loc[(result.exit_price!=result.entry_price)&result[FEATURES].notna().all(axis=1)].reset_index(drop=True)
 return result

def purged_split(samples,train_fraction=.6,val_fraction=.2):
 if not (0<train_fraction<1 and 0<val_fraction<1 and train_fraction+val_fraction<1):raise ValueError('invalid split fractions')
 s=samples.sort_values('entry_ms').reset_index(drop=True)
 if len(s)<10:raise ValueError('not enough samples')
 a=s.entry_ms.iloc[int(len(s)*train_fraction)];b=s.entry_ms.iloc[int(len(s)*(train_fraction+val_fraction))]
 train=s[(s.entry_ms<a)&(s.exit_ms<a)];val=s[(s.entry_ms>=a)&(s.entry_ms<b)&(s.exit_ms<b)];test=s[s.entry_ms>=b]
 if min(len(train),len(val),len(test))==0:raise ValueError('empty purged partition')
 return train.copy(),val.copy(),test.copy()

def evaluate(labels,probabilities):
 y=np.asarray(labels,dtype=int);p=np.asarray(probabilities,dtype=float)
 if len(y)==0 or len(y)!=len(p) or not np.isin(y,[0,1]).all() or not np.isfinite(p).all() or (p<0).any() or (p>1).any():raise ValueError('invalid predictions')
 eps=1e-15;q=np.clip(p,eps,1-eps);bins=[]
 for i in range(10):
  mask=(p>=i/10)&((p<(i+1)/10) if i<9 else (p<=1))
  if mask.any():bins.append({'lower':i/10,'upper':(i+1)/10,'n':int(mask.sum()),'mean_probability':float(p[mask].mean()),'observed_rate':float(y[mask].mean())})
 return {'n':len(y),'accuracy':float(np.mean((p>=.5)==y)),'brier':float(np.mean((p-y)**2)),'log_loss':float(-np.mean(y*np.log(q)+(1-y)*np.log(1-q))),'ece':float(sum(b['n']/len(y)*abs(b['mean_probability']-b['observed_rate']) for b in bins)),'calibration_bins':bins}

def sensitivity(labels,probabilities,ratios):
 y=np.asarray(labels,dtype=int);p=np.asarray(probabilities,dtype=float);evaluate(y,p);won=(p>=.5)==y;result=[]
 for ratio in ratios:
  if ratio<=0:raise ValueError('payout ratio must be positive')
  result.append({'assumed_profit_ratio':ratio,'breakeven_win_rate':1/(1+ratio),'direction_win_rate':float(won.mean()),'hypothetical_net_units_per_stake':float(np.mean(np.where(won,ratio,-1)))})
 return result

def align_enhanced(samples,observations,max_age_ms):
 if 'decision_ms' not in samples or 'entry_ms' not in samples or samples[['decision_ms','entry_ms']].isna().any().any() or not (samples.decision_ms<samples.entry_ms).all():raise ValueError('decision_ms must precede entry_ms')
 if 'feature_end_ms' in samples and not (samples.feature_end_ms<samples.decision_ms).all():raise ValueError('features must precede decision')
 values=[]
 for t in samples.decision_ms:
  candidates=observations[(observations.event_ms<=t)&(observations.available_ms<=t)&(observations.event_ms>=t-max_age_ms)].sort_values(['event_ms','available_ms'])
  values.append(float(candidates.iloc[-1].value) if len(candidates) else np.nan)
 return pd.Series(values,index=samples.index)
