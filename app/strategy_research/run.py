"""Prespecified walk-forward comparisons; no orders and no test-set selection."""
import argparse,hashlib,json
from datetime import datetime,timezone
from pathlib import Path
import numpy as np
import pandas as pd
from sklearn.linear_model import LogisticRegression
from strategy_mvp.core import FEATURES,align_enhanced,build_features,build_samples,evaluate,load_index
from strategy_mvp.model import _logit,make_models,read_observations
from .study import DAY,THRESHOLDS,walk_windows,screen_gate,summarize,compare_days,signal,_day_ci


def iso(ms):return datetime.fromtimestamp(int(ms)/1000,timezone.utc).isoformat()

def walk_predictions(samples,model_name,columns=FEATURES):
 frames=[];folds=[]
 for index,(train,cal,screen,test) in enumerate(walk_windows(samples)):
  for frame in (train,cal,screen,test):
   if not ((frame.feature_end_ms<frame.decision_ms)&(frame.decision_ms<frame.entry_ms)).all():raise ValueError('invalid decision time')
  if any(x.label.nunique()!=2 for x in (train,cal,screen,test)):
   folds.append({'fold':index,'status':'skipped_one_class'});continue
  model=make_models()[model_name];model.fit(train[columns],train.label)
  cal_p=model.predict_proba(cal[columns])[:,1]
  calibrator=LogisticRegression(C=1.0,random_state=17).fit(_logit(cal_p),cal.label)
  ps=calibrator.predict_proba(_logit(model.predict_proba(screen[columns])[:,1]))[:,1]
  pt=calibrator.predict_proba(_logit(model.predict_proba(test[columns])[:,1]))[:,1]
  gate=screen_gate(screen.label,ps)
  subset=test[['entry_ms','decision_ms','exit_ms','label','rv30','roc_60']].copy();subset['probability']=pt;subset['correct']=(pt>=.5)==subset.label;subset['screen_passed']=gate['passed'];subset['fold']=index
  low,high=train.rv30.quantile([.33,.67]);down,up=train.roc_60.quantile([.33,.67])
  subset['volatility']=np.where(subset.rv30<=low,'low',np.where(subset.rv30<=high,'medium','high'))
  subset['trend']=np.where(subset.roc_60<=down,'down',np.where(subset.roc_60<=up,'sideways','up'))
  fold_result={'fold':index,'status':'evaluated','train_n':len(train),'calibration_n':len(cal),'screen_n':len(screen),'test_n':len(test),'test_first_utc':iso(test.entry_ms.min()),'test_last_utc':iso(test.exit_ms.max()),'screen_gate':gate,'screen_strengths':summarize(screen,ps),'test_metrics':evaluate(test.label,pt)}
  folds.append(fold_result);frames.append(subset)
 if not frames:return None,folds
 result=pd.concat(frames,ignore_index=True)
 if result.entry_ms.duplicated().any():raise ValueError('overlapping test windows')
 return result,folds

def summarize_predictions(frame):
 p=frame.probability.to_numpy();y=frame.label.to_numpy();strengths=summarize(frame,p)
 # Margin and payout are only explicitly hypothetical, never used as real quotes.
 assumed=np.array([signal(float(x),.85,.03) for x in p]);allowed=(assumed!='NO TRADE') & frame.screen_passed.to_numpy()
 sensitive={}
 for ratio in (.70,.80,.85):
  mask=np.array([signal(float(x),ratio,.03)!='NO TRADE' for x in p]) & frame.screen_passed.to_numpy();wins=frame.correct.to_numpy()[mask];count=int(mask.sum())
  sensitive[str(ratio)]={'assumed_payout_ratio':ratio,'margin_probability_points':.03,'signals':count,'coverage':count/len(frame),'distinct_days':int((frame.entry_ms[mask]//DAY).nunique()),'distinct_folds':int(frame.fold[mask].nunique()),'accuracy':float(wins.mean()) if count else None,'day_block_ci':_day_ci(frame.entry_ms.to_numpy()[mask],wins) if count else None,'hypothetical_net_per_stake':float(np.where(wins,ratio,-1).mean()) if count else None}
 regimes={}
 for col in ('volatility','trend'):
  regimes[col]={}
  for state,subset in frame.groupby(col):
   fstats=[]
   for fold,part in subset.groupby('fold'):
    fstats.append({'fold':int(fold),'n':len(part),'accuracy':float(part.correct.mean())})
   regimes[col][state]={'n':len(subset),'accuracy':float(subset.correct.mean()),'day_block_ci':_day_ci(subset.entry_ms,subset.correct),'folds':fstats}
 return {'n':len(frame),'metrics':evaluate(y,p),'strengths':strengths,'screen_passed_candidates':int(frame.screen_passed.sum()),'no_verified_live_odds_decision':'NO TRADE','assumed_85_percent_gate_signals':int(allowed.sum()),'hypothetical_payout_sensitivity':sensitive,'regimes':regimes}

def run(index_path,output,db_path=None):
 raw=Path(index_path).read_bytes();bars=load_index(index_path);features=build_features(bars);out=Path(output);out.mkdir(parents=True,exist_ok=True)
 meta={'sha256':hashlib.sha256(raw).hexdigest(),'bars':len(bars),'first_utc':iso(bars.open_ms.min()),'last_utc':iso(bars.open_ms.max()),'method':'30d training / 4d calibration / 3d screening / 7d test, step 7d; outcomes purged at every boundary; fixed rules','thresholds':list(THRESHOLDS),'price_availability':'retrospective backfill; feature close ~59s before decision, decision 1s before simulated minute-open entry; actual publication time unknown','scope':'these data were examined in earlier research, so walk-forward is retrospective and not an untouched independent external holdout','real_payout':'unavailable; decisions NO TRADE','seed':17}
 for horizon in (30,60):
  mechanisms={'fixed':build_samples(bars,horizon,features=features),'rolling':build_samples(bars,horizon,entry_spacing_minutes=1,features=features)}
  result={'horizon_minutes':horizon,'metadata':meta,'mechanisms':{},'comparison':{},'openmarket':{}}
  forecasts={}
  for mechanism,samples in mechanisms.items():
   result['mechanisms'][mechanism]={}
   for name in ('logistic_regression','lightgbm'):
    pred,folds=walk_predictions(samples,name)
    if pred is None:result['mechanisms'][mechanism][name]={'status':'unavailable','folds':folds};continue
    forecasts[(mechanism,name)]=pred
    result['mechanisms'][mechanism][name]={'status':'evaluated','folds':folds,'pooled':summarize_predictions(pred)}
  for name in ('logistic_regression','lightgbm'):
   if ('fixed',name) in forecasts and ('rolling',name) in forecasts:result['comparison'][name]=compare_days(forecasts[('fixed',name)],forecasts[('rolling',name)])
  if not db_path:result['openmarket']={'status':'unavailable','reason':'no database'}
  else:
   obs=read_observations(db_path);s=mechanisms['fixed'];late=obs.empty or obs.available_ms.min()>s.decision_ms.max() or obs.event_ms.min()>s.decision_ms.max()
   if late:result['openmarket']={'status':'unavailable','source_observations':len(obs),'common_samples':0,'reason':'no observation actually received before any decision in this historical range'}
   else:
    joined=s.copy();joined['cvd']=align_enhanced(joined,obs,300000);joined=joined.dropna(subset=['cvd'])
    result['openmarket']={'status':'unavailable','source_observations':len(obs),'common_samples':len(joined),'reason':'fewer than 500 verified common samples'}
    if len(joined)>=500:
     result['openmarket']={'status':'evaluated','source_observations':len(obs),'common_samples':len(joined),'models':{}}
     for name in ('logistic_regression','lightgbm'):
      base,_=walk_predictions(joined,name);enh,_=walk_predictions(joined,name,FEATURES+['cvd'])
      if base is None or enh is None:result['openmarket']['models'][name]={'status':'unavailable','reason':'insufficient chronological fold coverage'}
      else:result['openmarket']['models'][name]={'price_only':summarize_predictions(base),'price_plus_cvd':summarize_predictions(enh)}
  (out/f'selective-{horizon}m.json').write_text(json.dumps(result,ensure_ascii=False,indent=2,allow_nan=False)+'\n')
  lines=[f'# BTC {horizon}M：固定与滚动入场回顾性研究','', '历史分钟 K 线是假设可用时间的回填数据；它们已参与上一轮研究，以下 Walk-Forward 不构成从未看过的外部独立样本，也不是真实事件合约结算。','', '| 机制 | 模型 | 样本外候选 | 方向准确率 | Brier | 校准筛选通过的候选 |','|---|---|---:|---:|---:|---:|']
  for mechanism,models in result['mechanisms'].items():
   for name,value in models.items():
    if value['status']=='evaluated':
     x=value['pooled'];lines.append(f"| {mechanism} | {name} | {x['n']} | {x['metrics']['accuracy']:.2%} | {x['metrics']['brier']:.4f} | {x['screen_passed_candidates']} |")
  lines+=['','## 不同信号强度','', '阈值为模型概率较大一侧的下限，均为预先固定的描述性结果；候选数包含重叠到期窗口，不能按独立交易计算置信度。没有可靠实时赔率时，实际决策均为 NO TRADE。','', '| 机制 | 模型 | 阈值 | 候选数 | 覆盖率 | 准确率 | 按日分块 95% 区间 |','|---|---|---:|---:|---:|---:|---|']
  for mechanism,models in result['mechanisms'].items():
   for name,value in models.items():
    if value['status']!='evaluated':continue
    for threshold,v in value['pooled']['strengths'].items():
     accuracy=f"{v['accuracy']:.2%}" if v['accuracy'] is not None else '无样本';ci=v['accuracy_ci'];ci_text=f'{ci[0]:.2%}–{ci[1]:.2%}' if ci else '不足';lines.append(f"| {mechanism} | {name} | {threshold} | {v['signals']} | {v['coverage']:.2%} | {accuracy} | {ci_text} |")
  lines+=['','## 固定与滚动的配对比较','', '日均方向准确率之差（滚动减固定）与按日 bootstrap 区间；同时比较多个模型及条件属探索性分析。','']
  for name,c in result['comparison'].items():
   ci=c['paired_day_ci'];lines.append(f"- {name}：共同 {c['common_days']} 天，差值 {c['difference_rolling_minus_fixed']:.2%}，区间 {ci[0]:.2%}–{ci[1]:.2%}。" if ci else f"- {name}：共同日数不足。")
  lines+=['','## 假设赔率的敏感性','', '仅供敏感性分析：假设每笔净收益比率为 85%、安全边际 3 个百分点，且仅在先前筛选段通过时计数。每分钟候选会相互重叠；少量日期或单一折不能证明统计优势。','', '| 机制 | 模型 | 假设候选数 | 覆盖率 | 日期数 | 验证折数 | 准确率 | 日块 95% 区间 |','|---|---|---:|---:|---:|---:|---:|---|']
  for mechanism,models in result['mechanisms'].items():
   for name,value in models.items():
    if value['status']!='evaluated':continue
    v=value['pooled']['hypothetical_payout_sensitivity']['0.85'];acc=f"{v['accuracy']:.2%}" if v['accuracy'] is not None else '无样本'
    ci=v['day_block_ci'];ci_text=f'{ci[0]:.2%}–{ci[1]:.2%}' if ci else '不足'
    lines.append(f"| {mechanism} | {name} | {v['signals']} | {v['coverage']:.2%} | {v['distinct_days']} | {v['distinct_folds']} | {acc} | {ci_text} |")
  lines+=['','## 资金流、条件和赔率','',f"OpenMarket 同窗口对照：{result['openmarket']['status']}，共同样本 {result['openmarket'].get('common_samples',0)}。",'','各状态逐折样本数、方向准确率及假设赔率敏感性见 JSON；模型及规则没有按这些结果在测试集上挑选。价格、赔率和时钟限制详见方法说明。']
  (out/f'selective-{horizon}m.md').write_text('\n'.join(lines)+'\n')
  print(horizon,'completed',flush=True)

def main():
 p=argparse.ArgumentParser();p.add_argument('--index-jsonl',required=True);p.add_argument('--output',required=True);p.add_argument('--openmarket-sqlite');a=p.parse_args();run(a.index_jsonl,a.output,a.openmarket_sqlite)
if __name__=='__main__':main()
