"""Generate reproducible offline reports; consumes only local files."""
import argparse
from datetime import datetime,timezone
import hashlib,json,platform
from pathlib import Path
import numpy as np
import pandas as pd
import sklearn
import lightgbm
from .core import FEATURES,load_index,build_samples,purged_split
from .model import evaluate_model,make_models,enhanced_comparison,baseline_results


def iso(ms):return datetime.fromtimestamp(int(ms)/1000,timezone.utc).isoformat()
def partition(frame):
 return {'n':len(frame),'first_entry_utc':iso(frame.entry_ms.min()),'last_entry_utc':iso(frame.entry_ms.max()),'last_exit_utc':iso(frame.exit_ms.max()),'up_rate':float(frame.label.mean())}

def run(index_path,output,db_path=None):
 path=Path(index_path);raw=path.read_bytes();bars=load_index(path);output=Path(output);output.mkdir(parents=True,exist_ok=True)
 info={'source':'Binance BTCUSDT index 1m OHLC historical backfill','sha256':hashlib.sha256(raw).hexdigest(),'bars':len(bars),'first_open_utc':iso(bars.open_ms.min()),'last_open_utc':iso(bars.open_ms.max()),'history_availability':'retrospective bars; original point-in-time publication/revision timestamps unavailable','assumption':'decision 1 second before entry; last feature candle closed about 59 seconds earlier; actual historic publication time unverified','fill':'entry and exit equal minute index bar open; zero spread and fees; not actual event contract settlement','equal_price_handling':'equal index prices excluded; actual contract tie rule unverified','split':'chronological 60/20/20; labels purged at boundaries; validation-only Platt calibration; no test-set tuning','feature_columns':FEATURES,'seed':17,'python':platform.python_version(),'numpy':np.__version__,'pandas':pd.__version__,'scikit_learn':sklearn.__version__,'lightgbm':lightgbm.__version__}
 outputs={}
 for horizon in (30,60):
  samples=build_samples(bars,horizon);train,val,test=purged_split(samples)
  result={'horizon_minutes':horizon,'source':info,'total_eligible':len(samples),'partitions':{'train':partition(train),'validation':partition(val),'test':partition(test)},'models':{}}
  for name,model in make_models().items():result['models'][name]=evaluate_model(model,train,val,test)
  result['baselines']=baseline_results(test.label)
  result['enhanced_comparison']=enhanced_comparison(samples,db_path)
  (output/f'report-{horizon}m.json').write_text(json.dumps(result,ensure_ascii=False,indent=2,allow_nan=False)+'\n')
  lines=[f'# BTC {horizon} 分钟：离线指数近似研究','',f"样本：总计 {len(samples)}，训练 {len(train)}、校准 {len(val)}、样本外 {len(test)}。",f"样本外区间：{iso(test.entry_ms.min())} 至 {iso(test.exit_ms.max())}。",'', '价格和标签来自分钟指数开盘价近似，假设决策在入场前 1 秒，最后一根特征 K 线约在决策前 59 秒收盘；历史实际发布和修订时间无法验证。这不是事件合约结算，也不是实盘收益。', '', '| 模型 | 样本外方向准确率 | Brier | Log loss | ECE | AUC | 日块 bootstrap 准确率 95% 区间 |', '|---|---:|---:|---:|---:|---:|---:|']
  for name,model in result['models'].items():
   x=model['calibrated'];ci=model['accuracy_day_block_bootstrap_95'];ci_text=f'{ci[0]:.2%}–{ci[1]:.2%}' if ci else '样本天数不足';lines.append(f"| {name} | {x['accuracy']:.2%} | {x['brier']:.4f} | {x['log_loss']:.4f} | {x['ece']:.4f} | {model['auc']:.4f} | {ci_text} |")
  lines+=['','## 恒定基准与方向分布','', '50% 常数概率只作为概率基准，方向平局按 HIGHER 计数；恒涨和恒跌的概率为 1 和 0。','', '| 基准 | 方向准确率 | Brier |','|---|---:|---:|']
  for name,base in result['baselines'].items():lines.append(f"| {name} | {base['accuracy']:.2%} | {base['brier']:.4f} |")
  lines+=['','| 模型 | 阶段 | 预测上涨占比 | 预测下跌占比 | 概率均值 | 概率标准差 |','|---|---|---:|---:|---:|---:|']
  for name,model in result['models'].items():
   for stage,d in model['probability_distribution'].items():lines.append(f"| {name} | {stage} | {d['higher_fraction']:.2%} | {d['lower_fraction']:.2%} | {d['mean']:.4f} | {d['std']:.4f} |")
  lines+=['','## 支付比率假设敏感性','', '以每笔投入 1 单位、赢时净收益为假设比率、输时损失 1 单位计算；忽略费用，未确认实盘赔率、交易规则和结算价格。仅供敏感性比较，不是盈利回测。','', '| 模型 | 假设净收益比率 | 盈亏平衡胜率 | 假设每笔净收益 |','|---|---:|---:|---:|']
  for name,model in result['models'].items():
   for v in model['hypothetical_payout_sensitivity']:lines.append(f"| {name} | {v['assumed_profit_ratio']:.0%} | {v['breakeven_win_rate']:.2%} | {v['hypothetical_net_units_per_stake']:.4f} |")
  lines+=['','## 样本外市场状态','', '分组边界仅使用训练集分位数；每组样本较少，不能作为独立发现。','', '| 模型 | 状态 | 样本 | 方向准确率 | Brier |','|---|---|---:|---:|---:|']
  for name,model in result['models'].items():
   for regime,stats in model['regimes'].items():lines.append(f"| {name} | {regime} | {stats['n']} | {stats['accuracy']:.2%} | {stats['brier']:.4f} |")
  observed=any(m['accuracy_day_block_bootstrap_95'] and m['accuracy_day_block_bootstrap_95'][0]>.5 for m in result['models'].values())
  finding='至少一个模型的描述性日块区间下界超过 50%，仍需独立复验。' if observed else '本次未观察到稳健的方向统计优势。'
  lines+=['','## 校准和市场状态','', '逐概率桶校准、未校准和校准后的概率误差，以及按训练集阈值定义的趋势和波动率分组结果，见同目录 JSON。分组样本较少，仅作描述。','', '## OpenMarket 增强模型','',f"状态：{result['enhanced_comparison']['status']}；真实可用的共同样本：{result['enhanced_comparison'].get('observable_common_samples',0)}。仅在同一决策时刻实际已接收、来源已验证且未过期的数据达到最低样本数时，才比较同一窗口上的纯价格与价格+CVD 模型。",'', '## 可观察优势','',finding+' 行情存在时序相关和单次样本外选择风险；即使方向准确率高于 50%，也不能据此认定达到假设赔率的盈亏平衡或具有可交易优势。','', '相等价格样本被排除，实际事件合约平局规则尚未核实。']
  (output/f'report-{horizon}m.md').write_text('\n'.join(lines)+'\n');outputs[horizon]=result
 return outputs

def main():
 p=argparse.ArgumentParser();p.add_argument('--index-jsonl',required=True);p.add_argument('--output',required=True);p.add_argument('--openmarket-sqlite');args=p.parse_args()
 results=run(args.index_jsonl,args.output,args.openmarket_sqlite)
 for h,r in results.items():print(h,'minutes:',{name:round(m['calibrated']['accuracy'],4) for name,m in r['models'].items()},'enhanced:',r['enhanced_comparison']['status'],r['enhanced_comparison'].get('observable_common_samples'))
if __name__=='__main__':main()
