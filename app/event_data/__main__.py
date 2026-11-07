import argparse,json
from pathlib import Path
from .storage import Store
from .collect import collect_index_history,collect_rest_snapshot,collect_openmarket,collect_websocket,load_token,backfill_openmarket
from .transport import ReadOnlyMCP
from .datasets import export_dataset,export_series

def main():
 p=argparse.ArgumentParser(description='Read-only BTC data ingestion')
 p.add_argument('--config',required=True,type=Path)
 sub=p.add_subparsers(dest='cmd',required=True)
 sub.add_parser('init');h=sub.add_parser('history');h.add_argument('--interval',required=True,choices=['1m','5m','15m','30m','1h','4h']);h.add_argument('--start-ms',required=True,type=int);h.add_argument('--end-ms',required=True,type=int)
 sub.add_parser('index-snapshot');sub.add_parser('openmarket-snapshot');sub.add_parser('index-stream')
 om=sub.add_parser('openmarket-history');om.add_argument('--market',choices=['BINANCE','BINANCE_FUTURES'],required=True);om.add_argument('--interval',choices=['MINUTE','FIVE_MINUTES','FIFTEEN_MINUTES','THIRTY_MINUTES','HOUR','FOUR_HOURS'],required=True);om.add_argument('--start-ms',type=int,required=True);om.add_argument('--end-ms',type=int,required=True);om.add_argument('--window-ms',type=int,required=True)
 om.add_argument('--type',default='TRADE_SIDE_AGNOSTIC_AGG',choices=['TRADE_SIDE_AGNOSTIC_AGG','TRADE_AGG','OPEN_INTEREST_AGG','FUNDING_RATE_AGG','LIQUIDATION_AGG']);om.add_argument('--side',choices=['BUY','SELL'])
 d=sub.add_parser('dataset');d.add_argument('--decision-ms',required=True,type=int);d.add_argument('--horizon',choices=['30m','1h'],required=True);d.add_argument('--enhanced',action='store_true')
 ds=sub.add_parser('dataset-series');ds.add_argument('--start-ms',type=int,required=True);ds.add_argument('--end-ms',type=int,required=True);ds.add_argument('--step-ms',type=int,required=True);ds.add_argument('--enhanced',action='store_true')
 a=p.parse_args(); c=json.loads(a.config.read_text());store=Store(Path(c['database_path']).expanduser())
 try:
  if a.cmd=='init':print('database initialized')
  elif a.cmd=='history':print(json.dumps({'next_ms':collect_index_history(store,c,a.interval,a.start_ms,a.end_ms)}))
  elif a.cmd=='index-snapshot':print(json.dumps(collect_rest_snapshot(store,c)))
  elif a.cmd=='openmarket-snapshot':
   ok,total=collect_openmarket(store,c,load_token(c['openmarket_credentials_path']));print(json.dumps({'successful':ok,'total':total}));
   if ok!=total:raise SystemExit(1)
  elif a.cmd=='openmarket-history':
   client=ReadOnlyMCP(c['openmarket_mcp'],load_token(c['openmarket_credentials_path']),timeout=c['timeout_seconds'],retries=c['read_retries']);client.initialise()
   print(json.dumps({'next_ms':backfill_openmarket(store,client,a.market,a.interval,a.start_ms,a.end_ms,a.window_ms,kind=a.type,side=a.side)}))
  elif a.cmd=='index-stream':collect_websocket(store,c)
  elif a.cmd=='dataset':print(json.dumps(export_dataset(store,a.decision_ms,a.horizon,a.enhanced,interval=c['openmarket_interval'],max_age_ms=c['dataset_max_age_ms'],required_metrics=c['required_feature_kinds'],metric_max_age_ms=c.get('metric_max_age_ms'))))
  elif a.cmd=='dataset-series':
   for row in export_series(store,a.start_ms,a.end_ms,a.step_ms,enhanced=a.enhanced,interval=c['openmarket_interval'],max_age_ms=c['dataset_max_age_ms'],required_metrics=c['required_feature_kinds'],metric_max_age_ms=c.get('metric_max_age_ms')):print(json.dumps(row))
 finally:store.close()
if __name__=='__main__':main()
