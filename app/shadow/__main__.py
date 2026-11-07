"""Local shadow process control; no order execution."""
import argparse,json,time
from pathlib import Path
from .runner import run,validate_config,request_safe_stop
from .stats import dashboard,RESEARCH_BREAK_EVEN,RESEARCH_THRESHOLD
from .store import ShadowStore
from .status import health_overview,runner_active


def main():
 parser=argparse.ArgumentParser(description='BTCUSDT read-only shadow research')
 sub=parser.add_subparsers(dest='command',required=True)
 start=sub.add_parser('run');start.add_argument('--config',required=True,type=Path);start.add_argument('--duration-seconds',type=int)
 status=sub.add_parser('status');status.add_argument('--directory',required=True,type=Path);status.add_argument('--max-health-age-ms',type=int,default=180000);status.add_argument('--index-max-age-ms',type=int,default=5000)
 stop=sub.add_parser('stop');stop.add_argument('--directory',required=True,type=Path)
 args=parser.parse_args()
 if args.command=='run':
  config=json.loads(args.config.read_text());validate_config(config);collector=json.loads(Path(config['collector_config_path']).expanduser().read_text());run(config,collector,args.duration_seconds)
 elif args.command=='stop':
  request_safe_stop(args.directory);print('Safe stop requested; pending results remain pending until recovery.')
 else:
  path=args.directory/'shadow.sqlite'
  if not path.exists():raise SystemExit('No shadow database found')
  store=ShadowStore(path)
  try:
   now=time.time_ns()//1000000;rows=store.predictions();health=[dict(r) for r in store.db.execute('SELECT * FROM health ORDER BY source')]
   result={'timestamp_ms':now,'research_payout_ratio':'0.85','breakeven_probability':str(RESEARCH_BREAK_EVEN),'research_threshold':str(RESEARCH_THRESHOLD),'live_quote_verified':False,'execution_decision':'NO TRADE','runner_active':runner_active(args.directory),'health':health_overview(health,now,args.max_health_age_ms,args.index_max_age_ms),'unresolved_gaps':store.unresolved_gaps(),'last_7_days':dashboard(rows,now,7*86400000),'last_30_days':dashboard(rows,now,30*86400000)}
   print(json.dumps(result,ensure_ascii=False,indent=2))
  finally:store.close()
if __name__=='__main__':main()
