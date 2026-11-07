"""Descriptive proxy statistics; no statement of real profitability."""
import math
from decimal import Decimal

RESEARCH_PAYOUT=Decimal('0.85')
RESEARCH_MARGIN=Decimal('0.03')
RESEARCH_BREAK_EVEN=Decimal(1)/(Decimal(1)+RESEARCH_PAYOUT)
RESEARCH_THRESHOLD=RESEARCH_BREAK_EVEN+RESEARCH_MARGIN

def _wilson(wins,n):
 if not n:return None
 z=1.959963984540054;p=wins/n;d=1+z*z/n;center=(p+z*z/(2*n))/d;half=z*math.sqrt(p*(1-p)/n+z*z/(4*n*n))/d
 return [max(0,center-half),min(1,center+half)]

def dashboard(rows,now_ms,window_ms):
 if window_ms<=0:raise ValueError('invalid window')
 groups={f'BTCUSDT:{h}:{m}':[] for h in (30,60) for m in ('FIXED','ROLLING')}
 for raw in rows:
  r=dict(raw)
  if r.get('classification')!='shadow_v1' or r.get('symbol')!='BTCUSDT' or not (now_ms-window_ms<=r['decision_ms']<=now_ms):continue
  key=f"{r['symbol']}:{r['horizon_minutes']}:{r['mechanism']}"
  if key in groups:groups[key].append(r)
 out={}
 for key,items in groups.items():
  if not items:
   out[key]={'coverage_start_ms':None,'coverage_end_ms':None,'predictions':0,'expired':0,'verified':0,'scored':0,'missing':0,'missing_rate':None,'accuracy':None,'direction':{},'calibration_bins':[],'ece':None,'brier':None,'screen_passed_count':0,'nonoverlap_candidates':0,'nonoverlap_count':0,'overlap_fraction':None,'nonoverlap_accuracy':None,'nonoverlap_wilson_95_descriptive':None,'hypothetical_85pct':{'payout_ratio':'0.85','probability_margin':'0.03','breakeven_probability':str(RESEARCH_BREAK_EVEN),'threshold':str(RESEARCH_THRESHOLD),'selected_candidates':0,'independent_candidates':0,'independent_selected':0,'independent_accuracy':None,'independent_wilson_95_descriptive':None,'hypothetical_net_per_stake':None,'qualification':'assumption only; no actual quote, fees or contract settlement'},'execution_decision':'NO TRADE','uncertainty_note':'No observations.'}
   continue
  items.sort(key=lambda x:x['entry_ms']);settled=[r for r in items if r['result_status'] in ('VERIFIED','MISSING')];verified=[r for r in settled if r['result_status']=='VERIFIED'];scored=[r for r in verified if r.get('direction_result') in ('HIGHER','LOWER')]
  wins=[r for r in scored if r['candidate_direction']==r['direction_result']];selected=[r for r in items if r['screen_passed'] and Decimal(str(max(r['probability_up'],1-r['probability_up'])))>RESEARCH_THRESHOLD]
  def nonoverlap(candidates):
   chosen=[];last_expiry=None
   for r in candidates:
    if last_expiry is None or r['entry_ms']>=last_expiry:chosen.append(r);last_expiry=r['expiry_ms']
   return chosen
  independent_candidates=nonoverlap(items);selected_candidates=nonoverlap(selected)
  independent=[r for r in independent_candidates if r['result_status']=='VERIFIED' and r.get('direction_result') in ('HIGHER','LOWER')]
  independent_selected=[r for r in selected_candidates if r['result_status']=='VERIFIED' and r.get('direction_result') in ('HIGHER','LOWER')]
  iw=sum(r['candidate_direction']==r['direction_result'] for r in independent);sw=sum(r['candidate_direction']==r['direction_result'] for r in independent_selected)
  bins=[]
  for i in range(10):
   b=[r for r in scored if i/10<=r['probability_up'] and (r['probability_up']<(i+1)/10 if i<9 else r['probability_up']<=1)]
   if b:bins.append({'lower':i/10,'upper':(i+1)/10,'n':len(b),'mean_probability':sum(r['probability_up'] for r in b)/len(b),'up_rate':sum(r['direction_result']=='HIGHER' for r in b)/len(b)})
  direction={}
  for side in ('HIGHER','LOWER'):
   subset=[r for r in scored if r['candidate_direction']==side];direction[side]={'n':len(subset),'correct':sum(r['direction_result']==side for r in subset),'accuracy':sum(r['direction_result']==side for r in subset)/len(subset) if subset else None}
  ece=sum(b['n']/len(scored)*abs(b['mean_probability']-b['up_rate']) for b in bins) if scored else None
  out[key]={'coverage_start_ms':min(r['decision_ms'] for r in items),'coverage_end_ms':max(r['decision_ms'] for r in items),'predictions':len(items),'expired':len(settled),'verified':len(verified),'scored':len(scored),'missing':sum(r['result_status']=='MISSING' for r in settled),'missing_rate':sum(r['result_status']=='MISSING' for r in settled)/len(settled) if settled else None,'accuracy':len(wins)/len(scored) if scored else None,'direction':direction,'calibration_bins':bins,'ece':ece,'brier':sum((r['probability_up']-(r['direction_result']=='HIGHER'))**2 for r in scored)/len(scored) if scored else None,'screen_passed_count':sum(bool(r['screen_passed']) for r in items),'nonoverlap_candidates':len(independent_candidates),'nonoverlap_count':len(independent),'overlap_fraction':1-len(independent_candidates)/len(items) if items else None,'nonoverlap_accuracy':iw/len(independent) if independent else None,'nonoverlap_wilson_95_descriptive':_wilson(iw,len(independent)),'hypothetical_85pct':{'payout_ratio':'0.85','probability_margin':'0.03','breakeven_probability':str(RESEARCH_BREAK_EVEN),'threshold':str(RESEARCH_THRESHOLD),'selected_candidates':len(selected),'independent_candidates':len(selected_candidates),'independent_selected':len(independent_selected),'independent_accuracy':sw/len(independent_selected) if independent_selected else None,'independent_wilson_95_descriptive':_wilson(sw,len(independent_selected)),'hypothetical_net_per_stake':float((Decimal(sw)*RESEARCH_PAYOUT-Decimal(len(independent_selected)-sw))/Decimal(len(independent_selected))) if independent_selected else None,'qualification':'assumption only; no actual quote, fees or contract settlement'},'execution_decision':'NO TRADE','uncertainty_note':'Wilson interval is descriptive; non-overlap does not guarantee independence; no simultaneous multiple-testing adjustment.'}
 return out
