"""Compact loopback web terminal for BTC SHADOW and separately armed live control."""
from __future__ import annotations

import argparse
import json
import os
import sqlite3
import time
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from collections import defaultdict
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, quote, urlparse

from .live_api import LiveAPIError, change_live_state_from_site, live_public_origin, read_live_state
from .live_stats import read_live_statistics_for_site
from .stats import RESEARCH_THRESHOLD, dashboard
from .status import health_overview, runner_active

INTERVAL_MS = {
    "1m": 60_000,
    "5m": 300_000,
    "15m": 900_000,
    "30m": 1_800_000,
    "1h": 3_600_000,
}
PREDICTION_COLUMNS = (
    "id,symbol,mechanism,horizon_minutes,decision_ms,recorded_ms,entry_ms,"
    "expiry_ms,screen_passed,probability_up,candidate_direction,"
    "execution_decision,result_status,direction_result,entry_price,exit_price,"
    "entry_event_ms,exit_event_ms,classification"
)
HTML = r"""<!doctype html>
<html lang="zh-CN">
<head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<meta name="theme-color" content="#111217"><title>BTC · 事件合约</title>
<link rel="stylesheet" href="/static/dashboard.css">
<link rel="stylesheet" href="/static/dashboard-mobile.css">
<link rel="stylesheet" href="/static/dashboard-pages.css">
</head>
<body>
<div class="shell">
<header class="top">
 <div class="brand"><div><h1>BTC</h1><div class="sub">事件合约</div></div></div>
 <div class="topright"><span id="runner-pill" class="pill"><i class="dot"></i> 正在检查</span><span id="updated">等待数据</span></div>
</header>
<nav class="section-nav" aria-label="观察台页面">
 <a href="/" data-nav="overview">行情</a>
 <a href="/records" data-nav="records">记录</a>
 <a href="/statistics" data-nav="statistics">统计</a>
 <a href="/live" data-nav="live">交易</a>
</nav>
<main class="layout">
<section class="main">
 <section class="panel" id="records-choice">
  <div class="panel-head"><h2>记录</h2><select id="records-kind" aria-label="记录类型"><option value="chart">布林带</option><option value="scheduled">定时模拟</option><option value="research">研究</option></select></div>
 </section>
 <details class="panel" id="research-details"><summary>研究数据</summary>
 <div class="metrics" id="model-metrics">
  <div class="metric"><div class="metric-label">近 7 天 · 假设盈亏</div><div id="pnl7" class="metric-value">—</div><div class="metric-foot">每笔虚拟 1U · 85% 研究赔率</div></div>
  <div class="metric"><div class="metric-label">近 30 天 · 假设盈亏</div><div id="pnl30" class="metric-value">—</div><div class="metric-foot">不代表真实交易收益</div></div>
  <div class="metric"><div class="metric-label">近 30 天 · 代理方向准确率</div><div id="accuracy" class="metric-value">—</div><div id="accuracy-foot" class="metric-foot">等待已验证样本</div></div>
  <div class="metric"><div class="metric-label">近 30 天 · 正式候选</div><div id="samples" class="metric-value">—</div><div id="samples-foot" class="metric-foot">包含待结算候选</div></div>
 </div>
 </details>
 <section class="panel" id="chart-panel">
  <div class="panel-head">
   <div><h2>BTCUSDT 指数</h2><div id="chart-source" class="hint">北京时间 · 读取中</div></div>
   <div class="controls">
    <select id="interval" aria-label="K线周期"><option>1m</option><option>5m</option><option>15m</option><option>30m</option><option>1h</option></select>
    <select id="horizon" aria-label="预测周期"><option value="all">全部周期</option><option value="30">30M</option><option value="60">1H</option></select>
    <select id="mechanism" aria-label="策略模式"><option value="all">全部信号</option><option value="FIXED">固定</option><option value="ROLLING">滚动</option><option value="CHART_30M" selected>布林带</option></select>
   </div>
  </div>
  <div class="chart-wrap"><div id="chart" role="img" aria-label="BTC 指数 K 线和模拟信号"></div></div>
 </section>
 <section class="panel" id="scheduled-paper-panel">
  <div class="panel-head"><h2>定时模拟</h2><span class="tag">不下单</span></div>
  <div class="controls"><span>30分钟</span><select id="scheduled-direction-30"><option value="HIGHER">看涨</option><option value="LOWER">看跌</option></select><input id="scheduled-stake-30" type="number" min="0.01" step="0.01" placeholder="输入模拟金额"><button id="scheduled-start-30">启用30分钟模拟</button><button id="scheduled-stop-30">停止30分钟模拟</button><span id="scheduled-state-30">未启用</span></div>
  <div class="controls"><span>1小时</span><select id="scheduled-direction-60"><option value="HIGHER">看涨</option><option value="LOWER">看跌</option></select><input id="scheduled-stake-60" type="number" min="0.01" step="0.01" placeholder="输入模拟金额"><button id="scheduled-start-60">启用1小时模拟</button><button id="scheduled-stop-60">停止1小时模拟</button><span id="scheduled-state-60">未启用</span></div>
  <div class="hint" id="scheduled-worker">模拟服务检查中</div>
  <div class="table-wrap"><table><thead><tr><th>周期</th><th>方向</th><th>模拟金额</th><th>入场时间</th><th>到期时间</th><th>本根收盘代理</th><th>下根收盘代理</th><th>K线方向</th><th>入场代理价</th><th>到期代理价</th><th>合约代理结果</th></tr></thead><tbody id="scheduled-history"><tr><td colspan="11" class="empty">暂无模拟记录</td></tr></tbody></table></div>
  <button class="records-more" data-more="scheduled-paper-panel" type="button">查看更多定时模拟记录</button>
 </section>
 <section class="panel" id="chart-strategy-panel">
  <div class="panel-head"><h2>布林带信号</h2><span class="tag">指数代理</span></div>
  <div class="table-wrap"><table><thead><tr><th>时间 / 编号</th><th>方向</th><th>触发原因</th><th>请求</th><th>入场指数</th><th>到期指数</th><th>代理结果</th></tr></thead><tbody id="chart-strategy-history"><tr><td colspan="7" class="empty">正在读取…</td></tr></tbody></table></div>
  <button class="records-more" data-more="chart-strategy-panel" type="button">查看更多</button>
 </section>
 <section class="panel" id="model-history-panel">
  <div class="panel-head"><h2>研究信号</h2><div id="history-count" class="hint">—</div></div>
  <div class="table-wrap"><table><thead><tr><th>决策时间</th><th>周期</th><th>策略</th><th>预测方向</th><th>入场代理价 (USDT)</th><th>到期代理价 (USDT)</th><th>概率</th><th>筛选</th><th>代理结果</th><th>执行</th></tr></thead><tbody id="history"><tr><td colspan="10" class="empty">正在读取记录…</td></tr></tbody></table></div>
  <button class="records-more" data-more="model-history-panel" type="button">查看更多研究记录</button>
 </section>
 <section class="panel" id="actual-statistics-panel">
  <div class="panel-head"><div><h2>自动请求</h2><div class="hint" id="actual-statistics-period">统计起点读取中</div></div><span class="tag">指数代理</span></div>
  <div class="hint" id="actual-statistics-status">代理盈亏估算，非币安结算。</div>
  <div class="metrics">
   <div class="metric"><div class="metric-label">自动请求 · 成交未核对</div><div id="actual-request-count" class="metric-value neutral">—</div></div>
   <div class="metric"><div class="metric-label">已有代理结果</div><div id="actual-scored" class="metric-value neutral">—</div></div>
   <div class="metric"><div class="metric-label">指数代理胜率</div><div id="actual-win-rate" class="metric-value neutral">—</div></div>
   <div class="metric"><div class="metric-label">累计代理盈亏估算</div><div id="actual-estimated-pnl" class="metric-value neutral">—</div></div>
  </div>
  <div class="hint" id="actual-statistics-breakdown">结果读取中</div>
  <div class="chart-wrap" id="live-pnl-wrap"><div id="live-pnl-chart" role="img" aria-label="按每笔请求金额计算的累计指数代理盈亏估算曲线"></div></div>
  <div class="table-wrap"><table><thead><tr><th>请求时间</th><th>周期</th><th>方向</th><th>金额</th><th>提交赔付比例</th><th>指数代理结果</th><th>单笔代理盈亏</th><th>累计代理盈亏</th></tr></thead><tbody id="actual-statistics-rows"><tr><td colspan="8" class="empty">正在读取…</td></tr></tbody></table></div>
  <button class="records-more" data-more="actual-statistics-panel" type="button">查看更多</button>
 </section>
 <section class="panel" id="live-trading-panel">
  <div class="panel-head"><h2>自动交易 · 布林带</h2><span id="live-state-tag" class="tag amber">读取中</span></div>
  <input id="live-strategy" type="hidden" value="bb_dual_touch_30m_intrabar_v2">
  <div class="live-form">
   <label for="live-period">周期<select id="live-period"><option value="10">10 分钟</option><option value="30" selected>30 分钟</option><option value="60">1 小时</option></select></label>
   <label for="live-amount">金额（USDT）<input id="live-amount" type="number" inputmode="decimal" min="5" step="0.01" placeholder="至少 5 USDT"></label>
   <label for="live-direction-mode">方向<select id="live-direction-mode"><option value="SIGNAL">按信号</option><option value="REVERSE">反向</option><option value="LONG_ONLY">只做多</option><option value="SHORT_ONLY">只做空</option></select></label>
   <label for="live-cooldown">间隔<select id="live-cooldown"><option value="false">到期后 1 分钟</option><option value="true">到期后 5 分钟</option></select></label>
  </div>
  <div class="notice" id="live-readiness">正在读取交易状态</div>
  <div class="live-actions"><button id="live-enable" type="button" disabled>启用</button><button id="live-disable" type="button" disabled>暂停</button><button id="live-review" type="button" hidden disabled>已核对上一笔</button></div>
  <div class="hint">赔付比例为接口配置值，非实时报价。请求状态与指数代理结果不等于币安成交和结算。</div>
  <div class="table-wrap"><table><thead><tr><th>信号</th><th>请求时间</th><th>方向</th><th>金额</th><th>请求时指数价</th><th>到期指数代理价</th><th>指数代理结果</th><th>请求状态</th></tr></thead><tbody id="live-attempts"><tr><td colspan="8" class="empty">暂无自动请求</td></tr></tbody></table></div>
  <button class="records-more" data-more="live-trading-panel" type="button">查看更多</button>
 </section>
</section>
<aside class="sidebar">
 <details class="panel side-panel" id="system-details">
  <summary><span>系统状态</span><span id="run-state" class="tag">读取中</span></summary>
  <div class="side-section">
   <div class="kv"><span class="key">交易标的</span><span id="symbol" class="val">BTCUSDT</span></div>
   <div class="kv"><span class="key">观察周期</span><span id="horizons" class="val">30M / 1H</span></div>
   <div class="kv"><span class="key">历史研究模式</span><span class="val">FIXED / ROLLING</span></div>
   <div class="kv"><span class="key">执行权限</span><span class="val"><span class="tag amber">NO TRADE</span></span></div>
  </div>
  <div class="side-section">
   <div class="kv"><span class="key">采集检查频率</span><span id="poll" class="val">—</span></div>
   <div class="kv"><span class="key">入场准备提前量</span><span id="lead" class="val">—</span></div>
   <div class="kv"><span class="key">Tick 最大延迟</span><span id="tick-delay" class="val">—</span></div>
   <div class="kv"><span class="key">研究赔率假设</span><span id="payout" class="val">85%</span></div>
   <div class="kv"><span class="key">研究筛选门槛</span><span id="threshold" class="val">57.05%</span></div>
  </div>
  <div class="side-section">
   <div class="kv"><span class="key">数据源健康</span><span id="health-summary" class="val">—</span></div>
   <div id="health" class="health-list"></div>
   <div class="kv"><span class="key">未修复数据缺口</span><span id="gaps" class="val">—</span></div>
  </div>
  <button id="stop" class="stop" disabled>安全停止 SHADOW</button>
  <div class="hint stop-note">仅停止只读采集，不会平仓或改变交易开关。</div>
 </details>
 <details class="panel side-panel" id="model-statistics">
  <summary>研究口径</summary>
  <div class="hint">每笔虚拟 1 U · 85% 假设赔率 · 指数代理结果</div>
  <div class="side-section">
   <div class="kv"><span class="key">30 天已评分样本</span><span id="scored" class="val">—</span></div>
   <div class="kv"><span class="key">30 天待结算</span><span id="pending" class="val">—</span></div>
   <div class="kv"><span class="key">30 天缺失率</span><span id="missing" class="val">—</span></div>
  </div>
 </details>
</aside>
</main>
<div class="footer"><span id="live-footer-status" hidden></span><a href="https://www.tradingview.com/" target="_blank" rel="noopener">TradingView Lightweight Charts™</a></div>
</div>
<div id="toast" class="toast" role="status"></div>
<script src="/static/lightweight-charts-4.2.3.js"></script>
<script>
(function(){
"use strict";
const $=id=>document.getElementById(id);
const page=({"/":"overview","/records":"records","/statistics":"statistics","/live":"live"})[location.pathname]||"overview";
document.body.dataset.view=page;
document.querySelectorAll(".section-nav a").forEach(a=>{if(a.dataset.nav===page)a.setAttribute("aria-current","page");});
if(page==="records")$("mechanism").value="all";
if(page==="records")document.body.dataset.recordKind="chart";
if(page==="records")$("records-kind").addEventListener("change",e=>{document.body.dataset.recordKind=e.target.value;});
if(page==="live"){$("runner-pill").innerHTML='<i class="dot"></i> 正在读取交易开关';$("updated").textContent="等待状态";}
document.querySelectorAll(".records-more").forEach(button=>button.addEventListener("click",()=>{const panel=$(button.dataset.more);panel.classList.toggle("expanded");button.textContent=panel.classList.contains("expanded")?"收起":"查看更多";}));
document.querySelectorAll(".records-more").forEach(button=>{const body=$(button.dataset.more).querySelector("tbody");const update=()=>{button.hidden=body.rows.length<=5;};new MutationObserver(update).observe(body,{childList:true});update();});
let latestOverview=null, latestChart=null, latestHistory=[], chartView=null, chartSeries=null, chartInterval=null, livePnlChart=null, livePnlSeries=null, livePnlPointCount=0;
const esc=v=>String(v===null||v===undefined?"—":v).replace(/[&<>"']/g,c=>({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;","'":"&#39;"}[c]));
const pct=v=>v===null||v===undefined?"—":(Number(v)*100).toFixed(1)+"%";
const fmtTime=v=>v?new Date(Number(v)).toLocaleString("zh-CN",{month:"2-digit",day:"2-digit",hour:"2-digit",minute:"2-digit",second:"2-digit"}):"—";
const fmtNum=v=>v===null||v===undefined?"—":Number(v).toLocaleString("en-US",{maximumFractionDigits:2});
function toast(message){$("toast").textContent=message;$("toast").style.display="block";setTimeout(()=>$("toast").style.display="none",3000);}
async function api(path,options){const r=await fetch(path,Object.assign({cache:"no-store"},options||{}));const data=await r.json();if(!r.ok)throw new Error(data.error||"请求失败");return data;}
function pnlText(v){if(v===null||v===undefined)return "—";const n=Number(v);return (n>0?"+":"")+n.toFixed(2)+"U";}
function setPnl(id,v){const el=$(id);el.textContent=pnlText(v);el.className="metric-value "+(v===null||v===undefined?"neutral":Number(v)>0?"positive":Number(v)<0?"negative":"neutral");}
function renderOverview(data){
 latestOverview=data;
 const active=!!data.runner_active;
 $("runner-pill").innerHTML='<i class="dot '+(active?"ok":"")+'"></i>'+(active?"观察运行中":"观察已停止");
 $("run-state").textContent=active?"运行中":"未运行";$("run-state").className="tag "+(active?"green":"");
 $("stop").disabled=!active;
 $("updated").textContent=fmtTime(data.timestamp_ms);
 const a=data.last_7_days||{},b=data.last_30_days||{};
 setPnl("pnl7",a.hypothetical_pnl_1u);setPnl("pnl30",b.hypothetical_pnl_1u);
 $("accuracy").textContent=pct(b.accuracy);$("accuracy-foot").textContent=(b.scored||0)+" 条已评分代理样本";
 $("samples").textContent=fmtNum(b.predictions||0);$("samples-foot").textContent=(b.pending||0)+" 条待结算 · "+(b.missing||0)+" 条缺失";
 $("symbol").textContent=(data.settings||{}).symbol||"BTCUSDT";
 $("horizons").textContent=((data.settings||{}).horizons||[30,60]).map(x=>x+"M").join(" / ");
 $("poll").textContent=(data.settings||{}).poll_seconds?data.settings.poll_seconds+" 秒":"—";
 $("lead").textContent=(data.settings||{}).preparation_lead_ms?data.settings.preparation_lead_ms/1000+" 秒":"—";
 $("tick-delay").textContent=(data.settings||{}).tick_max_delay_ms?data.settings.tick_max_delay_ms+" ms":"—";
 $("payout").textContent=(data.settings||{}).research_payout_ratio?Number(data.settings.research_payout_ratio)*100+"%":"85%";
 $("threshold").textContent=(Number(data.research_threshold)*100).toFixed(2)+"%";
 $("scored").textContent=fmtNum(b.scored||0);$("pending").textContent=fmtNum(b.pending||0);$("missing").textContent=pct(b.missing_rate);
 $("gaps").textContent=fmtNum(data.unresolved_gaps||0);
 const health=Object.entries(data.health||{});const ok=health.filter(x=>x[1].effective_status==="OK").length;
 $("health-summary").textContent=ok+"/"+health.length+" 正常";
 $("health").innerHTML=health.map(([name,item])=>'<div class="health-item"><i class="dot '+(item.effective_status==="OK"?"ok":"bad")+'"></i>'+esc(name.toUpperCase())+' · '+esc(item.effective_status)+'</div>').join("");
}
function renderHistory(rows){
 latestHistory=rows||[];$("history-count").textContent=latestHistory.length+" 条记录";
 if(!latestHistory.length){$("history").innerHTML='<tr><td colspan="10" class="empty">暂无正式 SHADOW 信号记录</td></tr>';return;}
 $("history").innerHTML=latestHistory.map(r=>{
 const up=r.candidate_direction==="HIGHER";const dir=up?"看涨":"看跌";
  const label=r.proxy_result_label||"未判定";const color=label==="赢"?"green":label==="输"?"red":"amber";
  const result='<span class="tag '+color+'">'+esc(label)+'</span>';
  return '<tr><td>'+esc(fmtTime(r.decision_ms))+'</td><td>'+esc(r.horizon_minutes)+"M"+'</td><td>'+esc(r.mechanism==="FIXED"?"固定":r.mechanism==="ROLLING"?"滚动":r.mechanism)+'</td><td class="'+(up?"positive":"negative")+'">'+dir+'</td><td>'+esc(r.entry_price_display??"—")+'</td><td>'+esc(r.exit_price_display??"—")+'</td><td>'+esc(pct(r.confidence))+'</td><td>'+(r.screen_passed?'<span class="tag green">通过</span>':'<span class="tag red">未通过</span>')+'</td><td>'+result+'</td><td><span class="tag">未交易</span></td></tr>';
 }).join("");
}
function beijingChartTime(time){const parts=new Intl.DateTimeFormat("en-CA",{timeZone:"Asia/Shanghai",year:"numeric",month:"2-digit",day:"2-digit",hour:"2-digit",minute:"2-digit",hourCycle:"h23"}).formatToParts(new Date(Number(time)*1000));const p=Object.fromEntries(parts.map(x=>[x.type,x.value]));return p.year+"-"+p.month+"-"+p.day+" "+p.hour+":"+p.minute;}
function beijingChartTick(time){return beijingChartTime(time).slice(5);}
function drawChart(data){
 latestChart=data;const el=$("chart"), candles=data.candles||[];
 const source=(data.source||"暂无数据").replaceAll("Binance Index Klines","币安官方指数 K 线").replaceAll("Binance Index WebSocket sampled OHLC (not official Klines)","币安指数实时快照聚合");
 const note=(data.note||"").replace(" · sampled OHLC 是已接收指数快照聚合，不等同官方 K 线"," · 实时快照聚合并非官方 K 线");
 $("chart-source").textContent="北京时间 · "+(source.includes("官方")?"币安官方指数":"币安指数快照")+" · "+candles.length+" 根";
 $("chart-source").title=source+" · "+note;
 if(!window.LightweightCharts){$("chart-source").textContent="图表组件加载失败";return;}
 if(!chartView){chartView=LightweightCharts.createChart(el,{width:el.clientWidth,height:el.clientHeight,layout:{background:{type:"solid",color:"#0e1518"},textColor:"#a6b4b9"},grid:{vertLines:{color:"#253135"},horzLines:{color:"#253135"}},localization:{timeFormatter:beijingChartTime},timeScale:{timeVisible:true,secondsVisible:false,tickMarkFormatter:beijingChartTick},crosshair:{mode:LightweightCharts.CrosshairMode.Normal}});chartSeries=chartView.addCandlestickSeries({upColor:"#50d4ac",downColor:"#f28983",borderVisible:false,wickUpColor:"#50d4ac",wickDownColor:"#f28983",priceFormat:{type:"price",precision:0,minMove:1}});}
 const bars=candles.map(c=>({time:Math.floor(c.time_ms/1000),open:Number(c.open),high:Number(c.high),low:Number(c.low),close:Number(c.close)}));chartSeries.setData(bars);
 const available=new Set(bars.map(b=>b.time));const width=Number(data.interval_ms||60000);
 const marks=(data.markers||[]).map(m=>{const time=Math.floor(Math.floor(Number(m.time_ms)/width)*width/1000),up=m.direction==="HIGHER";return {time,position:m.type==="EXIT"?"aboveBar":up?"belowBar":"aboveBar",color:m.type==="EXIT"?"#a6c9e6":up?"#50d4ac":"#f28983",shape:m.type==="EXIT"?"circle":up?"arrowUp":"arrowDown",text:m.type==="EXIT"?"到期":up?"看涨":"看跌"};}).filter(m=>available.has(m.time)).sort((a,b)=>a.time-b.time);
 chartSeries.setMarkers(marks);if(chartInterval!==data.interval){chartView.timeScale().fitContent();chartInterval=data.interval;}chartView.applyOptions({width:el.clientWidth,height:el.clientHeight});
}
async function refreshOverview(){try{renderOverview(await api("/api/overview"));}catch(e){toast(e.message);}}
async function refreshChart(){try{const q=new URLSearchParams({interval:$("interval").value,horizon:$("horizon").value,mechanism:$("mechanism").value,limit:"240"});const data=await api("/api/candles?"+q.toString());drawChart(data);}catch(e){$("chart-source").textContent=e.message;}}
async function refreshHistory(){if($("mechanism").value==="CHART_30M")return;try{const q=new URLSearchParams({horizon:$("horizon").value,mechanism:$("mechanism").value,limit:"80"});renderHistory((await api("/api/predictions?"+q.toString())).predictions);}catch(e){$("history").innerHTML='<tr><td colspan="10" class="empty">'+esc(e.message)+'</td></tr>';}}
const liveStateLabel={API_ACCEPTED_UNVERIFIED:"接口已受理",TIME_EXPIRED_UNVERIFIED:"到期已释放，成交未核对",REJECTED_UNVERIFIED:"接口拒绝",UNKNOWN:"请求结果未知",USER_REVIEWED_UNVERIFIED:"已人工核对",SKIPPED:"发送前跳过",RESERVED:"准备中",SUBMITTING:"提交中"};
function indicatorBadge(row){return row.indicator_history_status==="TRUNCATED"?'<span class="tag red">旧指标截断，触轨未核实</span>':row.indicator_history_status==="UNKNOWN"?'<span class="tag amber">指标来源待核对</span>':"";}
async function refreshChartStrategy(){
 try{
  const data=await api("/api/chart-strategy"),rows=data.observations||[];
  $("chart-strategy-history").innerHTML=rows.length?rows.map(r=>'<tr><td>'+esc(fmtTime(r.received_ms))+' <span class="row-id">#'+esc(r.observation_id)+'</span></td><td>'+esc(r.direction_label)+'</td><td>'+esc(r.reason_label)+'</td><td>'+esc(r.live_request_state?liveStateLabel[r.live_request_state]||"处理中":data.live_status_available?"未发请求":"状态不可用")+' '+indicatorBadge(r)+'</td><td>'+esc(r.request_index_price_display??r.entry_price_display??"—")+'</td><td>'+esc(r.exit_price_display??"—")+'</td><td>'+esc(r.result_label)+'</td></tr>').join(""):'<tr><td colspan="7" class="empty">暂无布林带信号</td></tr>';
 }catch(e){$("chart-strategy-history").innerHTML='<tr><td colspan="7" class="empty">信号数据不可用</td></tr>';}
}
let scheduledLoaded=false;
async function refreshScheduled(){try{const d=await api("/api/scheduled-paper");$("scheduled-worker").textContent=d.worker_active?"模拟服务运行中":"模拟服务未运行，配置不会执行";for(const h of [30,60]){const c=(d.configurations||[]).find(x=>x.horizon===h);$("scheduled-state-"+h).textContent=c&&c.enabled?"已启用":"未启用";if(!scheduledLoaded&&c){$("scheduled-direction-"+h).value=c.direction;$("scheduled-stake-"+h).value=c.stake;}}scheduledLoaded=true;const rs=d.orders||[];$("scheduled-history").innerHTML=rs.length?rs.map(r=>{const outcome=r.status==="MISSING"?"数据缺失":r.status==="PENDING"?"待到期":r.outcome==="WIN"?"赢":r.outcome==="LOSS"?"输":"未判定";return '<tr><td>'+esc(r.horizon===60?"1小时":"30分钟")+'</td><td>'+esc(r.direction==="HIGHER"?"看涨":"看跌")+'</td><td>'+esc(r.stake)+'</td><td>'+esc(fmtTime(r.entry_ms))+'</td><td>'+esc(fmtTime(r.expiry_ms))+'</td><td>'+esc(r.previous_close===null?"—":Number(r.previous_close).toFixed(0))+'</td><td>'+esc(r.next_close===null?"—":Number(r.next_close).toFixed(0))+'</td><td>'+esc(r.candle_outcome==="WIN"?"预测正确":r.candle_outcome==="LOSS"?"预测错误":"待确认")+'</td><td>'+esc(r.entry_price===null?"—":Number(r.entry_price).toFixed(0))+'</td><td>'+esc(r.exit_price===null?"—":Number(r.exit_price).toFixed(0))+'</td><td>'+esc(outcome)+'</td></tr>'}).join(""):'<tr><td colspan="11" class="empty">暂无模拟记录</td></tr>'; }catch(e){$("scheduled-history").innerHTML='<tr><td colspan="11" class="empty">模拟服务不可用</td></tr>';}}
for(const h of [30,60])for(const enable of [true,false])$("scheduled-"+(enable?"start-":"stop-")+h).addEventListener("click",async()=>{try{const stake=$("scheduled-stake-"+h).value||(!enable?"1":"");await api("/api/scheduled-paper",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({horizon:h,enabled:enable,direction:$("scheduled-direction-"+h).value,stake})});toast(enable?"模拟已启用":"模拟已停止");refreshScheduled();}catch(e){toast("请填写有效金额，并确认模拟服务正在运行");}});
function updateStrategyVisibility(){const mode=$("mechanism").value;if(mode==="CHART_30M"&&$("horizon").value==="60")$("horizon").value="30";$("chart-strategy-panel").hidden=(mode!=="all"&&mode!=="CHART_30M")||$("horizon").value==="60";$("model-history-panel").hidden=mode==="CHART_30M";$("model-metrics").hidden=mode==="CHART_30M"&&page!=="statistics";$("model-statistics").hidden=mode==="CHART_30M"&&page!=="statistics";}
function updateLiveHeader(data){const active=!!data.enabled;$("live-footer-status").textContent=active?"交易运行中":"交易已暂停";if(page==="live"){$("runner-pill").innerHTML='<i class="dot '+(active?"ok":"")+'"></i> '+(active?"自动交易运行中":"自动交易已暂停");$("updated").textContent=active?data.horizon+" 分钟 · "+data.amount+" USDT":"BTCUSDT";}}
function renderLive(data){
 updateLiveHeader(data);
 const rows=data.attempts||[],cooldown=rows.find(r=>r.state==="API_ACCEPTED_UNVERIFIED");
 $("live-state-tag").textContent=data.enabled?(cooldown?"等待上笔到期":"运行中"):(cooldown?"已暂停 · 上笔待到期":"已暂停");
 $("live-state-tag").className="tag "+(data.enabled?"red":"amber");
 $("live-enable").disabled=!!data.enabled;$("live-disable").disabled=!data.enabled;
 $("live-amount").max=data.max_amount;
 if(data.enabled){$("live-period").value=String(data.horizon);$("live-direction-mode").value=data.direction_mode||"SIGNAL";$("live-cooldown").value=String(!!data.cooldown_five_minutes);}
 if(data.amount&&!$("live-amount").value)$("live-amount").value=data.amount;
 $("live-readiness").textContent=data.enabled
  ?(cooldown?"上一笔到期 "+fmtTime(cooldown.expiry_ms)+" · 下次最早 "+fmtTime(cooldown.expiry_ms+(cooldown.release_buffer_ms??(data.cooldown_five_minutes?300000:60000))):"等待新信号")
  :cooldown?"已暂停。上一笔仍会记录到期结果；新设置只用于后续信号。"
  :"已暂停。修改参数后可启用。";
 const unresolved=rows.find(r=>["API_ACCEPTED_UNVERIFIED","REJECTED_UNVERIFIED","UNKNOWN"].includes(r.state)&&r.bridge_attempt_id);
 $("live-review").hidden=!unresolved;$("live-review").disabled=!unresolved;$("live-review").dataset.attemptId=unresolved?unresolved.id:"";
 $("live-attempts").innerHTML=rows.length?rows.map(r=>'<tr><td>#'+esc(r.observation_id)+'</td><td>'+esc(fmtTime(r.created_ms))+'</td><td>'+esc(r.direction==="LONG"?"看涨":"看跌")+'</td><td>'+esc(r.amount)+" USDT"+'</td><td>'+esc(r.request_index_price_display??"—")+'</td><td>'+esc(r.proxy_exit_price_display??"—")+'</td><td>'+esc(r.proxy_result_label??"未判定")+'</td><td>'+esc(liveStateLabel[r.state]||"处理中")+' '+indicatorBadge(r)+'</td></tr>').join(""):'<tr><td colspan="8" class="empty">暂无自动请求</td></tr>';
}
async function refreshLive(){try{renderLive(await api("/api/live"));}catch(e){$("live-state-tag").textContent="不可用";$("live-enable").disabled=true;$("live-disable").disabled=true;$("live-review").disabled=true;$("live-readiness").textContent="交易控制服务状态不可用，请勿据此判断开关状态。";$("live-footer-status").textContent="真实交易状态无法读取";if(page==="live"){$("runner-pill").innerHTML='<i class="dot"></i> 交易状态不可用';$("updated").textContent="检查连接";}}}
const fmtBjt=v=>v?new Date(Number(v)).toLocaleString("zh-CN",{timeZone:"Asia/Shanghai",month:"2-digit",day:"2-digit",hour:"2-digit",minute:"2-digit",second:"2-digit",hour12:false}):"—";
const fmtPnl=v=>v===null||v===undefined?"—":(Number(v)>=0?"+":"")+Number(v).toFixed(2)+" USDT";
function renderActualStats(data){
 $("actual-statistics-period").textContent="自 "+fmtBjt(data.start_ms)+" 起 · 北京时间";
 $("actual-request-count").textContent=fmtNum(data.requests);
 $("actual-scored").textContent=fmtNum(data.scored);
 $("actual-win-rate").textContent=data.win_rate===null?"—":pct(data.win_rate);
 $("actual-estimated-pnl").textContent=data.scored?fmtPnl(data.estimated_pnl):"—";
 $("actual-estimated-pnl").className="metric-value "+(Number(data.estimated_pnl)<0?"negative":"positive");
 $("actual-statistics-breakdown").textContent="赢 "+data.wins+" · 输 "+data.losses+" · 待到期 "+data.pending+" · 缺失 "+data.missing+" · 待核对 "+(data.equal+data.unverifiable);
 $("actual-statistics-status").textContent="按每笔请求金额和提交赔付比例估算；非币安成交与结算。";
 const labels={VERIFIED:{WIN:"赢",LOSS:"输"},PENDING:"待到期",MISSING:"数据缺失",EQUAL:"价格相同待核对",UNVERIFIABLE:"账本未核对"};
 const rows=data.rows||[];
 $("actual-statistics-rows").innerHTML=rows.length?rows.map(r=>'<tr><td>'+esc(fmtBjt(r.created_ms))+'</td><td>'+esc(r.period_minutes?r.period_minutes+" 分钟":"—")+'</td><td>'+esc(r.direction==="LONG"?"看涨":"看跌")+'</td><td>'+esc(r.amount)+" USDT"+'</td><td>'+esc(r.submitted_ratio===null?"—":pct(r.submitted_ratio))+'</td><td>'+esc(r.proxy_status==="VERIFIED"?labels.VERIFIED[r.proxy_outcome]||"未判定":labels[r.proxy_status]||"未判定")+'</td><td>'+esc(fmtPnl(r.estimated_pnl))+'</td><td>'+esc(fmtPnl(r.cumulative_pnl))+'</td></tr>').join(""):'<tr><td colspan="8" class="empty">起点之后暂无自动请求</td></tr>';
 if(!window.LightweightCharts)return;
 const el=$("live-pnl-chart");
 if(!livePnlChart){livePnlChart=LightweightCharts.createChart(el,{width:el.clientWidth,height:el.clientHeight,layout:{background:{type:"solid",color:"#0e1518"},textColor:"#a6b4b9"},grid:{vertLines:{color:"#253135"},horzLines:{color:"#253135"}},localization:{timeFormatter:beijingChartTime},timeScale:{timeVisible:true,tickMarkFormatter:beijingChartTick}});livePnlSeries=livePnlChart.addLineSeries({color:"#50d4ac",lineWidth:2,priceFormat:{type:"price",precision:2,minMove:0.01}});}
 const values=new Map([[Math.floor(data.start_ms/1000),0]]);(data.curve||[]).forEach(p=>values.set(Math.floor(p.expiry_ms/1000),Number(p.cumulative_pnl)));
 const points=[...values].sort((a,b)=>a[0]-b[0]).map(([time,value])=>({time,value}));livePnlSeries.setData(points);
 if(livePnlPointCount!==points.length){livePnlChart.timeScale().fitContent();livePnlPointCount=points.length;}
 livePnlChart.applyOptions({width:el.clientWidth,height:el.clientHeight});
}
async function refreshActualStats(){try{renderActualStats(await api("/api/live-statistics"));}catch(e){$("actual-statistics-status").textContent="统计数据暂不可用；请检查本机只读账本。";$("actual-statistics-rows").innerHTML='<tr><td colspan="8" class="empty">统计读取失败</td></tr>';}}
const liveError={LIVE_AUTH_REQUIRED:"服务端交易授权不可用",INVALID_AMOUNT:"金额无效",INVALID_LIVE_CONFIGURATION:"金额或策略配置无效",BRIDGE_RECONCILE_REQUIRED:"桥接账本有待核对请求",UNRESOLVED_ATTEMPT:"上一笔请求仍待核对",STRATEGY_UNAVAILABLE:"该周期没有已定义策略",LIVE_SOURCE_UNAVAILABLE:"行情或桥接服务不可用"};
$("live-enable").addEventListener("click",async()=>{const amount=$("live-amount").value;if(!amount){toast("请填写金额");return;}$("live-enable").disabled=true;try{const r=await api("/api/live",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({action:"enable",strategy:$("live-strategy").value,horizon:Number($("live-period").value),amount,direction_mode:$("live-direction-mode").value,cooldown_five_minutes:$("live-cooldown").value==="true"})});renderLive(r);toast("持续自动交易已启用");}catch(e){toast(liveError[e.message]||"启用失败，请检查状态");refreshLive();}});
$("live-disable").addEventListener("click",async()=>{try{renderLive(await api("/api/live",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({action:"disable"})}));toast("已停用");}catch(e){toast("停用失败，请检查状态");refreshLive();}});
$("live-review").addEventListener("click",async()=>{const attempt_id=$("live-review").dataset.attemptId;if(!attempt_id)return;if(!confirm("请先在币安核对上一笔请求。确认已检查交易记录并解除阻止？这里不会把请求标记为已成交或已结算。"))return;$("live-review").disabled=true;try{renderLive(await api("/api/live",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({action:"review",attempt_id,checked_in_binance:true})}));toast("已记录人工核对");}catch(e){toast("核对失败，请检查桥接记录");refreshLive();}});
function refreshAll(){updateStrategyVisibility();if(page==="overview"){refreshOverview();refreshChart();}else if(page==="records"){refreshScheduled();refreshHistory();refreshChartStrategy();}else if(page==="statistics"){refreshOverview();refreshActualStats();}else if(page==="live"){refreshLive();}}
["interval","horizon","mechanism"].forEach(id=>$(id).addEventListener("change",()=>{updateStrategyVisibility();refreshChart();refreshHistory();refreshChartStrategy();}));
$("stop").addEventListener("click",async()=>{if(!confirm("确认向当前 SHADOW 进程发送安全停止请求？不会触发任何交易。"))return;$("stop").disabled=true;try{const r=await api("/api/stop",{method:"POST"});toast(r.message||"已提交安全停止请求");setTimeout(refreshOverview,1500);}catch(e){toast(e.message);refreshOverview();}});
window.addEventListener("resize",()=>{if(latestChart)drawChart(latestChart);if(livePnlChart){const el=$("live-pnl-chart");livePnlChart.applyOptions({width:el.clientWidth,height:el.clientHeight});}});
refreshAll();if(page==="overview"||page==="statistics")setInterval(refreshOverview,5000);if(page==="statistics")setInterval(refreshActualStats,15000);if(page==="overview")setInterval(refreshChart,15000);if(page==="records")setInterval(()=>{refreshHistory();refreshChartStrategy();refreshScheduled();},15000);if(page==="live")setInterval(refreshLive,5000);
})();
</script>
</body></html>"""


def _connect_readonly(path: Path):
    if path.is_symlink() or not path.is_file():
        return None
    resolved = path.resolve(strict=True)
    uri = "file:" + quote(str(resolved), safe="/") + "?mode=ro"
    db = sqlite3.connect(uri, uri=True, timeout=1.5)
    db.row_factory = sqlite3.Row
    db.execute("PRAGMA query_only=ON")
    return db


def _table_exists(db, name: str) -> bool:
    return db.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (name,)
    ).fetchone() is not None


def _read_predictions(db, since_ms: int | None = None, limit: int | None = None):
    if not _table_exists(db, "predictions"):
        return []
    where = "WHERE classification='shadow_v1'"
    params: list[object] = []
    if since_ms is not None:
        where += " AND decision_ms>=?"
        params.append(since_ms)
    sql = f"SELECT {PREDICTION_COLUMNS} FROM predictions {where} ORDER BY entry_ms DESC,id"
    if limit is not None:
        sql += " LIMIT ?"
        params.append(max(1, min(int(limit), 500)))
    return [dict(row) for row in db.execute(sql, params).fetchall()]


def _parse_config(path: Path | None):
    safe = {
        "symbol": "BTCUSDT",
        "horizons": [30, 60],
        "research_payout_ratio": "0.85",
        "poll_seconds": 60,
        "preparation_lead_ms": 45_000,
        "tick_max_delay_ms": 5_000,
    }
    if path is None:
        return safe
    if path.is_symlink() or not path.is_file():
        return safe
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return safe
    # Explicit allowlist: never return credential paths, tokens, or local paths.
    for key in (
        "symbol", "horizons", "research_payout_ratio", "poll_seconds",
        "preparation_lead_ms", "tick_max_delay_ms", "clock_max_error_ms",
        "openmarket_max_age_ms", "cvd_max_age_ms", "health_max_age_ms",
        "proxy_price_window_ms", "research_probability_margin",
    ):
        if key in raw and isinstance(raw[key], (str, int, float, list)):
            safe[key] = raw[key]
    return safe


def _summarise(stats: dict):
    groups = list(stats.values())
    predictions = sum(x.get("predictions", 0) for x in groups)
    expired = sum(x.get("expired", 0) for x in groups)
    verified = sum(x.get("verified", 0) for x in groups)
    scored = sum(x.get("scored", 0) for x in groups)
    correct = sum(round((x.get("accuracy") or 0) * x.get("scored", 0)) for x in groups)
    missing = sum(x.get("missing", 0) for x in groups)
    selected = sum(x.get("hypothetical_85pct", {}).get("independent_selected", 0) for x in groups)
    pnl = 0.0
    for item in groups:
        h = item.get("hypothetical_85pct", {})
        n = h.get("independent_selected", 0)
        net = h.get("hypothetical_net_per_stake")
        if n and net is not None:
            pnl += float(net) * n
    return {
        "predictions": predictions,
        "expired": expired,
        "verified": verified,
        "scored": scored,
        "correct": correct,
        "pending": max(0, predictions - expired),
        "missing": missing,
        "missing_rate": missing / expired if expired else None,
        "accuracy": correct / scored if scored else None,
        "selected_nonoverlap": selected,
        "hypothetical_pnl_1u": round(pnl, 4) if selected else None,
        "execution_decision": "NO TRADE",
    }


def build_overview(directory: Path, market_db: Path, config_path: Path | None = None, now_ms: int | None = None):
    now = int(now_ms if now_ms is not None else time.time() * 1000)
    health_rows = []
    gaps = 0
    formal_rows = []
    total_formal = 0
    events = []
    db = _connect_readonly(directory / "shadow.sqlite")
    if db:
        try:
            if _table_exists(db, "health"):
                health_rows = [dict(row) for row in db.execute(
                    "SELECT source,status,checked_ms,detail FROM health ORDER BY source"
                )]
            if _table_exists(db, "gaps"):
                gaps = db.execute("SELECT COUNT(*) FROM gaps WHERE status='OPEN'").fetchone()[0]
            if _table_exists(db, "predictions"):
                total_formal = db.execute(
                    "SELECT COUNT(*) FROM predictions WHERE classification='shadow_v1'"
                ).fetchone()[0]
                formal_rows = _read_predictions(db, now - 30 * 86_400_000)
            if _table_exists(db, "events"):
                events = [dict(row) for row in db.execute(
                    "SELECT kind,observed_ms FROM events ORDER BY id DESC LIMIT 8"
                )]
        finally:
            db.close()
    health = health_overview(health_rows, now, 180_000, 5_000)
    for item in health.values():
        item.pop("detail", None)  # Do not expose raw diagnostic payloads in the browser API.
    stats_7d = dashboard(formal_rows, now, 7 * 86_400_000)
    stats_30d = dashboard(formal_rows, now, 30 * 86_400_000)
    try:
        active = runner_active(directory)
    except OSError:
        active = False
    return {
        "timestamp_ms": now,
        "runner_active": active,
        "settings": _parse_config(config_path),
        "health": health,
        "healthy_sources": sum(x.get("effective_status") == "OK" for x in health.values()),
        "health_sources": len(health),
        "unresolved_gaps": gaps,
        "formal_predictions_total": total_formal,
        "last_7_days": _summarise(stats_7d),
        "last_30_days": _summarise(stats_30d),
        "research_threshold": float(RESEARCH_THRESHOLD),
        "execution_decision": "NO TRADE",
        "events": events,
        "database": {
            "shadow_available": (directory / "shadow.sqlite").is_file(),
            "market_available": market_db.is_file(),
        },
    }


def _load_one_minute(directory: Path, market_db: Path, now_ms: int):
    bars_by_open = {}
    source_labels = set()
    db = _connect_readonly(market_db)
    if db:
        try:
            if _table_exists(db, "bars"):
                rows = db.execute(
                    "SELECT dataset,open_ms,close_ms,available_ms,open,high,low,close "
                    "FROM bars WHERE dataset='binance:index:BTCUSDT:1m' "
                    "AND close_ms<? AND available_ms>close_ms "
                    "ORDER BY open_ms DESC,available_ms DESC,id DESC LIMIT 5000",
                    (now_ms,),
                ).fetchall()
                for row in rows:
                    t = row["open_ms"]
                    if t not in bars_by_open:
                        bars_by_open[t] = {
                            "time_ms": t, "open_ms": t, "close_ms": row["close_ms"],
                            "open": row["open"], "high": row["high"], "low": row["low"],
                            "close": row["close"], "source": "Binance Index Klines",
                        }
                if bars_by_open:
                    source_labels.add("Binance Index Klines")
        finally:
            db.close()
    shadow = _connect_readonly(directory / "shadow.sqlite")
    if shadow:
        try:
            if _table_exists(shadow, "ticks"):
                earliest_tick_ms = max(0, now_ms - 5000 * 60_000)
                ticks = shadow.execute(
                    "SELECT event_ms,received_ms,index_price FROM ticks "
                    "WHERE event_ms>? AND event_ms<=? AND received_ms<=? "
                    "ORDER BY event_ms,received_ms",
                    (earliest_tick_ms - 1, now_ms, now_ms),
                ).fetchall()
                grouped = defaultdict(list)
                for tick in ticks:
                    grouped[(tick["event_ms"] // 60_000) * 60_000].append(tick)
                for start, points in grouped.items():
                    if start in bars_by_open:
                        continue  # Official candle takes precedence over sampled ticks.
                    values = [Decimal(x["index_price"]) for x in points]
                    if not values:
                        continue
                    bars_by_open[start] = {
                        "time_ms": start, "open_ms": start, "close_ms": start + 59_999,
                        "open": str(values[0]), "high": str(max(values)),
                        "low": str(min(values)), "close": str(values[-1]),
                        "source": "Binance Index WebSocket sampled OHLC (not official Klines)",
                    }
                if any(bar["source"] == "Binance Index WebSocket sampled OHLC (not official Klines)" for bar in bars_by_open.values()):
                    source_labels.add("Binance Index WebSocket sampled OHLC (not official Klines)")
        finally:
            shadow.close()
    return [bars_by_open[k] for k in sorted(bars_by_open)], sorted(source_labels)


def build_candles(
    directory: Path, market_db: Path, interval: str = "1m", limit: int = 240,
    horizon: str = "all", mechanism: str = "all", now_ms: int | None = None,
):
    if interval not in INTERVAL_MS:
        raise ValueError("unsupported chart interval")
    if horizon not in ("all", "30", "60") or mechanism not in ("all", "FIXED", "ROLLING"):
        raise ValueError("unsupported signal filter")
    now = int(now_ms if now_ms is not None else time.time() * 1000)
    limit = max(20, min(int(limit), 500))
    base, source_labels = _load_one_minute(directory, market_db, now)
    width = INTERVAL_MS[interval]
    factor = width // 60_000
    groups = defaultdict(list)
    for bar in base:
        if bar["close_ms"] < now:
            groups[(bar["open_ms"] // width) * width].append(bar)
    candles = []
    for start, rows in sorted(groups.items()):
        rows.sort(key=lambda x: x["open_ms"])
        expected = [start + i * 60_000 for i in range(factor)]
        if [x["open_ms"] for x in rows] != expected:
            continue  # Never bridge or interpolate missing minutes.
        candles.append({
            "time_ms": start,
            "open": rows[0]["open"],
            "high": str(max(Decimal(x["high"]) for x in rows)),
            "low": str(min(Decimal(x["low"]) for x in rows)),
            "close": rows[-1]["close"],
        })
    candles = candles[-limit:]
    markers = []
    if candles:
        start_ms, end_ms = candles[0]["time_ms"], candles[-1]["time_ms"] + width
        db = _connect_readonly(directory / "shadow.sqlite")
        if db:
            try:
                rows = db.execute(
                    f"SELECT {PREDICTION_COLUMNS} FROM predictions "
                    "WHERE classification='shadow_v1' AND entry_ms>=? AND entry_ms<=? "
                    "ORDER BY entry_ms DESC",
                    (start_ms - 60 * 60_000, end_ms),
                ).fetchall()
                # On a 5m/15m/30m/1h chart, keep the latest signal for each
                # profile per candle. The full history table still shows every
                # candidate; this prevents rolling signals from overpainting.
                chosen = {}
                lane_map = {(30, "FIXED"): 0, (30, "ROLLING"): 1,
                            (60, "FIXED"): 2, (60, "ROLLING"): 3}
                for row in rows:
                    item = dict(row)
                    if horizon != "all" and str(item["horizon_minutes"]) != horizon:
                        continue
                    if mechanism != "all" and item["mechanism"] != mechanism:
                        continue
                    profile = (item["horizon_minutes"], item["mechanism"])
                    lane = lane_map.get(profile, 0)
                    direction = item["candidate_direction"]
                    if start_ms <= item["entry_ms"] <= end_ms:
                        bucket = (item["entry_ms"] // width) * width
                        key = ("ENTRY", bucket, profile)
                        chosen.setdefault(key, {
                            "type": "ENTRY", "time_ms": item["entry_ms"],
                            "price": item["entry_price"], "direction": direction,
                            "probability": item["probability_up"],
                            "result_status": item["result_status"], "lane": lane,
                        })
                    if (item["exit_price"] is not None
                            and item["exit_event_ms"] is not None
                            and start_ms <= item["expiry_ms"] <= end_ms):
                        bucket = (item["expiry_ms"] // width) * width
                        key = ("EXIT", bucket, profile)
                        chosen.setdefault(key, {
                            "type": "EXIT", "time_ms": item["expiry_ms"],
                            "price": item["exit_price"], "direction": direction,
                            "result_status": item["result_status"], "lane": lane,
                        })
                markers = sorted(chosen.values(), key=lambda x: (x["time_ms"], x["type"]))
            finally:
                db.close()
    note = ("缺失分钟不插值；高周期每根 K 线每种策略仅标记最新候选，完整记录见下表"
            if width > 60_000 else "缺失分钟不插值；只展示已收盘的指数 K 线")
    if any("sampled OHLC" in label for label in source_labels):
        note += " · sampled OHLC 是已接收指数快照聚合，不等同官方 K 线"
    if not candles:
        note = "暂无足够连续的指数 K 线数据；不会用模拟价格填充"
    return {
        "interval": interval,
        "interval_ms": width,
        "candles": candles,
        "markers": markers,
        "source": " + ".join(source_labels) if source_labels else "无可用行情来源",
        "note": note,
        "execution_decision": "NO TRADE",
    }


def display_index_price(value):
    """Round only the displayed index proxy; retain the exact source value."""
    if value is None:
        return None
    try:
        price = Decimal(str(value))
        if not price.is_finite():
            return None
        return format(price.to_integral_value(rounding=ROUND_HALF_UP), "f")
    except (InvalidOperation, TypeError, ValueError):
        return None


def proxy_result_label(status, forecast, observed):
    """Describe forecast correctness, never actual trade or settlement."""
    if status == "PENDING":
        return "待到期"
    if status == "MISSING":
        return "数据缺失"
    if status == "VERIFIED" and forecast in ("HIGHER", "LOWER") and observed in ("HIGHER", "LOWER"):
        return "赢" if forecast == observed else "输"
    return "未判定"


def build_predictions(directory: Path, horizon: str = "all", mechanism: str = "all", limit: int = 80):
    if horizon not in ("all", "30", "60") or mechanism not in ("all", "FIXED", "ROLLING"):
        raise ValueError("unsupported history filter")
    db = _connect_readonly(directory / "shadow.sqlite")
    if not db:
        return []
    try:
        if not _table_exists(db, "predictions"):
            return []
        where = ["classification='shadow_v1'"]
        params: list[object] = []
        if horizon in ("30", "60"):
            where.append("horizon_minutes=?")
            params.append(int(horizon))
        if mechanism in ("FIXED", "ROLLING"):
            where.append("mechanism=?")
            params.append(mechanism)
        params.append(max(1, min(int(limit), 200)))
        rows = [dict(row) for row in db.execute(
            f"SELECT {PREDICTION_COLUMNS} FROM predictions WHERE "
            + " AND ".join(where) + " ORDER BY entry_ms DESC,id LIMIT ?",
            params,
        ).fetchall()]
    finally:
        db.close()
    return [{
        "id": r["id"][:12],
        "decision_ms": r["decision_ms"],
        "entry_ms": r["entry_ms"],
        "expiry_ms": r["expiry_ms"],
        "horizon_minutes": r["horizon_minutes"],
        "mechanism": r["mechanism"],
        "screen_passed": bool(r["screen_passed"]),
        "confidence": max(float(r["probability_up"]), 1 - float(r["probability_up"])),
        "candidate_direction": r["candidate_direction"],
        "entry_price": r["entry_price"],
        "exit_price": r["exit_price"],
        "entry_price_display": display_index_price(r["entry_price"]),
        "exit_price_display": display_index_price(r["exit_price"]),
        "result_status": r["result_status"],
        "direction_result": r["direction_result"],
        "proxy_result_label": proxy_result_label(r["result_status"], r["candidate_direction"], r["direction_result"]),
        "execution_decision": "NO TRADE",
    } for r in rows]


def chart_labels(direction, reason):
    directions = {"HIGHER":"看涨", "LOWER":"看跌", "NO_SIGNAL":"无信号", "UNAVAILABLE":"数据不可用"}
    reasons = {"CONDITIONS_MET":"满足入场条件", "BOTH_LOWER_TOUCHED":"5m 与 1m 已收盘 K 线均触及布林下轨", "BOTH_UPPER_TOUCHED":"5m 与 1m 已收盘 K 线均触及布林上轨", "BOTH_LOWER_TOUCHED_INTRABAR":"5m 与 1m 盘中触及布林下轨", "BOTH_UPPER_TOUCHED_INTRABAR":"5m 与 1m 盘中触及布林上轨", "AMBIGUOUS_TOUCH":"上下轨同时触及，方向不明确", "CHARTS_NOT_ALIGNED":"1m 与 5m 收盘时间未对齐", "CONDITIONS_NOT_MET":"未满足入场条件", "OVERLAPPING_POSITION":"已有未到期模拟仓位", "INVALID_OR_MISSING_DATA":"指标数据无效或缺失", "INDICATOR_HISTORY_UNAVAILABLE":"布林带历史不完整，已阻止信号", "INDICATOR_FORMULA_MISMATCH":"布林带参数与历史数值不一致，已阻止信号", "STALE_OR_NONCONTIGUOUS":"数据过期或K线不连续", "INSUFFICIENT_CLOSED_BARS":"已收盘K线不足", "CHART_MISMATCH":"图表信息不匹配"}
    return directions.get(direction, "未识别"), reasons.get(reason, "其他原因")


def indicator_history_status(payload) -> str:
    """Expose only source completeness, never the raw chart response."""
    try:
        chart = json.loads(payload)
        flags = [chart[key]["truncated"] for key in ("five_min", "one_min")]
    except (ValueError, TypeError, KeyError):
        return "UNKNOWN"
    if any(flag is True for flag in flags):
        return "TRUNCATED"
    return "COMPLETE" if all(flag is False for flag in flags) else "UNKNOWN"


def enrich_live_attempts(state: dict, chart_db: Path, ticks_db: Path,
                         tick_max_age_ms: int) -> dict:
    """Read-only projection joining a submitted request to its chart signal.

    The request index is the latest tick already received when the local
    request began. It is never an exchange fill or settlement price.
    """
    attempts = state.get("attempts", [])
    if not attempts:
        return state
    chart = _connect_readonly(chart_db)
    ticks = _connect_readonly(ticks_db)
    try:
        for attempt in attempts:
            attempt.update(request_index_price_display=None,
                           request_index_event_ms=None,
                           proxy_entry_price_display=None,
                           proxy_exit_price_display=None,
                           indicator_history_status="UNKNOWN",
                           proxy_result_label="未找到信号")
            if chart and _table_exists(chart, "observations") and _table_exists(chart, "proxy_results"):
                row = chart.execute(
                    "SELECT o.decision,o.payload,o.expiry_ms,r.status,r.entry_price,r.exit_price,r.direction_result "
                    "FROM observations o LEFT JOIN proxy_results r ON r.observation_id=o.id "
                    "WHERE o.id=?", (attempt["observation_id"],)).fetchone()
                if row:
                    attempt["indicator_history_status"] = indicator_history_status(row["payload"])
                    duration_mismatch = attempt.get("expiry_ms") is not None and attempt["expiry_ms"] != row["expiry_ms"]
                    attempt["proxy_entry_price_display"] = display_index_price(row["entry_price"])
                    attempt["proxy_exit_price_display"] = None if duration_mismatch else display_index_price(row["exit_price"])
                    try:
                        direction = json.loads(row["decision"]).get("direction")
                    except (ValueError, TypeError, AttributeError):
                        attempt["proxy_result_label"] = "信号数据异常"
                    else:
                        actual_direction = {"LONG": "HIGHER", "SHORT": "LOWER"}.get(attempt.get("direction"), direction)
                        attempt["proxy_result_label"] = ("该周期待核对" if duration_mismatch else proxy_result_label(
                            row["status"], actual_direction, row["direction_result"]))
            if (attempt.get("bridge_attempt_id") and ticks and _table_exists(ticks, "ticks")
                    and type(attempt.get("created_ms")) is int):
                created = attempt["created_ms"]
                row = ticks.execute(
                    "SELECT event_ms,index_price FROM ticks "
                    "WHERE event_ms>=? AND event_ms<=? AND received_ms<=? "
                    "ORDER BY event_ms DESC,received_ms DESC LIMIT 1",
                    (created - tick_max_age_ms, created, created)).fetchone()
                if row:
                    attempt["request_index_price_display"] = display_index_price(row["index_price"])
                    attempt["request_index_event_ms"] = row["event_ms"]
            proxy_status = attempt.get("proxy_status")
            if proxy_status:
                attempt["proxy_entry_price_display"] = display_index_price(attempt.get("proxy_entry_price"))
                attempt["proxy_exit_price_display"] = display_index_price(attempt.get("proxy_exit_price"))
                attempt["proxy_result_label"] = {
                    "PENDING": "待到期", "MISSING": "数据缺失", "EQUAL": "价格相同待核对",
                }.get(proxy_status, {"WIN": "赢", "LOSS": "输"}.get(attempt.get("proxy_outcome"), "未判定"))
        return state
    finally:
        if chart:
            chart.close()
        if ticks:
            ticks.close()


def build_chart_observations(path: Path, limit: int = 80,
                             attempts_by_observation: dict | None = None):
    db = _connect_readonly(path)
    if db is None:
        return []
    try:
        if not _table_exists(db, "observations") or not _table_exists(db, "proxy_results"):
            return []
        rows = db.execute("SELECT o.id,o.received_ms,o.decision,o.payload,o.entry_ms,o.expiry_ms,r.status,r.entry_price,r.exit_price,r.direction_result FROM observations o LEFT JOIN proxy_results r ON r.observation_id=o.id WHERE json_valid(o.decision) AND json_extract(o.decision, '$.direction') IN ('HIGHER','LOWER') ORDER BY o.id DESC LIMIT ?", (min(max(limit, 1), 200),)).fetchall()
        result = []
        for row in rows:
            decision = json.loads(row["decision"])
            direction = decision.get("direction", "UNAVAILABLE")
            status = row["status"]
            label = "未判定"
            if direction in ("HIGHER", "LOWER"):
                if status == "MISSING": label = "数据缺失"
                elif status == "VERIFIED" and row["direction_result"] in ("HIGHER", "LOWER"): label = "赢" if direction == row["direction_result"] else "输"
                elif status in (None, "PENDING"): label = "待到期"
            reason = decision.get("reason", "")
            direction_label, reason_label = chart_labels(direction, reason)
            if reason == "CONDITIONS_MET":
                reason_label = ("收盘上穿布林中轨，高于均线和 AVWAP 低线，低于布林上轨" if direction == "HIGHER" else "收盘下穿布林中轨，低于均线和 AVWAP 高线，高于布林下轨")
            attempt = (attempts_by_observation or {}).get(row["id"])
            result.append({"observation_id":row["id"], "received_ms":row["received_ms"], "direction":direction, "direction_label":direction_label, "reason":reason, "reason_label":reason_label, "entry_ms":row["entry_ms"], "expiry_ms":row["expiry_ms"], "entry_price_display":display_index_price(row["entry_price"]), "exit_price_display":display_index_price(row["exit_price"]), "result_label":label, "indicator_history_status":indicator_history_status(row["payload"]), "live_request_state":attempt["state"] if attempt else None, "request_index_price_display":attempt.get("request_index_price_display") if attempt else None})
        return result
    finally:
        db.close()


def chart_strategy_markers(rows, candles, interval_ms):
    times = {c["time_ms"] for c in candles}
    markers = []
    for row in rows:
        for key, typ in (("entry_ms", "ENTRY"), ("expiry_ms", "EXIT")):
            t = row.get(key)
            if t is None or (typ == "EXIT" and row.get("result_label") not in ("赢", "输", "未判定")):
                continue
            if typ == "EXIT" and row.get("exit_price_display") is None:
                continue
            bucket = (t // interval_ms) * interval_ms
            if bucket in times:
                markers.append({"time_ms":bucket,"type":typ,"direction":row["direction"]})
    return sorted(markers, key=lambda x: x["time_ms"])


def scheduled_database(directory: Path):
    root = directory.parent / "scheduled-paper"
    if root.is_symlink() or not root.is_dir():
        raise ValueError("paper directory missing")
    st = root.stat()
    if st.st_uid != os.getuid() or st.st_mode & 0o077:
        raise ValueError("paper directory not private")
    path = root / "schedule.sqlite"
    if path.is_symlink():
        raise ValueError("invalid paper database")
    return path


def read_scheduled_config(directory: Path):
    db = _connect_readonly(scheduled_database(directory))
    if db is None:
        return {"configurations": [], "orders": [], "worker_active": False}
    try:
        if not _table_exists(db, "configs") or not _table_exists(db, "orders"):
            return {"configurations": [], "orders": [], "worker_active": False}
        heartbeat = db.execute("SELECT heartbeat_ms FROM runtime WHERE id=1").fetchone() if _table_exists(db, "runtime") else None
        active = bool(heartbeat and 0 <= time.time_ns()//1000000 - heartbeat[0] <= 15000)
        return {"configurations": [dict(x) for x in db.execute("SELECT * FROM configs ORDER BY horizon")], "orders": [dict(x) for x in db.execute("SELECT * FROM orders ORDER BY boundary_ms DESC,horizon LIMIT 100")], "worker_active": active}
    finally:
        db.close()


def save_scheduled_config(directory: Path, body):
    from event_execution.scheduled_paper import PaperSchedule
    if not isinstance(body,dict) or set(body)!={"horizon","enabled","direction","stake"}:
        raise ValueError("invalid paper request")
    ledger = PaperSchedule(scheduled_database(directory))
    try:
        ledger.configure(body["horizon"],body["enabled"],body["direction"],body["stake"],time.time_ns()//1000000)
    finally:
        ledger.close()


class Handler(BaseHTTPRequestHandler):
    server_version = "BTCShadowWeb/1.0"

    def _json(self, payload, status=200):
        body = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self._security_headers()
        self.end_headers()
        self.wfile.write(body)

    def _security_headers(self):
        self.send_header("X-Content-Type-Options", "nosniff")
        self.send_header("X-Frame-Options", "DENY")
        self.send_header("Referrer-Policy", "no-referrer")
        self.send_header(
            "Content-Security-Policy",
            "default-src 'self'; script-src 'self' 'unsafe-inline'; style-src 'self' 'unsafe-inline'; "
            "connect-src 'self'; img-src 'self' data:; base-uri 'none'; frame-ancestors 'none'",
        )

    def do_GET(self):
        parsed = urlparse(self.path)
        try:
            if parsed.path in ("/", "/records", "/statistics", "/live"):
                body = HTML.encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.send_header("Content-Length", str(len(body)))
                self.send_header("Cache-Control", "no-store")
                self._security_headers()
                self.end_headers()
                self.wfile.write(body)
                return
            query = parse_qs(parsed.query)
            if parsed.path == "/api/live-statistics" and getattr(self.server, "live_config", None):
                self._json(read_live_statistics_for_site(self.server.live_config))
                return
            if parsed.path == "/api/live" and getattr(self.server, "live_config", None):
                state = read_live_state(self.server.live_config)
                directory = getattr(self.server, "data_directory", None)
                if directory is not None:
                    settings = _parse_config(getattr(self.server, "config_path", None))
                    try:
                        enrich_live_attempts(state,
                            directory.parent / "chart-strategy" / "observations.sqlite",
                            directory / "shadow.sqlite",
                            settings["tick_max_delay_ms"])
                    except (sqlite3.Error, OSError, ValueError, TypeError):
                        state["reference_unavailable"] = True
                self._json(state)
                return
            if parsed.path == "/api/scheduled-paper":
                self._json(read_scheduled_config(self.server.data_directory))
                return
            if parsed.path == "/api/chart-strategy":
                attempts = {}
                live_status_available = False
                if getattr(self.server, "live_config", None):
                    try:
                        live = read_live_state(self.server.live_config, attempt_limit=200)
                        settings = _parse_config(getattr(self.server, "config_path", None))
                        enrich_live_attempts(live,
                            self.server.data_directory.parent / "chart-strategy" / "observations.sqlite",
                            self.server.data_directory / "shadow.sqlite",
                            settings["tick_max_delay_ms"])
                        attempts = {row["observation_id"]: row for row in live["attempts"]}
                        live_status_available = True
                    except (LiveAPIError, sqlite3.Error, OSError, ValueError, TypeError):
                        pass
                self._json({"observations": build_chart_observations(
                    self.server.data_directory.parent / "chart-strategy" / "observations.sqlite",
                    attempts_by_observation=attempts),
                    "live_status_available": live_status_available})
                return
            if parsed.path == "/static/lightweight-charts-4.2.3.js":
                body = (Path(__file__).parent / "static" / "lightweight-charts-4.2.3.js").read_bytes()
                self.send_response(200)
                self.send_header("Content-Type", "application/javascript; charset=utf-8")
                self.send_header("Content-Length", str(len(body)))
                self._security_headers()
                self.end_headers()
                self.wfile.write(body)
                return
            if parsed.path == "/static/dashboard.css":
                body = (Path(__file__).parent / "static" / "dashboard.css").read_bytes()
                self.send_response(200)
                self.send_header("Content-Type", "text/css; charset=utf-8")
                self.send_header("Content-Length", str(len(body)))
                self.send_header("Cache-Control", "no-store")
                self._security_headers()
                self.end_headers()
                self.wfile.write(body)
                return
            if parsed.path == "/static/dashboard-mobile.css":
                body = (Path(__file__).parent / "static" / "dashboard-mobile.css").read_bytes()
                self.send_response(200)
                self.send_header("Content-Type", "text/css; charset=utf-8")
                self.send_header("Content-Length", str(len(body)))
                self.send_header("Cache-Control", "no-store")
                self._security_headers()
                self.end_headers()
                self.wfile.write(body)
                return
            if parsed.path == "/static/dashboard-pages.css":
                body = (Path(__file__).parent / "static" / "dashboard-pages.css").read_bytes()
                self.send_response(200)
                self.send_header("Content-Type", "text/css; charset=utf-8")
                self.send_header("Content-Length", str(len(body)))
                self.send_header("Cache-Control", "no-store")
                self._security_headers()
                self.end_headers()
                self.wfile.write(body)
                return
            if parsed.path == "/api/overview":
                self._json(build_overview(
                    self.server.data_directory, self.server.market_db,
                    self.server.config_path,
                ))
                return
            if parsed.path == "/api/candles":
                mechanism = query.get("mechanism", ["all"])[0]
                data = build_candles(
                    self.server.data_directory, self.server.market_db,
                    interval=query.get("interval", ["1m"])[0],
                    limit=int(query.get("limit", ["240"])[0]),
                    horizon=query.get("horizon", ["all"])[0],
                    mechanism="all" if mechanism == "CHART_30M" else mechanism,
                )
                if mechanism in ("all", "CHART_30M") and query.get("horizon", ["all"])[0] != "60":
                    rows = build_chart_observations(self.server.data_directory.parent / "chart-strategy" / "observations.sqlite", limit=200)
                    markers = chart_strategy_markers(rows, data["candles"], data["interval_ms"])
                    data["markers"] = markers if mechanism == "CHART_30M" else data["markers"] + markers
                self._json(data)
                return
            if parsed.path == "/api/predictions":
                self._json({"predictions": build_predictions(
                    self.server.data_directory,
                    horizon=query.get("horizon", ["all"])[0],
                    mechanism=query.get("mechanism", ["all"])[0],
                    limit=int(query.get("limit", ["80"])[0]),
                )})
                return
            self._json({"error": "not_found"}, 404)
        except LiveAPIError as error:
            self._json({"error": str(error)}, 503)
        except (ValueError, OSError, sqlite3.Error):
            self._json({"error": "data_unavailable_or_invalid_request"}, 400)

    def do_POST(self):
        parsed = urlparse(self.path)
        if parsed.path not in ("/api/stop", "/api/scheduled-paper", "/api/live"):
            self._json({"error": "not_found"}, 404)
            return
        if parsed.path == "/api/live" and not getattr(self.server, "live_config", None):
            self._json({"error": "not_found"}, 404)
            return
        if self.client_address[0] not in ("127.0.0.1", "::1"):
            self._json({"error": "loopback_only"}, 403)
            return
        origin = self.headers.get("Origin")
        host = self.headers.get("Host")
        if origin and urlparse(origin).netloc != host:
            self._json({"error": "origin_mismatch"}, 403)
            return
        if parsed.path == "/api/live":
            try:
                if (origin != live_public_origin(self.server.live_config)
                        or host != urlparse(origin).netloc
                        or self.headers.get("Content-Type", "").split(";")[0].strip() != "application/json"):
                    self._json({"error": "origin_or_content_type_invalid"}, 403)
                    return
                length = int(self.headers.get("Content-Length", "0"))
                if not 0 < length <= 1024:
                    self._json({"error": "invalid_live_request"}, 400)
                    return
                body = json.loads(self.rfile.read(length))
                result = change_live_state_from_site(self.server.live_config, body,
                                                     now_ms=time.time_ns() // 1_000_000)
                self._json(result)
            except LiveAPIError as error:
                status = 403 if str(error) == "LIVE_AUTH_REQUIRED" else 409
                self._json({"error": str(error)}, status)
            except (ValueError, OSError, sqlite3.Error):
                self._json({"error": "invalid_or_unavailable"}, 400)
            return
        if parsed.path == "/api/scheduled-paper":
            if not origin or urlparse(origin).netloc != host or self.headers.get("Content-Type", "").split(";")[0].strip() != "application/json":
                self._json({"error":"origin_or_content_type_invalid"}, 403)
                return
            try:
                length = int(self.headers.get("Content-Length", "0"))
                if not 0 < length <= 1024:
                    raise ValueError("invalid body length")
                body = json.loads(self.rfile.read(length))
                save_scheduled_config(self.server.data_directory, body)
                self._json({"ok":True,"mode":"PAPER_ONLY"})
            except (ValueError, OSError, sqlite3.Error):
                self._json({"error":"invalid_or_unavailable"}, 400)
            return
        try:
            from .runner import request_safe_stop
            request_safe_stop(self.server.data_directory)
            self._json({"ok": True, "message": "安全停止请求已提交；等待 SHADOW 进程完成收尾"})
        except ValueError:
            self._json({"error": "no_active_runner_or_stop_already_requested"}, 409)
        except OSError:
            self._json({"error": "safe_stop_request_failed"}, 500)

    def log_message(self, fmt, *args):
        # Avoid logging query strings or local filesystem details.
        return


class LocalServer(ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = True


def serve(directory: Path, market_db: Path | None = None, config_path: Path | None = None, host="127.0.0.1", port=8765, live_config: Path | None = None):
    if host not in ("127.0.0.1", "localhost", "::1"):
        raise ValueError("dashboard must bind to loopback only")
    if directory.is_symlink() or not directory.is_dir():
        raise ValueError("SHADOW data directory must already exist and cannot be a symlink")
    directory = directory.resolve(strict=True)
    market_db = market_db or directory / "market.sqlite"
    if market_db.is_symlink():
        raise ValueError("market database cannot be a symlink")
    httpd = LocalServer((host, port), Handler)
    httpd.data_directory = directory
    httpd.market_db = market_db
    httpd.config_path = config_path
    httpd.live_config = live_config
    print(f"BTC SHADOW dashboard: http://{host}:{port}")
    print("Read-only dashboard; live control is separately armed only when configured.")
    try:
        httpd.serve_forever(poll_interval=0.5)
    except KeyboardInterrupt:
        pass
    finally:
        httpd.server_close()


def resolve_data_directory(config_path: Path):
    if config_path.is_symlink() or not config_path.is_file():
        raise ValueError("config file not found or is a symlink")
    try:
        config = json.loads(config_path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        raise ValueError("config file is unreadable") from None
    raw_directory = config.get("data_directory") if isinstance(config, dict) else None
    if not isinstance(raw_directory, str) or not raw_directory.strip():
        raise ValueError("config must define data_directory")
    # Match the runner: relative data_directory values resolve from the
    # process working directory, not from the config file's parent.
    return Path(raw_directory).expanduser()


def main():
    parser = argparse.ArgumentParser(description="Local read-only BTC SHADOW web dashboard")
    parser.add_argument("--directory", type=Path, help="existing isolated SHADOW data directory; defaults to config data_directory")
    parser.add_argument("--market-db", type=Path, help="market.sqlite path; defaults to DIRECTORY/market.sqlite")
    parser.add_argument("--config", type=Path, help="optional config JSON; only non-secret settings are displayed")
    parser.add_argument("--live-config", type=Path, help="private live control settings; default unavailable")
    parser.add_argument("--host", default="127.0.0.1", choices=("127.0.0.1", "localhost", "::1"))
    parser.add_argument("--port", type=int, default=8765)
    args = parser.parse_args()
    if not 1 <= args.port <= 65535:
        parser.error("--port must be between 1 and 65535")
    directory = args.directory
    if directory is None:
        if args.config is None:
            parser.error("provide --directory or --config with data_directory")
        try:
            directory = resolve_data_directory(args.config)
        except ValueError as exc:
            parser.error(str(exc))
    serve(directory, args.market_db, args.config, args.host, args.port, args.live_config)


if __name__ == "__main__":
    main()
