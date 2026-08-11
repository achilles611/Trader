from __future__ import annotations

import asyncio
from typing import Any

from .config import CopyTradeConfig
from .hyperliquid import WatchHealth
from .storage import CopyTradeDatabase


def create_dashboard_app(config: CopyTradeConfig, database: CopyTradeDatabase, health: WatchHealth | None = None) -> Any:
    """Build a deliberately small FastAPI dashboard with a live JSON websocket."""
    try:
        from fastapi import FastAPI, WebSocket, WebSocketDisconnect
        from fastapi.responses import HTMLResponse
    except ImportError as exc:
        raise RuntimeError("copy-dashboard requires fastapi and uvicorn; install requirements.txt.") from exc

    app = FastAPI(title="Trader Copytrade Alpha", docs_url=None, redoc_url=None)

    def snapshot() -> dict[str, Any]:
        payload = database.dashboard_snapshot()
        positions = payload["positions"]
        committed = sum(float(position["remaining_capital"]) for position in positions)
        unrealized = sum(float(position.get("unrealized_pnl") or 0.0) for position in positions)
        all_positions = payload["all_positions"]
        cash = config.capital.initial_capital - committed - sum(float(position["entry_fee"]) for position in all_positions) + sum(float(position["realized_pnl"]) for position in all_positions)
        follower_pnl = sum(float(position["realized_pnl"]) - float(position["entry_fee"]) for position in all_positions)
        target_pnl = sum(float(metric["net_pnl"]) for metric in payload["metrics"])
        curve = payload["portfolio_curve"]
        if curve:
            cash = float(curve[-1]["cash"])
            committed = float(curve[-1]["committed_capital"])
            equity = float(curve[-1]["equity"])
            drawdown = float(curve[-1]["drawdown_fraction"])
        else:
            equity, drawdown = cash + committed + unrealized, 0.0
        payload.update({
            "mode": config.mode, "initial_capital": config.capital.initial_capital,
            "committed_capital": committed, "unrealized_pnl": unrealized, "free_capital": cash, "live_equity": equity, "live_drawdown": drawdown,
            "follower_pnl": follower_pnl, "target_pnl": target_pnl,
            "risk": {"kill_switch": config.risk.kill_switch_path.exists(), "max_committed_fraction": config.risk.max_total_committed_fraction},
            "health": (health or WatchHealth()).as_dict(),
        })
        return payload

    @app.get("/api/snapshot")
    async def api_snapshot() -> dict[str, Any]:
        return snapshot()

    @app.get("/api/health")
    async def api_health() -> dict[str, Any]:
        return snapshot()["health"]

    @app.websocket("/ws")
    async def websocket_updates(websocket: WebSocket) -> None:
        await websocket.accept()
        try:
            while True:
                await websocket.send_json(snapshot())
                await asyncio.sleep(1)
        except WebSocketDisconnect:
            return

    @app.get("/", response_class=HTMLResponse)
    async def root() -> str:
        return _DASHBOARD_HTML

    return app


def serve_dashboard(config: CopyTradeConfig, database: CopyTradeDatabase, health: WatchHealth | None = None) -> None:
    try:
        import uvicorn
    except ImportError as exc:
        raise RuntimeError("copy-dashboard requires uvicorn; install requirements.txt.") from exc
    uvicorn.run(create_dashboard_app(config, database, health), host=config.artifacts.dashboard_host, port=config.artifacts.dashboard_port)


_DASHBOARD_HTML = """<!doctype html>
<html><head><meta charset="utf-8"><title>Copy-trading paper dashboard</title>
<style>
body {background:#10131a;color:#e8eef7;font:14px system-ui;margin:24px} h1{margin:0 0 4px}.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(180px,1fr));gap:12px;margin:20px 0}.card,table{background:#171c26;border:1px solid #2a3445;border-radius:8px;padding:12px}table{width:100%;border-collapse:collapse}td,th{padding:8px;text-align:left;border-bottom:1px solid #2a3445}.ok{color:#3ecf8e}.warn{color:#f8c35a}.bad{color:#f97068}</style>
</head><body><h1>Copy-trading paper dashboard</h1><div id="subtitle">Connecting...</div><div class="grid" id="stats"></div>
<h2>Target candidates</h2><table id="targets"><tr><th>Wallet</th><th>Status</th><th>Account equity</th><th>Coverage</th><th>Score</th><th>Eligible</th></tr></table>
<h2>Current target position state</h2><table id="targetPositions"><tr><th>Target</th><th>Observed positions</th><th>Snapshot time</th></tr></table>
<h2>Virtual sleeves</h2><table id="positions"><tr><th>Target</th><th>Symbol</th><th>Direction</th><th>Exposure</th><th>Mark</th><th>Unrealized P&amp;L</th><th>Realized P&amp;L</th></tr></table>
<h2>Latest target fills / market markers</h2><table id="fills"><tr><th>Time</th><th>Target</th><th>Symbol</th><th>Side</th><th>Price</th><th>Quantity</th></tr></table>
<canvas id="marketChart" width="960" height="260" aria-label="Target and follower price markers"></canvas>
<canvas id="equityChart" width="960" height="220" aria-label="Live follower equity curve"></canvas>
<h2>Simulated follower trade markers</h2><table id="execution"><tr><th>Time</th><th>Sleeve</th><th>Price</th><th>Quantity</th><th>Slippage</th></tr></table>
<h2>Recent skipped or filled signals</h2><table id="attempts"><tr><th>Target</th><th>Symbol</th><th>Action</th><th>Status</th><th>Reason</th><th>Latency ms</th></tr></table>
<script>
const dollar=v=>'$'+Number(v||0).toFixed(2), clear=id=>document.querySelector('#'+id).innerHTML=document.querySelector('#'+id+' tr').outerHTML;
function cell(v){const e=document.createElement('td');e.textContent=v;return e} function render(d){
 document.querySelector('#subtitle').textContent=`${d.mode.toUpperCase()} · ${d.health.state} · kill switch: ${d.risk.kill_switch?'ON':'off'}`;
 document.querySelector('#stats').innerHTML=[['Live equity',dollar(d.live_equity)],['Live drawdown',(100*d.live_drawdown).toFixed(2)+'%'],['Free capital',dollar(d.free_capital)],['Committed',dollar(d.committed_capital)],['Unrealized P&L',dollar(d.unrealized_pnl)],['Target P&L',dollar(d.target_pnl)],['Follower P&L',dollar(d.follower_pnl)],['Health',d.health.state]].map(x=>`<div class="card"><div>${x[0]}</div><strong>${x[1]}</strong></div>`).join('');
 clear('targets'); const t=document.querySelector('#targets'); d.scores.forEach(s=>{const snap=d.target_snapshots.find(x=>x.target_wallet===s.target_wallet)||{},coverage=d.backfill_coverage.find(x=>x.target_wallet===s.target_wallet)||{};let r=document.createElement('tr');[s.target_wallet,(d.targets.find(x=>x.wallet===s.target_wallet)||{}).status||'unknown',dollar(snap.account_value),coverage.coverage_state||'UNPROVEN',Number(s.total_score).toFixed(1),s.eligible?'yes':'no'].forEach(x=>r.append(cell(x)));t.append(r)});
 clear('targetPositions');const tp=document.querySelector('#targetPositions');d.target_snapshots.forEach(x=>{let r=document.createElement('tr');[x.target_wallet,JSON.stringify(x.positions),new Date(x.snapshot_timestamp).toLocaleString()].forEach(v=>r.append(cell(v)));tp.append(r)});
 clear('positions');const p=document.querySelector('#positions');d.positions.forEach(x=>{let r=document.createElement('tr');[x.target_wallet,x.symbol,x.direction,dollar(x.remaining_capital),x.current_mark||'—',dollar(x.unrealized_pnl),dollar(x.realized_pnl)].forEach(v=>r.append(cell(v)));p.append(r)});
 clear('fills');const f=document.querySelector('#fills');d.latest_fills.slice().reverse().forEach(x=>{let r=document.createElement('tr');[new Date(x.event_timestamp).toLocaleTimeString(),x.target_wallet,x.symbol,x.side,x.price,x.base_quantity].forEach(v=>r.append(cell(v)));f.append(r)});
 clear('execution');const e=document.querySelector('#execution');d.execution_fills.slice().reverse().forEach(x=>{let r=document.createElement('tr');[new Date(x.timestamp).toLocaleTimeString(),x.sleeve_id,x.price,x.quantity,x.slippage_bps+' bps'].forEach(v=>r.append(cell(v)));e.append(r)});
 clear('attempts');const a=document.querySelector('#attempts');d.execution_attempts.slice().reverse().forEach(x=>{let r=document.createElement('tr');[x.target_wallet,x.symbol,x.action,x.status,x.reason,x.detection_latency_ms].forEach(v=>r.append(cell(v)));a.append(r)});
 const c=document.querySelector('#marketChart'),ctx=c.getContext('2d'),ticks=d.latest_fills.slice().reverse();ctx.fillStyle='#171c26';ctx.fillRect(0,0,c.width,c.height);if(ticks.length){const prices=ticks.map(x=>Number(x.price));const lo=Math.min(...prices),hi=Math.max(...prices),span=hi-lo||1;ctx.strokeStyle='#47b8e0';ctx.beginPath();ticks.forEach((x,i)=>{const px=20+i*(c.width-40)/Math.max(ticks.length-1,1),py=c.height-25-(Number(x.price)-lo)*(c.height-50)/span;i?ctx.lineTo(px,py):ctx.moveTo(px,py)});ctx.stroke();d.execution_fills.forEach(x=>{const nearest=ticks.reduce((best,t)=>Math.abs(new Date(t.event_timestamp)-new Date(x.timestamp))<Math.abs(new Date(best.event_timestamp)-new Date(x.timestamp))?t:best,ticks[0]);const i=ticks.indexOf(nearest),px=20+i*(c.width-40)/Math.max(ticks.length-1,1),py=c.height-25-(Number(x.price)-lo)*(c.height-50)/span;ctx.fillStyle='#3ecf8e';ctx.beginPath();ctx.arc(px,py,4,0,Math.PI*2);ctx.fill()});ctx.fillStyle='#c7d1df';ctx.fillText(`${ticks[0].symbol} target price line; green = simulated fills`,20,16)}
 const ec=document.querySelector('#equityChart'),ex=ec.getContext('2d'),curve=d.portfolio_curve;ex.fillStyle='#171c26';ex.fillRect(0,0,ec.width,ec.height);if(curve.length){const vals=curve.map(x=>Number(x.equity)),lo=Math.min(...vals),hi=Math.max(...vals),span=hi-lo||1;ex.strokeStyle='#3ecf8e';ex.beginPath();vals.forEach((v,i)=>{const px=20+i*(ec.width-40)/Math.max(vals.length-1,1),py=ec.height-25-(v-lo)*(ec.height-50)/span;i?ex.lineTo(px,py):ex.moveTo(px,py)});ex.stroke();ex.fillStyle='#c7d1df';ex.fillText('Live follower equity curve',20,16)}
} const ws=new WebSocket((location.protocol==='https:'?'wss':'ws')+'://'+location.host+'/ws');ws.onmessage=e=>render(JSON.parse(e.data));ws.onclose=()=>setTimeout(()=>location.reload(),1000);
</script></body></html>"""
