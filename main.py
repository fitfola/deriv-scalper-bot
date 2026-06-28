"""
DERIV SCALPER BOT v5.1
Bot + Web Dashboard - CSS vars fixed
"""

import asyncio
import json
import os
import threading
import requests
from datetime import datetime
from collections import deque
from http.server import HTTPServer, BaseHTTPRequestHandler
import websockets

API_TOKEN      = os.environ.get("API_TOKEN", "")
APP_ID         = os.environ.get("APP_ID", "33G9IntANaJzG3qeRKAPk")
ACCOUNT_ID     = os.environ.get("ACCOUNT_ID", "DOT93156522")
SYMBOL         = os.environ.get("SYMBOL", "1HZ50V")
STAKE          = float(os.environ.get("STAKE", "0.35"))
DURATION       = int(os.environ.get("DURATION", "5"))
DURATION_UNIT  = os.environ.get("DURATION_UNIT", "t")
MAX_DAILY_LOSS = float(os.environ.get("MAX_DAILY_LOSS", "3.00"))
PORT           = int(os.environ.get("PORT", "8080"))
API_BASE       = "https://api.derivws.com"

state = {
    "status": "Starting...",
    "connected": False,
    "balance": 0.0,
    "start_balance": 0.0,
    "trades": [],
    "trades_today": 0,
    "wins": 0,
    "losses": 0,
    "daily_pnl": 0.0,
    "last_signal": "None yet",
    "last_price": 0.0,
    "symbol": SYMBOL,
    "logs": [],
    "is_trading": False,
    "started_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
}

price_history = deque(maxlen=50)
is_trading = False

def log(msg, level="INFO"):
    icons = {"INFO":"ℹ️","TRADE":"💰","WIN":"✅","LOSS":"❌","WARN":"⚠️","ERROR":"🔴"}
    ts = datetime.now().strftime("%H:%M:%S")
    line = f"[{ts}] {icons.get(level,'.')} {msg}"
    print(line, flush=True)
    state["logs"].insert(0, {"time": ts, "msg": msg, "level": level})
    if len(state["logs"]) > 50:
        state["logs"].pop()

def ema(prices, period):
    if len(prices) < period:
        return None
    p = list(prices)
    k = 2 / (period + 1)
    v = sum(p[:period]) / period
    for x in p[period:]:
        v = x * k + v * (1 - k)
    return v

def rsi(prices, period=7):
    if len(prices) < period + 1:
        return None
    p = list(prices)
    d = [p[i+1]-p[i] for i in range(len(p)-1)][-period:]
    g = sum(x for x in d if x > 0) / period
    l = sum(-x for x in d if x < 0) / period
    return 100 if l == 0 else 100 - (100/(1+g/l))

def get_signal():
    if len(price_history) < 22:
        return None
    p = list(price_history)
    f1 = ema(deque(p), 5)
    s1 = ema(deque(p), 20)
    f0 = ema(deque(p[:-1]), 5)
    s0 = ema(deque(p[:-1]), 20)
    r  = rsi(deque(p), 7)
    if None in [f1, s1, f0, s0, r]:
        return None
    log(f"EMA5={f1:.2f} EMA20={s1:.2f} RSI={r:.1f}")
    if f0 < s0 and f1 > s1 and r < 60:
        return "CALL"
    if f0 > s0 and f1 < s1 and r > 40:
        return "PUT"
    return None

def get_ws_url():
    headers = {
        "Authorization": f"Bearer {API_TOKEN}",
        "Deriv-App-ID": APP_ID,
        "Content-Type": "application/json"
    }
    resp = requests.post(
        f"{API_BASE}/trading/v1/options/accounts/{ACCOUNT_ID}/otp",
        headers=headers, timeout=15
    )
    if resp.status_code != 200:
        raise Exception(f"OTP failed: {resp.text}")
    return resp.json()["data"]["url"]

async def place_trade(ws, direction):
    global is_trading
    is_trading = True
    state["is_trading"] = True
    await ws.send(json.dumps({
        "buy": 1, "price": STAKE,
        "parameters": {
            "amount": STAKE, "basis": "stake",
            "contract_type": direction,
            "currency": "USD",
            "duration": DURATION,
            "duration_unit": DURATION_UNIT,
            "symbol": SYMBOL
        }
    }))

async def bot_loop():
    global is_trading
    while True:
        try:
            state["status"] = "Connecting to Deriv..."
            log("Getting WebSocket URL...")
            ws_url = get_ws_url()
            log("Connecting to Deriv...")
            async with websockets.connect(ws_url) as ws:
                state["connected"] = True
                state["status"] = "Live - Trading Active"
                log("Connected!", "INFO")
                await ws.send(json.dumps({"balance": 1, "subscribe": 1}))
                await ws.send(json.dumps({"ticks": SYMBOL, "subscribe": 1}))
                log(f"Subscribed to {SYMBOL}")
                async for raw in ws:
                    data = json.loads(raw)
                    msg_type = data.get("msg_type")
                    if msg_type == "tick":
                        price = float(data["tick"]["quote"])
                        price_history.append(price)
                        state["last_price"] = price
                        if state["start_balance"] == 0:
                            continue
                        daily_loss = state["start_balance"] - state["balance"]
                        if daily_loss >= MAX_DAILY_LOSS:
                            state["status"] = "Daily loss limit hit - Paused"
                            continue
                        if is_trading:
                            continue
                        signal = get_signal()
                        if signal:
                            state["last_signal"] = signal
                            log(f"Signal: {signal} at {price:.4f}", "TRADE")
                            await place_trade(ws, signal)
                    elif msg_type == "balance":
                        bal = float(data["balance"]["balance"])
                        if state["start_balance"] == 0:
                            state["start_balance"] = bal
                            log(f"Balance: ${bal:.2f}")
                        state["balance"] = bal
                        state["daily_pnl"] = bal - state["start_balance"]
                    elif msg_type == "buy":
                        if "error" in data:
                            log(f"Trade error: {data['error']['message']}", "ERROR")
                            is_trading = False
                            state["is_trading"] = False
                        else:
                            cid = data["buy"]["contract_id"]
                            state["trades_today"] += 1
                            log(f"Trade opened ID:{cid}", "TRADE")
                            await ws.send(json.dumps({
                                "proposal_open_contract": 1,
                                "contract_id": cid,
                                "subscribe": 1
                            }))
                    elif msg_type == "proposal_open_contract":
                        c = data.get("proposal_open_contract", {})
                        if c.get("is_expired") or c.get("status") == "sold":
                            profit = float(c.get("profit", 0))
                            state["balance"] += profit
                            state["daily_pnl"] += profit
                            result = "WIN" if profit > 0 else "LOSS"
                            if profit > 0:
                                state["wins"] += 1
                            else:
                                state["losses"] += 1
                            state["trades"].insert(0, {
                                "time": datetime.now().strftime("%H:%M:%S"),
                                "type": state["last_signal"],
                                "result": result,
                                "profit": profit,
                                "balance": state["balance"]
                            })
                            if len(state["trades"]) > 20:
                                state["trades"].pop()
                            log(f"{result} ${abs(profit):.2f} Balance:${state['balance']:.2f}", result)
                            is_trading = False
                            state["is_trading"] = False
                    elif "error" in data:
                        log(f"API: {data['error']['message']}", "ERROR")
        except Exception as e:
            state["connected"] = False
            state["status"] = "Reconnecting..."
            log(f"Lost connection: {e} retrying in 5s", "WARN")
            await asyncio.sleep(5)

def build_html():
    s = state
    total = s["wins"] + s["losses"]
    winrate = round((s["wins"]/total)*100) if total > 0 else 0
    pnl = s["daily_pnl"]
    pnl_color = "#00e5a0" if pnl >= 0 else "#f87171"
    pnl_sign = "+" if pnl >= 0 else ""
    status_color = "#00e5a0" if s["connected"] else "#fbbf24"

    trades_html = ""
    if s["trades"]:
        for t in s["trades"]:
            color = "#00e5a0" if t["result"] == "WIN" else "#f87171"
            ps = "+" if t["profit"] > 0 else ""
            trades_html += f"""
            <tr>
              <td style="color:#6b7280;font-size:11px">{t['time']}</td>
              <td style="color:#93c5fd;font-weight:700">{t['type']}</td>
              <td style="color:{color};font-weight:700">{t['result']}</td>
              <td style="color:{color};font-weight:700">{ps}${abs(t['profit']):.2f}</td>
              <td style="color:#9ca3af">${t['balance']:.2f}</td>
            </tr>"""
    else:
        trades_html = '<tr><td colspan="5" style="text-align:center;color:#4b5563;padding:20px">No trades yet — waiting for signals...</td></tr>'

    logs_html = ""
    colors = {"WIN":"#00e5a0","LOSS":"#f87171","TRADE":"#93c5fd","WARN":"#fbbf24","ERROR":"#f87171","INFO":"#4b5563"}
    for l in s["logs"][:20]:
        c = colors.get(l["level"], "#4b5563")
        logs_html += f'<div style="padding:6px 10px;border-left:2px solid {c};margin-bottom:3px;font-size:11px;color:{c};background:#0a0c10;border-radius:0 4px 4px 0">[{l["time"]}] {l["msg"]}</div>'

    price_str = f"{s['last_price']:.4f}" if s["last_price"] else "—"
    trading_str = "Trade Open" if s["is_trading"] else "Watching"

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<meta http-equiv="refresh" content="5">
<title>DerivScalper Dashboard</title>
<style>
body{{background:#08090c;color:#e2e8f0;font-family:'Segoe UI',sans-serif;margin:0;padding:16px;min-height:100vh}}
.header{{display:flex;align-items:center;justify-content:space-between;margin-bottom:20px;padding-bottom:14px;border-bottom:1px solid #1a1d27}}
.logo{{font-family:monospace;font-size:13px;color:#00e5a0;letter-spacing:2px;font-weight:700}}
.pill{{font-size:11px;padding:4px 12px;border-radius:20px;font-family:monospace;background:rgba(0,229,160,0.1);color:{status_color};border:1px solid {status_color}40}}
.grid{{display:grid;grid-template-columns:1fr 1fr;gap:10px;margin-bottom:14px}}
.card{{background:#0f1117;border:1px solid #1a1d27;border-radius:10px;padding:14px}}
.label{{font-size:10px;color:#4b5563;text-transform:uppercase;letter-spacing:1.5px;margin-bottom:6px;font-family:monospace}}
.value{{font-family:monospace;font-size:20px;font-weight:700}}
.sub{{font-size:11px;color:#4b5563;margin-top:4px}}
.section{{font-size:10px;color:#4b5563;text-transform:uppercase;letter-spacing:2px;font-family:monospace;margin:14px 0 8px}}
table{{width:100%;border-collapse:collapse;background:#0f1117;border:1px solid #1a1d27;border-radius:10px;overflow:hidden}}
th{{padding:8px 12px;font-size:10px;color:#4b5563;text-transform:uppercase;letter-spacing:1px;font-family:monospace;text-align:left;border-bottom:1px solid #1a1d27}}
td{{padding:8px 12px;border-bottom:1px solid #0d1017;font-family:monospace;font-size:12px}}
.note{{text-align:center;color:#374151;font-size:10px;font-family:monospace;margin-top:16px;padding-top:12px;border-top:1px solid #1a1d27}}
</style>
</head>
<body>
<div class="header">
  <div class="logo">DERIV SCALPER v5.1</div>
  <div class="pill">{s['status']}</div>
</div>

<div class="grid">
  <div class="card">
    <div class="label">Balance</div>
    <div class="value" style="color:#00e5a0">${s['balance']:.2f}</div>
    <div class="sub">Started at ${s['start_balance']:.2f}</div>
  </div>
  <div class="card">
    <div class="label">Today P&L</div>
    <div class="value" style="color:{pnl_color}">{pnl_sign}${abs(pnl):.2f}</div>
    <div class="sub">{s['trades_today']} trades today</div>
  </div>
  <div class="card">
    <div class="label">Wins</div>
    <div class="value" style="color:#00e5a0">{s['wins']}</div>
    <div class="sub">Win rate: {winrate}%</div>
  </div>
  <div class="card">
    <div class="label">Losses</div>
    <div class="value" style="color:#f87171">{s['losses']}</div>
    <div class="sub">Symbol: {s['symbol']}</div>
  </div>
</div>

<div class="card" style="margin-bottom:14px">
  <div class="label">Live Price</div>
  <div class="value" style="color:#fff">{price_str}</div>
  <div class="sub">Last signal: {s['last_signal']} &nbsp;|&nbsp; {trading_str}</div>
</div>

<div class="section">Recent Trades</div>
<table>
  <tr><th>Time</th><th>Type</th><th>Result</th><th>Profit</th><th>Balance</th></tr>
  {trades_html}
</table>

<div class="section">Bot Logs</div>
{logs_html}

<div class="note">Auto-refreshes every 5s &nbsp;|&nbsp; Started {s['started_at']}</div>
</body>
</html>"""

class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        try:
            html = build_html()
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.end_headers()
            self.wfile.write(html.encode("utf-8"))
        except Exception as e:
            self.send_response(500)
            self.end_headers()
            self.wfile.write(f"Error: {e}".encode())

    def log_message(self, *args):
        pass

def run_server():
    server = HTTPServer(("0.0.0.0", PORT), Handler)
    log(f"Dashboard on port {PORT}")
    server.serve_forever()

if __name__ == "__main__":
    print("DERIV SCALPER BOT v5.1", flush=True)
    if not API_TOKEN:
        log("No API_TOKEN!", "ERROR")
        exit(1)
    log(f"Token: {API_TOKEN[:8]}...")
    log(f"Account: {ACCOUNT_ID} | Symbol: {SYMBOL} | Stake: ${STAKE}")
    t = threading.Thread(target=run_server, daemon=True)
    t.start()
    asyncio.run(bot_loop())
