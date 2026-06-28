"""
DERIV SCALPER BOT v5.0
Bot + Web Dashboard in one file
Railway web service with live monitoring UI
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

# ── CONFIG ────────────────────────────────────────────
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

# ── SHARED STATE ──────────────────────────────────────
state = {
    "status":        "Starting...",
    "connected":     False,
    "balance":       0.0,
    "start_balance": 0.0,
    "trades":        [],
    "trades_today":  0,
    "wins":          0,
    "losses":        0,
    "daily_pnl":     0.0,
    "last_signal":   "—",
    "last_price":    0.0,
    "symbol":        SYMBOL,
    "logs":          [],
    "is_trading":    False,
    "started_at":    datetime.now().strftime("%Y-%m-%d %H:%M:%S")
}

price_history = deque(maxlen=50)
is_trading    = False

# ── LOGGING ───────────────────────────────────────────
def log(msg, level="INFO"):
    icons = {"INFO":"ℹ️","TRADE":"💰","WIN":"✅","LOSS":"❌","WARN":"⚠️","ERROR":"🔴"}
    ts   = datetime.now().strftime("%H:%M:%S")
    line = f"[{ts}] {icons.get(level,'•')} {msg}"
    print(line, flush=True)
    state["logs"].insert(0, {"time": ts, "msg": msg, "level": level})
    if len(state["logs"]) > 50:
        state["logs"].pop()

# ── INDICATORS ────────────────────────────────────────
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

# ── DERIV CONNECTION ──────────────────────────────────
def get_ws_url():
    headers = {
        "Authorization": f"Bearer {API_TOKEN}",
        "Deriv-App-ID":  APP_ID,
        "Content-Type":  "application/json"
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
            log("Connecting...")
            async with websockets.connect(ws_url) as ws:
                state["connected"] = True
                state["status"]    = "🟢 Live — Trading Active"
                log("Connected to Deriv!", "INFO")
                await ws.send(json.dumps({"balance": 1, "subscribe": 1}))
                await ws.send(json.dumps({"ticks": SYMBOL, "subscribe": 1}))
                log(f"Subscribed to {SYMBOL}")

                async for raw in ws:
                    data     = json.loads(raw)
                    msg_type = data.get("msg_type")

                    if msg_type == "tick":
                        price = float(data["tick"]["quote"])
                        price_history.append(price)
                        state["last_price"] = price

                        if state["start_balance"] == 0:
                            continue

                        daily_loss = state["start_balance"] - state["balance"]
                        if daily_loss >= MAX_DAILY_LOSS:
                            state["status"] = "🛑 Daily loss limit hit"
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
                        state["balance"]   = bal
                        state["daily_pnl"] = bal - state["start_balance"]

                    elif msg_type == "buy":
                        if "error" in data:
                            log(f"Trade error: {data['error']['message']}", "ERROR")
                            is_trading = False
                            state["is_trading"] = False
                        else:
                            cid = data["buy"]["contract_id"]
                            state["trades_today"] += 1
                            log(f"Trade #{state['trades_today']} opened", "TRADE")
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
                                "time":   datetime.now().strftime("%H:%M:%S"),
                                "type":   state["last_signal"],
                                "result": result,
                                "profit": profit,
                                "balance": state["balance"]
                            })
                            if len(state["trades"]) > 20:
                                state["trades"].pop()
                            log(f"{result} ${abs(profit):.2f} | Balance: ${state['balance']:.2f}", result)
                            is_trading = False
                            state["is_trading"] = False

                    elif "error" in data:
                        log(f"API: {data['error']['message']}", "ERROR")

        except Exception as e:
            state["connected"] = False
            state["status"]    = f"⚠️ Reconnecting..."
            log(f"Connection lost: {e} — retrying in 5s...", "WARN")
            await asyncio.sleep(5)

# ── WEB DASHBOARD ─────────────────────────────────────
DASHBOARD_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<meta http-equiv="refresh" content="5">
<title>DerivScalper Dashboard</title>
<style>
@import url('https://fonts.googleapis.com/css2?family=Space+Grotesk:wght@400;500;600;700&family=Space+Mono:wght@400;700&display=swap');
:root{--bg:#08090c;--card:#0f1117;--border:#1a1d27;--accent:#00e5a0;--red:#f87171;--yellow:#fbbf24;--text:#e2e8f0;--muted:#4b5563;--mono:'Space Mono',monospace;--sans:'Space Grotesk',sans-serif}
*{box-sizing:border-box;margin:0;padding:0}
body{background:var(--bg);color:var(--text);font-family:var(--sans);min-height:100vh;padding:16px}
.header{display:flex;align-items:center;justify-content:space-between;margin-bottom:20px;padding-bottom:16px;border-bottom:1px solid var(--border)}
.logo{font-family:var(--mono);font-size:13px;color:var(--accent);letter-spacing:2px}
.status-pill{font-size:11px;padding:4px 10px;border-radius:20px;font-family:var(--mono);background:rgba(0,229,160,0.1);color:var(--accent);border:1px solid rgba(0,229,160,0.2)}
.status-pill.warn{background:rgba(251,191,36,0.1);color:var(--yellow);border-color:rgba(251,191,36,0.2)}
.grid{display:grid;grid-template-columns:1fr 1fr;gap:10px;margin-bottom:16px}
.card{background:var(--card);border:1px solid var(--border);border-radius:10px;padding:14px}
.card-label{font-size:10px;color:var(--muted);text-transform:uppercase;letter-spacing:1.5px;margin-bottom:6px;font-family:var(--mono)}
.card-value{font-family:var(--mono);font-size:22px;font-weight:700;color:var(--accent)}
.card-value.red{color:var(--red)}
.card-value.yellow{color:var(--yellow)}
.card-value.white{color:#fff}
.card-sub{font-size:11px;color:var(--muted);margin-top:4px}
.section-title{font-size:11px;color:var(--muted);text-transform:uppercase;letter-spacing:2px;font-family:var(--mono);margin:16px 0 8px}
.trade-row{display:flex;justify-content:space-between;align-items:center;padding:10px 14px;background:var(--card);border:1px solid var(--border);border-radius:8px;margin-bottom:6px;font-size:12px}
.trade-time{color:var(--muted);font-family:var(--mono);font-size:10px}
.trade-type{font-family:var(--mono);font-weight:700;color:#93c5fd}
.trade-win{color:var(--accent);font-family:var(--mono);font-weight:700}
.trade-loss{color:var(--red);font-family:var(--mono);font-weight:700}
.log-row{padding:8px 12px;background:var(--card);border-left:2px solid var(--border);margin-bottom:4px;font-size:11px;font-family:var(--mono);color:var(--muted);border-radius:0 6px 6px 0}
.log-row.win{border-color:var(--accent);color:var(--accent)}
.log-row.loss{border-color:var(--red);color:var(--red)}
.log-row.trade{border-color:#93c5fd;color:#93c5fd}
.log-row.warn{border-color:var(--yellow);color:var(--yellow)}
.pnl-pos{color:var(--accent)}
.pnl-neg{color:var(--red)}
.empty{text-align:center;color:var(--muted);font-size:12px;padding:20px;font-family:var(--mono)}
.refresh-note{text-align:center;color:var(--muted);font-size:10px;font-family:var(--mono);margin-top:16px;padding-top:12px;border-top:1px solid var(--border)}
</style>
</head>
<body>
<div class="header">
  <div class="logo">DERIV SCALPER v5.0</div>
  <div class="status-pill {status_class}">{status}</div>
</div>

<div class="grid">
  <div class="card">
    <div class="card-label">Balance</div>
    <div class="card-value">${balance}</div>
    <div class="card-sub">Started at ${start_balance}</div>
  </div>
  <div class="card">
    <div class="card-label">Today's P&L</div>
    <div class="card-value {pnl_class}">{pnl_sign}${daily_pnl}</div>
    <div class="card-sub">{trades_today} trades today</div>
  </div>
  <div class="card">
    <div class="card-label">Wins</div>
    <div class="card-value">{wins}</div>
    <div class="card-sub">Win rate: {winrate}%</div>
  </div>
  <div class="card">
    <div class="card-label">Losses</div>
    <div class="card-value red">{losses}</div>
    <div class="card-sub">Symbol: {symbol}</div>
  </div>
</div>

<div class="card" style="margin-bottom:16px">
  <div class="card-label">Live Price</div>
  <div class="card-value white">{last_price}</div>
  <div class="card-sub">Last signal: {last_signal} | {is_trading_str}</div>
</div>

<div class="section-title">Recent Trades</div>
{trades_html}

<div class="section-title">Bot Logs</div>
{logs_html}

<div class="refresh-note">Auto-refreshes every 5 seconds · Started {started_at}</div>
</body>
</html>"""

class DashboardHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == "/health":
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"OK")
            return

        s = state
        total = s["wins"] + s["losses"]
        winrate = round((s["wins"]/total)*100) if total > 0 else 0
        pnl = s["daily_pnl"]
        pnl_class = "pnl-pos" if pnl >= 0 else "pnl-neg"
        pnl_sign  = "+" if pnl >= 0 else ""
        status_class = "" if s["connected"] else "warn"

        trades_html = ""
        if s["trades"]:
            for t in s["trades"]:
                rc = "trade-win" if t["result"]=="WIN" else "trade-loss"
                ps = "+" if t["profit"]>0 else ""
                trades_html += f"""<div class="trade-row">
                  <span class="trade-time">{t['time']}</span>
                  <span class="trade-type">{t['type']}</span>
                  <span class="{rc}">{ps}${abs(t['profit']):.2f}</span>
                  <span style="color:#6b7280;font-size:10px">${t['balance']:.2f}</span>
                </div>"""
        else:
            trades_html = '<div class="empty">No trades yet — waiting for signals...</div>'

        logs_html = ""
        for l in s["logs"][:15]:
            lc = l["level"].lower()
            logs_html += f'<div class="log-row {lc}">[{l["time"]}] {l["msg"]}</div>'

        html = DASHBOARD_HTML.format(
            status       = s["status"],
            status_class = status_class,
            balance      = f"{s['balance']:.2f}",
            start_balance= f"{s['start_balance']:.2f}",
            daily_pnl    = f"{abs(pnl):.2f}",
            pnl_class    = pnl_class,
            pnl_sign     = pnl_sign,
            trades_today = s["trades_today"],
            wins         = s["wins"],
            losses       = s["losses"],
            winrate      = winrate,
            symbol       = s["symbol"],
            last_price   = f"{s['last_price']:.4f}" if s["last_price"] else "—",
            last_signal  = s["last_signal"],
            is_trading_str = "🔄 Trade open" if s["is_trading"] else "⏳ Watching",
            trades_html  = trades_html,
            logs_html    = logs_html,
            started_at   = s["started_at"]
        )

        self.send_response(200)
        self.send_header("Content-Type", "text/html")
        self.end_headers()
        self.wfile.write(html.encode())

    def log_message(self, *args):
        pass  # silence HTTP logs

def run_web_server():
    server = HTTPServer(("0.0.0.0", PORT), DashboardHandler)
    log(f"Dashboard running on port {PORT}")
    server.serve_forever()

# ── ENTRY ─────────────────────────────────────────────
if __name__ == "__main__":
    print("DERIV SCALPER BOT v5.0 — Bot + Dashboard", flush=True)

    if not API_TOKEN:
        log("No API_TOKEN set!", "ERROR")
        exit(1)

    log(f"Token: {API_TOKEN[:8]}...")
    log(f"Account: {ACCOUNT_ID}")
    log(f"Symbol: {SYMBOL} | Stake: ${STAKE}")

    # Start web server in background thread
    web_thread = threading.Thread(target=run_web_server, daemon=True)
    web_thread.start()

    # Run bot
    asyncio.run(bot_loop())
