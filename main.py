"""
DERIV SCALPER BOT v5.2
Fixed: proposal → buy flow | Clean minimal dashboard
"""

import asyncio
import json
import os
import threading
from datetime import datetime
from collections import deque
from http.server import HTTPServer, BaseHTTPRequestHandler
import websockets

API_TOKEN      = os.environ.get("API_TOKEN", "")
APP_ID         = os.environ.get("APP_ID", "1089")
SYMBOL         = os.environ.get("SYMBOL", "1HZ50V")
STAKE          = float(os.environ.get("STAKE", "0.35"))
DURATION       = int(os.environ.get("DURATION", "5"))
DURATION_UNIT  = os.environ.get("DURATION_UNIT", "t")
MAX_DAILY_LOSS = float(os.environ.get("MAX_DAILY_LOSS", "3.00"))
PORT           = int(os.environ.get("PORT", "8080"))

WS_URL = f"wss://ws.binaryws.com/websockets/v3?app_id={APP_ID}"

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
    "last_signal": "—",
    "last_price": 0.0,
    "logs": [],
    "is_trading": False,
    "started_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
}

price_history = deque(maxlen=50)
is_trading = False
pending_proposal_id = None


def log(msg, level="INFO"):
    icons = {"INFO": "ℹ️", "TRADE": "💰", "WIN": "✅", "LOSS": "❌", "WARN": "⚠️", "ERROR": "🔴"}
    ts = datetime.now().strftime("%H:%M:%S")
    line = f"[{ts}] {icons.get(level, '.')} {msg}"
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
    d = [p[i + 1] - p[i] for i in range(len(p) - 1)][-period:]
    g = sum(x for x in d if x > 0) / period
    l = sum(-x for x in d if x < 0) / period
    return 100 if l == 0 else 100 - (100 / (1 + g / l))


def get_signal():
    if len(price_history) < 22:
        return None
    p = list(price_history)
    f1 = ema(deque(p), 5)
    s1 = ema(deque(p), 20)
    f0 = ema(deque(p[:-1]), 5)
    s0 = ema(deque(p[:-1]), 20)
    r = rsi(deque(p), 7)
    if None in [f1, s1, f0, s0, r]:
        return None
    log(f"EMA5={f1:.2f} EMA20={s1:.2f} RSI={r:.1f}")
    if f0 < s0 and f1 > s1 and r < 60:
        return "CALL"
    if f0 > s0 and f1 < s1 and r > 40:
        return "PUT"
    return None


async def request_proposal(ws, direction):
    """Step 1: Request a price proposal from Deriv"""
    global pending_proposal_id
    await ws.send(json.dumps({
        "proposal": 1,
        "amount": STAKE,
        "basis": "stake",
        "contract_type": direction,
        "currency": "USD",
        "duration": DURATION,
        "duration_unit": DURATION_UNIT,
        "symbol": SYMBOL
    }))
    log(f"Proposal requested: {direction}", "TRADE")


async def buy_proposal(ws, proposal_id):
    """Step 2: Buy the proposal once we receive it"""
    await ws.send(json.dumps({
        "buy": proposal_id,
        "price": STAKE
    }))
    log(f"Buying proposal {proposal_id}", "TRADE")


async def bot_loop():
    global is_trading, pending_proposal_id
    while True:
        try:
            state["status"] = "Connecting..."
            log(f"Connecting to Deriv WS (App ID: {APP_ID})...")
            async with websockets.connect(WS_URL) as ws:
                # Authorize
                await ws.send(json.dumps({"authorize": API_TOKEN}))

                async for raw in ws:
                    data = json.loads(raw)
                    msg_type = data.get("msg_type")

                    # --- Auth ---
                    if msg_type == "authorize":
                        if "error" in data:
                            log(f"Auth failed: {data['error']['message']}", "ERROR")
                            state["status"] = "Auth Failed"
                            break
                        bal = float(data["authorize"]["balance"])
                        state["balance"] = bal
                        if state["start_balance"] == 0:
                            state["start_balance"] = bal
                        state["connected"] = True
                        state["status"] = "Live — Watching"
                        log(f"Authorized. Balance: ${bal:.2f}")
                        # Subscribe to balance and ticks
                        await ws.send(json.dumps({"balance": 1, "subscribe": 1}))
                        await ws.send(json.dumps({"ticks": SYMBOL, "subscribe": 1}))
                        log(f"Subscribed to {SYMBOL}")

                    # --- Live price ticks ---
                    elif msg_type == "tick":
                        price = float(data["tick"]["quote"])
                        price_history.append(price)
                        state["last_price"] = price

                        if state["start_balance"] == 0:
                            continue

                        daily_loss = state["start_balance"] - state["balance"]
                        if daily_loss >= MAX_DAILY_LOSS:
                            state["status"] = "Daily loss limit hit — Paused"
                            continue

                        if is_trading:
                            continue

                        signal = get_signal()
                        if signal:
                            state["last_signal"] = signal
                            is_trading = True
                            state["is_trading"] = True
                            state["status"] = f"Trade Open ({signal})"
                            await request_proposal(ws, signal)

                    # --- Balance update ---
                    elif msg_type == "balance":
                        state["balance"] = float(data["balance"]["balance"])
                        state["daily_pnl"] = state["balance"] - state["start_balance"]

                    # --- Proposal response → buy it ---
                    elif msg_type == "proposal":
                        if "error" in data:
                            log(f"Proposal error: {data['error']['message']}", "ERROR")
                            is_trading = False
                            state["is_trading"] = False
                            state["status"] = "Live — Watching"
                        else:
                            proposal_id = data["proposal"]["id"]
                            pending_proposal_id = proposal_id
                            await buy_proposal(ws, proposal_id)

                    # --- Buy confirmation ---
                    elif msg_type == "buy":
                        if "error" in data:
                            log(f"Buy error: {data['error']['message']}", "ERROR")
                            is_trading = False
                            state["is_trading"] = False
                            state["status"] = "Live — Watching"
                        else:
                            cid = data["buy"]["contract_id"]
                            state["trades_today"] += 1
                            log(f"Trade open — Contract ID: {cid}", "TRADE")
                            await ws.send(json.dumps({
                                "proposal_open_contract": 1,
                                "contract_id": cid,
                                "subscribe": 1
                            }))

                    # --- Contract result ---
                    elif msg_type == "proposal_open_contract":
                        c = data.get("proposal_open_contract", {})
                        if c.get("is_expired") or c.get("status") in ("sold", "won", "lost"):
                            profit = float(c.get("profit", 0))
                            state["daily_pnl"] = state["balance"] - state["start_balance"]
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
                            log(f"{result} ${abs(profit):.2f} | Balance: ${state['balance']:.2f}", result)
                            is_trading = False
                            state["is_trading"] = False
                            state["status"] = "Live — Watching"

                    elif "error" in data:
                        log(f"Error: {data['error']['message']}", "ERROR")

        except Exception as e:
            state["connected"] = False
            state["status"] = "Reconnecting..."
            log(f"Connection lost: {e} — retrying in 5s", "WARN")
            await asyncio.sleep(5)


def build_html():
    s = state
    total = s["wins"] + s["losses"]
    winrate = round((s["wins"] / total) * 100) if total > 0 else 0
    pnl = s["daily_pnl"]
    pnl_color = "#22c55e" if pnl >= 0 else "#ef4444"
    pnl_sign = "+" if pnl >= 0 else ""
    dot_color = "#22c55e" if s["connected"] else "#f59e0b"

    trades_rows = ""
    for t in s["trades"]:
        color = "#22c55e" if t["result"] == "WIN" else "#ef4444"
        ps = "+" if t["profit"] > 0 else ""
        trades_rows += f"""<tr>
          <td>{t['time']}</td>
          <td style="color:#60a5fa">{t['type']}</td>
          <td style="color:{color}">{t['result']}</td>
          <td style="color:{color}">{ps}${abs(t['profit']):.2f}</td>
          <td>${t['balance']:.2f}</td>
        </tr>"""
    if not trades_rows:
        trades_rows = '<tr><td colspan="5" class="empty">No trades yet — watching for signals...</td></tr>'

    logs_html = ""
    level_colors = {"WIN": "#22c55e", "LOSS": "#ef4444", "TRADE": "#60a5fa",
                    "WARN": "#f59e0b", "ERROR": "#ef4444", "INFO": "#6b7280"}
    for l in s["logs"][:15]:
        c = level_colors.get(l["level"], "#6b7280")
        logs_html += f'<div class="log-line" style="border-left-color:{c};color:{c}">[{l["time"]}] {l["msg"]}</div>'

    price_str = f"{s['last_price']:.4f}" if s["last_price"] else "—"

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<meta http-equiv="refresh" content="4">
<title>Scalper Bot</title>
<style>
  * {{ box-sizing: border-box; margin: 0; padding: 0; }}
  body {{ background: #0d0d0d; color: #e2e8f0; font-family: 'Segoe UI', sans-serif; padding: 16px; font-size: 14px; }}

  .topbar {{ display: flex; justify-content: space-between; align-items: center; margin-bottom: 18px; }}
  .brand {{ font-weight: 700; font-size: 15px; letter-spacing: 1px; color: #fff; }}
  .status {{ display: flex; align-items: center; gap: 6px; font-size: 12px; color: #9ca3af; }}
  .dot {{ width: 8px; height: 8px; border-radius: 50%; background: {dot_color}; }}

  .cards {{ display: grid; grid-template-columns: 1fr 1fr; gap: 10px; margin-bottom: 14px; }}
  .card {{ background: #161616; border: 1px solid #222; border-radius: 10px; padding: 14px; }}
  .card-label {{ font-size: 10px; color: #6b7280; text-transform: uppercase; letter-spacing: 1px; margin-bottom: 6px; }}
  .card-value {{ font-size: 22px; font-weight: 700; font-family: monospace; }}
  .card-sub {{ font-size: 11px; color: #6b7280; margin-top: 4px; }}

  .price-card {{ background: #161616; border: 1px solid #222; border-radius: 10px; padding: 14px; margin-bottom: 14px; }}
  .price-big {{ font-size: 26px; font-weight: 700; font-family: monospace; color: #fff; }}
  .price-sub {{ font-size: 11px; color: #6b7280; margin-top: 4px; }}

  .section-title {{ font-size: 10px; color: #4b5563; text-transform: uppercase; letter-spacing: 1.5px; margin: 14px 0 8px; }}

  table {{ width: 100%; border-collapse: collapse; background: #161616; border: 1px solid #222; border-radius: 10px; overflow: hidden; margin-bottom: 14px; }}
  th {{ padding: 8px 12px; font-size: 10px; color: #4b5563; text-transform: uppercase; letter-spacing: 1px; text-align: left; border-bottom: 1px solid #222; font-weight: 500; }}
  td {{ padding: 8px 12px; font-size: 12px; font-family: monospace; border-bottom: 1px solid #1a1a1a; color: #9ca3af; }}
  tr:last-child td {{ border-bottom: none; }}
  .empty {{ text-align: center; color: #374151; padding: 20px !important; }}

  .log-line {{ padding: 5px 10px; border-left: 2px solid #333; margin-bottom: 3px; font-size: 11px; font-family: monospace; border-radius: 0 4px 4px 0; background: #111; }}

  .footer {{ text-align: center; font-size: 10px; color: #374151; margin-top: 16px; padding-top: 12px; border-top: 1px solid #1a1a1a; font-family: monospace; }}
</style>
</head>
<body>

<div class="topbar">
  <div class="brand">⚡ SCALPER BOT</div>
  <div class="status"><div class="dot"></div>{s['status']}</div>
</div>

<div class="cards">
  <div class="card">
    <div class="card-label">Balance</div>
    <div class="card-value" style="color:#22c55e">${s['balance']:.2f}</div>
    <div class="card-sub">Start: ${s['start_balance']:.2f}</div>
  </div>
  <div class="card">
    <div class="card-label">Today P&L</div>
    <div class="card-value" style="color:{pnl_color}">{pnl_sign}${abs(pnl):.2f}</div>
    <div class="card-sub">{s['trades_today']} trades</div>
  </div>
  <div class="card">
    <div class="card-label">Wins</div>
    <div class="card-value" style="color:#22c55e">{s['wins']}</div>
    <div class="card-sub">Win rate: {winrate}%</div>
  </div>
  <div class="card">
    <div class="card-label">Losses</div>
    <div class="card-value" style="color:#ef4444">{s['losses']}</div>
    <div class="card-sub">Max loss: ${MAX_DAILY_LOSS}</div>
  </div>
</div>

<div class="price-card">
  <div class="card-label">Live Price — {s['symbol'] if 'symbol' in s else SYMBOL}</div>
  <div class="price-big">{price_str}</div>
  <div class="price-sub">Last signal: <b style="color:#60a5fa">{s['last_signal']}</b> &nbsp;·&nbsp; {'🔄 Trade open' if s['is_trading'] else '👁 Watching'}</div>
</div>

<div class="section-title">Recent Trades</div>
<table>
  <tr><th>Time</th><th>Type</th><th>Result</th><th>Profit</th><th>Balance</th></tr>
  {trades_rows}
</table>

<div class="section-title">Bot Logs</div>
{logs_html}

<div class="footer">Auto-refresh every 4s &nbsp;·&nbsp; Started {s['started_at']}</div>
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
    log(f"Dashboard running on port {PORT}")
    server.serve_forever()


if __name__ == "__main__":
    print("DERIV SCALPER BOT v5.2", flush=True)
    if not API_TOKEN:
        log("No API_TOKEN set!", "ERROR")
        exit(1)
    log(f"Token: {API_TOKEN[:8]}... | Symbol: {SYMBOL} | Stake: ${STAKE} | App ID: {APP_ID}")
    t = threading.Thread(target=run_server, daemon=True)
    t.start()
    asyncio.run(bot_loop())
