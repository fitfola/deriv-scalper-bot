"""
DERIV SCALPER BOT v6.0
Strategy: RSI bounce on mean-reverting synthetic index
- Trades RSI extremes (oversold/overbought bounces)
- 3 consecutive loss stop
- 15s cooldown between trades
- Designed for small accounts ($10+)
"""

import asyncio
import json
import os
import threading
import requests
import time
from datetime import datetime
from collections import deque
from http.server import HTTPServer, BaseHTTPRequestHandler
import websockets

API_TOKEN       = os.environ.get("API_TOKEN", "")
APP_ID          = os.environ.get("APP_ID", "33G9IntANaJzG3qeRKAPk")
ACCOUNT_ID      = os.environ.get("ACCOUNT_ID", "DOT93156522")
SYMBOL          = os.environ.get("SYMBOL", "1HZ50V")
STAKE           = float(os.environ.get("STAKE", "0.35"))
DURATION        = int(os.environ.get("DURATION", "5"))
DURATION_UNIT   = os.environ.get("DURATION_UNIT", "t")
MAX_DAILY_LOSS  = float(os.environ.get("MAX_DAILY_LOSS", "3.00"))
MAX_CONSEC_LOSS = int(os.environ.get("MAX_CONSEC_LOSS", "3"))
TRADE_COOLDOWN  = int(os.environ.get("TRADE_COOLDOWN", "15"))
PORT            = int(os.environ.get("PORT", "8080"))
API_BASE        = "https://api.derivws.com"

state = {
    "status": "Starting...",
    "connected": False,
    "balance": 0.0,
    "start_balance": 0.0,
    "trades": [],
    "trades_today": 0,
    "wins": 0,
    "losses": 0,
    "consec_losses": 0,
    "daily_pnl": 0.0,
    "last_signal": "—",
    "last_price": 0.0,
    "logs": [],
    "is_trading": False,
    "paused": False,
    "started_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
}

price_history = deque(maxlen=60)
is_trading = False
last_trade_time = 0


def log(msg, level="INFO"):
    icons = {"INFO": "ℹ️", "TRADE": "💰", "WIN": "✅", "LOSS": "❌", "WARN": "⚠️", "ERROR": "🔴"}
    ts = datetime.now().strftime("%H:%M:%S")
    print(f"[{ts}] {icons.get(level,'.')} {msg}", flush=True)
    state["logs"].insert(0, {"time": ts, "msg": msg, "level": level})
    if len(state["logs"]) > 50:
        state["logs"].pop()


def rsi(prices, period=7):
    if len(prices) < period + 1:
        return None
    p = list(prices)[-period-1:]
    d = [p[i+1]-p[i] for i in range(len(p)-1)]
    g = sum(x for x in d if x > 0) / period
    l = sum(-x for x in d if x < 0) / period
    if l == 0:
        return 100
    return 100 - (100 / (1 + g/l))


def ema(prices, period):
    if len(prices) < period:
        return None
    p = list(prices)
    k = 2 / (period + 1)
    v = sum(p[:period]) / period
    for x in p[period:]:
        v = x * k + v * (1 - k)
    return v


def get_signal():
    """
    RSI Bounce Strategy for mean-reverting synthetic index (1HZ50V):
    - CALL when RSI is deeply oversold (< 25) and starting to recover
    - PUT when RSI is deeply overbought (> 75) and starting to fade
    - Confirmed by EMA5 vs EMA20 trend direction
    """
    if len(price_history) < 20:
        return None

    p = list(price_history)
    r_now = rsi(p, 7)
    r_prev = rsi(p[:-1], 7)
    e5 = ema(p, 5)
    e20 = ema(p, 20)

    if None in [r_now, r_prev, e5, e20]:
        return None

    log(f"RSI={r_now:.1f} (was {r_prev:.1f}) EMA5={e5:.1f} EMA20={e20:.1f}")

    # CALL: RSI was oversold and is now recovering upward
    if r_prev < 25 and r_now > r_prev and e5 >= e20:
        return "CALL"

    # PUT: RSI was overbought and is now fading downward
    if r_prev > 75 and r_now < r_prev and e5 <= e20:
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


async def bot_loop():
    global is_trading, last_trade_time
    while True:
        try:
            state["status"] = "Connecting..."
            ws_url = get_ws_url()
            log("Connecting to Deriv...")
            async with websockets.connect(ws_url) as ws:
                state["connected"] = True
                state["status"] = "Live — Watching"
                log("Connected!")
                await ws.send(json.dumps({"balance": 1, "subscribe": 1}))
                await ws.send(json.dumps({"ticks": SYMBOL, "subscribe": 1}))
                log(f"Watching {SYMBOL} | Stake: ${STAKE} | Max loss streak: {MAX_CONSEC_LOSS}")

                async for raw in ws:
                    data = json.loads(raw)
                    msg_type = data.get("msg_type")

                    if msg_type == "tick":
                        price = float(data["tick"]["quote"])
                        price_history.append(price)
                        state["last_price"] = price

                        if state["start_balance"] == 0:
                            continue

                        # Guard: daily loss limit
                        if state["balance"] - state["start_balance"] <= -MAX_DAILY_LOSS:
                            state["status"] = "🛑 Daily loss limit — Paused"
                            state["paused"] = True
                            continue

                        # Guard: consecutive loss streak
                        if state["consec_losses"] >= MAX_CONSEC_LOSS:
                            state["status"] = f"🛑 {MAX_CONSEC_LOSS} losses in a row — Paused"
                            state["paused"] = True
                            continue

                        if is_trading:
                            continue

                        # Cooldown between trades
                        elapsed = time.time() - last_trade_time
                        if elapsed < TRADE_COOLDOWN:
                            remaining = int(TRADE_COOLDOWN - elapsed)
                            state["status"] = f"⏳ Cooldown {remaining}s"
                            continue

                        state["status"] = "Live — Watching"
                        signal = get_signal()
                        if signal:
                            state["last_signal"] = signal
                            is_trading = True
                            state["is_trading"] = True
                            state["status"] = f"🔄 Trade Open ({signal})"
                            log(f"Signal: {signal} @ {price:.2f}", "TRADE")
                            await ws.send(json.dumps({
                                "proposal": 1,
                                "amount": STAKE,
                                "basis": "stake",
                                "contract_type": signal,
                                "currency": "USD",
                                "duration": DURATION,
                                "duration_unit": DURATION_UNIT,
                                "underlying_symbol": SYMBOL
                            }))

                    elif msg_type == "balance":
                        bal = float(data["balance"]["balance"])
                        if state["start_balance"] == 0:
                            state["start_balance"] = bal
                            log(f"Starting balance: ${bal:.2f}")
                        state["balance"] = bal
                        state["daily_pnl"] = bal - state["start_balance"]

                    elif msg_type == "proposal":
                        if "error" in data:
                            log(f"Proposal error: {data['error']['message']}", "ERROR")
                            is_trading = False
                            state["is_trading"] = False
                        else:
                            pid = data["proposal"]["id"]
                            log(f"Got proposal, buying...", "TRADE")
                            await ws.send(json.dumps({"buy": pid, "price": STAKE}))

                    elif msg_type == "buy":
                        if "error" in data:
                            log(f"Buy error: {data['error']['message']}", "ERROR")
                            is_trading = False
                            state["is_trading"] = False
                        else:
                            cid = data["buy"]["contract_id"]
                            state["trades_today"] += 1
                            log(f"Contract open: {cid}", "TRADE")
                            await ws.send(json.dumps({
                                "proposal_open_contract": 1,
                                "contract_id": cid,
                                "subscribe": 1
                            }))

                    elif msg_type == "proposal_open_contract":
                        c = data.get("proposal_open_contract", {})
                        if c.get("is_expired") or c.get("status") in ("sold", "won", "lost"):
                            profit = float(c.get("profit", 0))
                            state["daily_pnl"] = state["balance"] - state["start_balance"]
                            result = "WIN" if profit > 0 else "LOSS"

                            if profit > 0:
                                state["wins"] += 1
                                state["consec_losses"] = 0  # reset streak on win
                            else:
                                state["losses"] += 1
                                state["consec_losses"] += 1

                            state["trades"].insert(0, {
                                "time": datetime.now().strftime("%H:%M:%S"),
                                "type": state["last_signal"],
                                "result": result,
                                "profit": profit,
                                "balance": state["balance"]
                            })
                            if len(state["trades"]) > 20:
                                state["trades"].pop()

                            log(f"{result} ${abs(profit):.2f} | Bal: ${state['balance']:.2f} | Streak: {state['consec_losses']} losses", result)
                            is_trading = False
                            state["is_trading"] = False
                            last_trade_time = time.time()

                    elif "error" in data:
                        log(f"WS error: {data['error']['message']}", "ERROR")

        except Exception as e:
            state["connected"] = False
            state["status"] = "Reconnecting..."
            is_trading = False
            state["is_trading"] = False
            log(f"Lost connection: {e} — retry in 5s", "WARN")
            await asyncio.sleep(5)


def build_html():
    s = state
    total = s["wins"] + s["losses"]
    winrate = round((s["wins"] / total) * 100) if total > 0 else 0
    pnl = s["daily_pnl"]
    pnl_color = "#22c55e" if pnl >= 0 else "#ef4444"
    pnl_sign = "+" if pnl >= 0 else ""
    dot_color = "#22c55e" if s["connected"] else "#f59e0b"
    streak = s["consec_losses"]
    streak_color = "#ef4444" if streak >= 2 else "#f59e0b" if streak == 1 else "#22c55e"

    trades_rows = ""
    for t in s["trades"]:
        color = "#22c55e" if t["result"] == "WIN" else "#ef4444"
        ps = "+" if t["profit"] > 0 else ""
        trades_rows += f"""<tr>
          <td>{t['time']}</td>
          <td style="color:#60a5fa">{t['type']}</td>
          <td style="color:{color};font-weight:700">{t['result']}</td>
          <td style="color:{color}">{ps}${abs(t['profit']):.2f}</td>
          <td>${t['balance']:.2f}</td>
        </tr>"""
    if not trades_rows:
        trades_rows = '<tr><td colspan="5" class="empty">Waiting for RSI signal...</td></tr>'

    logs_html = ""
    lc = {"WIN":"#22c55e","LOSS":"#ef4444","TRADE":"#60a5fa","WARN":"#f59e0b","ERROR":"#ef4444","INFO":"#4b5563"}
    for l in s["logs"][:12]:
        c = lc.get(l["level"], "#4b5563")
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
  *{{box-sizing:border-box;margin:0;padding:0}}
  body{{background:#0a0a0a;color:#e2e8f0;font-family:'Segoe UI',sans-serif;padding:16px;font-size:14px}}
  .topbar{{display:flex;justify-content:space-between;align-items:center;margin-bottom:16px}}
  .brand{{font-weight:700;font-size:15px;color:#fff;letter-spacing:1px}}
  .status{{display:flex;align-items:center;gap:6px;font-size:11px;color:#9ca3af}}
  .dot{{width:8px;height:8px;border-radius:50%;background:{dot_color}}}
  .cards{{display:grid;grid-template-columns:1fr 1fr;gap:10px;margin-bottom:12px}}
  .card{{background:#141414;border:1px solid #1f1f1f;border-radius:12px;padding:14px}}
  .lbl{{font-size:10px;color:#6b7280;text-transform:uppercase;letter-spacing:1px;margin-bottom:5px}}
  .val{{font-size:21px;font-weight:700;font-family:monospace}}
  .sub{{font-size:11px;color:#6b7280;margin-top:4px}}
  .price-card{{background:#141414;border:1px solid #1f1f1f;border-radius:12px;padding:14px;margin-bottom:12px}}
  .price-big{{font-size:24px;font-weight:700;font-family:monospace;color:#fff}}
  .strat-box{{background:#0f1a0f;border:1px solid #1a2e1a;border-radius:12px;padding:12px;margin-bottom:12px;font-size:11px;color:#4ade80;line-height:1.6}}
  .sec{{font-size:10px;color:#374151;text-transform:uppercase;letter-spacing:1.5px;margin:12px 0 6px}}
  table{{width:100%;border-collapse:collapse;background:#141414;border:1px solid #1f1f1f;border-radius:12px;overflow:hidden;margin-bottom:12px}}
  th{{padding:7px 10px;font-size:10px;color:#4b5563;text-transform:uppercase;letter-spacing:1px;text-align:left;border-bottom:1px solid #1f1f1f;font-weight:500}}
  td{{padding:7px 10px;font-size:12px;font-family:monospace;border-bottom:1px solid #111;color:#9ca3af}}
  tr:last-child td{{border-bottom:none}}
  .empty{{text-align:center;color:#374151;padding:18px!important}}
  .log-line{{padding:4px 8px;border-left:2px solid #333;margin-bottom:2px;font-size:11px;font-family:monospace;border-radius:0 4px 4px 0;background:#0d0d0d}}
  .footer{{text-align:center;font-size:10px;color:#374151;margin-top:14px;padding-top:10px;border-top:1px solid #1a1a1a}}
</style>
</head>
<body>
<div class="topbar">
  <div class="brand">⚡ SCALPER BOT</div>
  <div class="status"><div class="dot"></div>{s['status']}</div>
</div>

<div class="cards">
  <div class="card">
    <div class="lbl">Balance</div>
    <div class="val" style="color:#22c55e">${s['balance']:.2f}</div>
    <div class="sub">Start: ${s['start_balance']:.2f}</div>
  </div>
  <div class="card">
    <div class="lbl">Today P&L</div>
    <div class="val" style="color:{pnl_color}">{pnl_sign}${abs(pnl):.2f}</div>
    <div class="sub">{s['trades_today']} trades today</div>
  </div>
  <div class="card">
    <div class="lbl">Win Rate</div>
    <div class="val" style="color:#22c55e">{winrate}%</div>
    <div class="sub">{s['wins']}W / {s['losses']}L</div>
  </div>
  <div class="card">
    <div class="lbl">Loss Streak</div>
    <div class="val" style="color:{streak_color}">{streak}</div>
    <div class="sub">Max allowed: {MAX_CONSEC_LOSS}</div>
  </div>
</div>

<div class="price-card">
  <div class="lbl">Live Price — {SYMBOL}</div>
  <div class="price-big">{price_str}</div>
  <div class="sub" style="margin-top:6px">Signal: <b style="color:#60a5fa">{s['last_signal']}</b> &nbsp;·&nbsp; {'🔄 Trade open' if s['is_trading'] else '👁 Watching'}</div>
</div>

<div class="strat-box">
  📊 <b>Strategy:</b> RSI Bounce on {SYMBOL}<br>
  🟢 CALL when RSI &lt; 25 (oversold bounce) + EMA trend up<br>
  🔴 PUT when RSI &gt; 75 (overbought fade) + EMA trend down<br>
  ⏱ {TRADE_COOLDOWN}s cooldown · 🛑 Stops after {MAX_CONSEC_LOSS} consecutive losses
</div>

<div class="sec">Recent Trades</div>
<table>
  <tr><th>Time</th><th>Type</th><th>Result</th><th>Profit</th><th>Balance</th></tr>
  {trades_rows}
</table>

<div class="sec">Bot Logs</div>
{logs_html}

<div class="footer">Refresh every 4s · Started {s['started_at']} · Stake ${STAKE} · Max loss ${MAX_DAILY_LOSS}</div>
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
    print("DERIV SCALPER BOT v6.0", flush=True)
    if not API_TOKEN:
        log("No API_TOKEN set!", "ERROR")
        exit(1)
    log(f"Account: {ACCOUNT_ID} | Symbol: {SYMBOL} | Stake: ${STAKE} | Cooldown: {TRADE_COOLDOWN}s")
    t = threading.Thread(target=run_server, daemon=True)
    t.start()
    asyncio.run(bot_loop())
