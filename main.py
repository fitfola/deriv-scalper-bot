"""
DERIV SCALPER BOT v4.0
Uses NEW Deriv API: REST OTP then WebSocket
PAT token from home.deriv.com
"""

import asyncio
import json
import os
import requests
from datetime import datetime
from collections import deque
import websockets

API_TOKEN      = os.environ.get("API_TOKEN", "")
APP_ID         = os.environ.get("APP_ID", "33G9IntANaJzG3qeRKAPk")
ACCOUNT_ID     = os.environ.get("ACCOUNT_ID", "DOT93156522")
SYMBOL         = "1HZ50V"
STAKE          = 0.35
DURATION       = 5
DURATION_UNIT  = "t"
MAX_DAILY_LOSS = 3.00
API_BASE       = "https://api.derivws.com"

price_history       = deque(maxlen=50)
daily_start_balance = None
current_balance     = None
trades_today        = 0
is_trading          = False

def log(msg, level="INFO"):
    icons = {"INFO":"ℹ️ ","TRADE":"💰","WIN":"✅","LOSS":"❌","WARN":"⚠️ ","ERROR":"🔴"}
    ts = datetime.now().strftime("%H:%M:%S")
    line = f"[{ts}] {icons.get(level,'.')} {msg}"
    print(line, flush=True)

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
    log(f"EMA5={f1:.4f} EMA20={s1:.4f} RSI={r:.1f}")
    if f0 < s0 and f1 > s1 and r < 60:
        return "CALL"
    if f0 > s0 and f1 < s1 and r > 40:
        return "PUT"
    return None

def get_ws_url():
    log(f"Getting WebSocket URL for account {ACCOUNT_ID}...")
    headers = {
        "Authorization": f"Bearer {API_TOKEN}",
        "Deriv-App-ID": APP_ID,
        "Content-Type": "application/json"
    }
    resp = requests.post(
        f"{API_BASE}/trading/v1/options/accounts/{ACCOUNT_ID}/otp",
        headers=headers,
        timeout=15
    )
    log(f"OTP status: {resp.status_code}")
    log(f"OTP response: {resp.text[:300]}")
    if resp.status_code != 200:
        raise Exception(f"OTP failed: {resp.text}")
    ws_url = resp.json()["data"]["url"]
    log(f"Got WebSocket URL!")
    return ws_url

async def place_trade(ws, direction):
    global is_trading
    is_trading = True
    await ws.send(json.dumps({
        "buy": 1,
        "price": STAKE,
        "parameters": {
            "amount": STAKE,
            "basis": "stake",
            "contract_type": direction,
            "currency": "USD",
            "duration": DURATION,
            "duration_unit": DURATION_UNIT,
            "symbol": SYMBOL
        }
    }))

async def run(ws_url):
    global daily_start_balance, current_balance, trades_today, is_trading
    log("Connecting to WebSocket...")
    async with websockets.connect(ws_url) as ws:
        log("Connected!")
        await ws.send(json.dumps({"balance": 1, "subscribe": 1}))
        await ws.send(json.dumps({"ticks": SYMBOL, "subscribe": 1}))
        log(f"Subscribed to {SYMBOL}. Waiting for signals...")
        async for raw in ws:
            data = json.loads(raw)
            msg_type = data.get("msg_type")
            if msg_type == "tick":
                price = float(data["tick"]["quote"])
                price_history.append(price)
                if daily_start_balance is None:
                    continue
                loss = daily_start_balance - (current_balance or daily_start_balance)
                if loss >= MAX_DAILY_LOSS:
                    log("Daily loss limit hit. Paused.", "WARN")
                    continue
                if is_trading:
                    continue
                signal = get_signal()
                if signal:
                    log(f"Signal: {signal} at {price}", "TRADE")
                    await place_trade(ws, signal)
            elif msg_type == "balance":
                bal = float(data["balance"]["balance"])
                if daily_start_balance is None:
                    daily_start_balance = bal
                    log(f"Balance: ${bal:.2f}")
                current_balance = bal
            elif msg_type == "buy":
                if "error" in data:
                    log(f"Trade error: {data['error']['message']}", "ERROR")
                    is_trading = False
                else:
                    cid = data["buy"]["contract_id"]
                    trades_today += 1
                    log(f"Trade #{trades_today} opened ID:{cid}", "TRADE")
                    await ws.send(json.dumps({
                        "proposal_open_contract": 1,
                        "contract_id": cid,
                        "subscribe": 1
                    }))
            elif msg_type == "proposal_open_contract":
                c = data.get("proposal_open_contract", {})
                if c.get("is_expired") or c.get("status") == "sold":
                    profit = float(c.get("profit", 0))
                    current_balance = (current_balance or 0) + profit
                    lvl = "WIN" if profit > 0 else "LOSS"
                    log(f"{'WIN' if profit>0 else 'LOSS'} ${abs(profit):.2f} | Balance: ${current_balance:.2f}", lvl)
                    is_trading = False
            elif "error" in data:
                log(f"API error: {data['error']['message']}", "ERROR")

if __name__ == "__main__":
    print("DERIV SCALPER BOT v4.0", flush=True)
    if not API_TOKEN:
        log("No API_TOKEN set!", "ERROR")
        exit(1)
    log(f"Token: {API_TOKEN[:8]}...")
    log(f"App ID: {APP_ID}")
    log(f"Account: {ACCOUNT_ID}")
    try:
        ws_url = get_ws_url()
        asyncio.run(run(ws_url))
    except KeyboardInterrupt:
        log("Stopped.", "WARN")
    except Exception as e:
        log(f"Fatal: {e}", "ERROR")
        raise
