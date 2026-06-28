"""
DERIV SCALPER BOT v4.0
Uses NEW Deriv API: REST OTP → WebSocket
PAT token from home.deriv.com/account/api-token
"""

import asyncio
import json
import os
import requests
from datetime import datetime
from collections import deque
import websockets

# ── CONFIG ────────────────────────────────────────────
API_TOKEN      = os.environ.get("API_TOKEN", "")
APP_ID         = os.environ.get("APP_ID", "33G9IntANaJzG3qeRKAPk")
SYMBOL         = "1HZ50V"
STAKE          = 0.35
DURATION       = 5
DURATION_UNIT  = "t"
MAX_DAILY_LOSS = 3.00
API_BASE       = "https://api.derivws.com"

# ── STATE ─────────────────────────────────────────────
price_history       = deque(maxlen=50)
daily_start_balance = None
current_balance     = None
trades_today        = 0
is_trading          = False

# ── LOGGING ───────────────────────────────────────────
def log(msg, level="INFO"):
    icons = {"INFO":"ℹ️ ","TRADE":"💰","WIN":"✅","LOSS":"❌","WARN":"⚠️ ","ERROR":"🔴"}
    ts   = datetime.now().strftime("%H:%M:%S")
    line = f"[{ts}] {icons.get(level,'•')} {msg}"
    print(line, flush=True)
    try:
        with open("bot_log.txt", "a") as f:
            f.write(line + "\n")
    except:
        pass

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
    p  = list(price_history)
    f1 = ema(deque(p), 5)
    s1 = ema(deque(p), 20)
    f0 = ema(deque(p[:-1]), 5)
    s0 = ema(deque(p[:-1]), 20)
    r  = rsi(deque(p), 7)
    if None in [f1,s1,f0,s0,r]:
        return None
    log(f"EMA5={f1:.4f} EMA20={s1:.4f} RSI={r:.1f}")
    if f0 < s0 and f1 > s1 and r < 60:
        return "CALL"
    if f0 > s0 and f1 < s1 and r > 40:
        return "PUT"
    return None

# ── GET ACCOUNT ID ────────────────────────────────────
def get_account_id():
    log("Getting account list...")
    headers = {
        "Authorization": f"Bearer {API_TOKEN}",
        "Deriv-App-ID": APP_ID,
        "Content-Type": "application/json"
    }
    resp = requests.get(f"{API_BASE}/trading/v1/options/accounts", headers=headers, timeout=15)
    log(f"Account API status: {resp.status_code}")
    log(f"Account API response: {resp.text[:200]}")
    
    if resp.status_code != 200:
        raise Exception(f"Failed to get accounts: {resp.text}")
    
    data = resp.json()
    accounts = data.get("data", [])
    if not accounts:
        # Try creating a demo account
        log("No accounts found, creating demo account...")
        create_resp = requests.post(
            f"{API_BASE}/trading/v1/options/accounts",
            headers=headers,
            json={"currency": "USD", "group": "row", "account_type": "demo"},
            timeout=15
        )
        log(f"Create account response: {create_resp.text[:200]}")
        accounts = create_resp.json().get("data", [])
    
    if not accounts:
        raise Exception("Could not get or create account")
    
    acc = accounts[0]
account_id = acc.get("id") or acc.get("account_id") or acc.get("loginid")
log(f"Account data keys: {list(acc.keys())}")

    log(f"Account ID: {account_id}")
    return account_id

# ── GET WEBSOCKET URL ─────────────────────────────────
def get_ws_url(account_id):
    log("Getting WebSocket URL via OTP...")
    headers = {
        "Authorization": f"Bearer {API_TOKEN}",
        "Deriv-App-ID": APP_ID,
        "Content-Type": "application/json"
    }
    resp = requests.post(
        f"{API_BASE}/trading/v1/options/accounts/{account_id}/otp",
        headers=headers,
        timeout=15
    )
    log(f"OTP API status: {resp.status_code}")
    log(f"OTP API response: {resp.text[:200]}")
    
    if resp.status_code != 200:
        raise Exception(f"Failed to get OTP: {resp.text}")
    
    ws_url = resp.json()["data"]["url"]
    log(f"WebSocket URL obtained: {ws_url[:50]}...")
    return ws_url

# ── TRADE ─────────────────────────────────────────────
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

# ── MAIN LOOP ─────────────────────────────────────────
async def run(ws_url):
    global daily_start_balance, current_balance, trades_today, is_trading

    log(f"Connecting to WebSocket...")
    async with websockets.connect(ws_url) as ws:
        log("✅ Connected!")

        # Get balance
        await ws.send(json.dumps({"balance": 1, "subscribe": 1}))
        # Subscribe to ticks
        await ws.send(json.dumps({"ticks": SYMBOL, "subscribe": 1}))
        log(f"📡 Subscribed to {SYMBOL}. Collecting ticks...")

        async for raw in ws:
            data     = json.loads(raw)
            msg_type = data.get("msg_type")

            if msg_type == "tick":
                price = float(data["tick"]["quote"])
                price_history.append(price)
                if daily_start_balance is None:
                    continue
                if (daily_start_balance - (current_balance or daily_start_balance)) >= MAX_DAILY_LOSS:
                    log("🛑 Daily loss limit reached. Paused.", "WARN")
                    continue
                if is_trading:
                    continue
                signal = get_signal()
                if signal:
                    log(f"🎯 {signal} signal! Price: {price}", "TRADE")
                    await place_trade(ws, signal)

            elif msg_type == "balance":
                bal = float(data["balance"]["balance"])
                if daily_start_balance is None:
                    daily_start_balance = bal
                    log(f"💵 Balance: ${bal:.2f}")
                current_balance = bal

            elif msg_type == "buy":
                if "error" in data:
                    log(f"Trade error: {data['error']['message']}", "ERROR")
                    is_trading = False
                else:
                    cid = data["buy"]["contract_id"]
                    trades_today += 1
                    log(f"Trade #{trades_today} opened | ID: {cid}", "TRADE")
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
                    level = "WIN" if profit > 0 else "LOSS"
                    log(f"{'WIN' if profit>0 else 'LOSS'} ${abs(profit):.2f} | Balance: ${current_balance:.2f}", level)
                    is_trading = False

            elif "error" in data:
                log(f"Error: {data['error']['message']}", "ERROR")

# ── ENTRY ─────────────────────────────────────────────
if __name__ == "__main__":
    print("""
╔══════════════════════════════════════════╗
║       DERIV SCALPER BOT v4.0            ║
║   New REST+WebSocket API | PAT Token    ║
╚══════════════════════════════════════════╝
    """, flush=True)

    if not API_TOKEN:
        log("❌ No API_TOKEN set! Add it to Railway Variables.", "ERROR")
        exit(1)

    log(f"Token starts with: {API_TOKEN[:8]}...")
    log(f"App ID: {APP_ID}")

    try:
        account_id = get_account_id()
        ws_url     = get_ws_url(account_id)
        asyncio.run(run(ws_url))
    except KeyboardInterrupt:
        log("Bot stopped.", "WARN")
    except Exception as e:
        log(f"Fatal: {e}", "ERROR")
        raise
