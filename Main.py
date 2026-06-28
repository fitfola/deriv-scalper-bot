"""
╔══════════════════════════════════════════════════════╗
║         DERIV SCALPER BOT v2.0                       ║
║   Updated for NEW Deriv API (REST + WebSocket)       ║
║   Uses PAT token (pat_xxx) — correct format          ║
╚══════════════════════════════════════════════════════╝
"""

import asyncio
import json
import time
import requests
from datetime import datetime
from collections import deque
import websockets
from config import API_TOKEN, SYMBOL, STAKE, DURATION, DURATION_UNIT, MAX_DAILY_LOSS, APP_ID

# ════════════════════════════════════════════════════════
#  STEP 1: Get OTP + WebSocket URL from Deriv REST API
# ════════════════════════════════════════════════════════

def get_account_id():
    """Get your Deriv account ID using your PAT token."""
    url = "https://api.derivws.com/trading/v1/options/accounts"
    headers = {
        "Deriv-App-ID": str(APP_ID),
        "Authorization": f"Bearer {API_TOKEN}",
        "Content-Type": "application/json"
    }
    resp = requests.get(url, headers=headers)
    data = resp.json()
    if resp.status_code != 200:
        raise Exception(f"Failed to get account: {data}")
    accounts = data.get("data", [])
    if not accounts:
        raise Exception("No accounts found. Make sure your token has correct permissions.")
    # Pick first account
    account = accounts[0]
    log(f"Account found: {account['id']} | Type: {account.get('account_type','unknown')}")
    return account["id"]

def get_websocket_url(account_id):
    """Exchange PAT token for a WebSocket OTP URL."""
    url = f"https://api.derivws.com/trading/v1/options/accounts/{account_id}/otp"
    headers = {
        "Deriv-App-ID": str(APP_ID),
        "Authorization": f"Bearer {API_TOKEN}",
        "Content-Type": "application/json"
    }
    resp = requests.post(url, headers=headers)
    data = resp.json()
    if resp.status_code != 200:
        raise Exception(f"Failed to get WebSocket URL: {data}")
    ws_url = data["data"]["url"]
    log(f"WebSocket URL obtained successfully")
    return ws_url

# ════════════════════════════════════════════════════════
#  INDICATORS
# ════════════════════════════════════════════════════════

price_history = deque(maxlen=50)

def calculate_ema(prices, period):
    if len(prices) < period:
        return None
    prices_list = list(prices)
    k = 2 / (period + 1)
    ema = sum(prices_list[:period]) / period
    for price in prices_list[period:]:
        ema = price * k + ema * (1 - k)
    return ema

def calculate_rsi(prices, period=7):
    if len(prices) < period + 1:
        return None
    prices_list = list(prices)
    deltas = [prices_list[i+1] - prices_list[i] for i in range(len(prices_list)-1)]
    deltas = deltas[-period:]
    gains  = [d for d in deltas if d > 0]
    losses = [-d for d in deltas if d < 0]
    avg_gain = sum(gains) / period if gains else 0
    avg_loss = sum(losses) / period if losses else 0
    if avg_loss == 0:
        return 100
    rs  = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))

def get_signal():
    if len(price_history) < 22:
        return None
    prices = list(price_history)
    fast_now  = calculate_ema(deque(prices),     5)
    slow_now  = calculate_ema(deque(prices),     20)
    fast_prev = calculate_ema(deque(prices[:-1]), 5)
    slow_prev = calculate_ema(deque(prices[:-1]), 20)
    rsi = calculate_rsi(deque(prices), 7)
    if None in [fast_now, slow_now, fast_prev, slow_prev, rsi]:
        return None
    log(f"EMA5={fast_now:.4f} EMA20={slow_now:.4f} RSI={rsi:.1f}")
    if fast_prev < slow_prev and fast_now > slow_now and rsi < 60:
        return "CALL"
    if fast_prev > slow_prev and fast_now < slow_now and rsi > 40:
        return "PUT"
    return None

# ════════════════════════════════════════════════════════
#  LOGGING
# ════════════════════════════════════════════════════════

def log(msg, level="INFO"):
    icons = {"INFO":"ℹ️","TRADE":"💰","WIN":"✅","LOSS":"❌","WARN":"⚠️","ERROR":"🔴"}
    icon  = icons.get(level, "•")
    ts    = datetime.now().strftime("%H:%M:%S")
    line  = f"[{ts}] {icon}  {msg}"
    print(line)
    with open("bot_log.txt", "a") as f:
        f.write(line + "\n")

# ════════════════════════════════════════════════════════
#  TRADE STATE
# ════════════════════════════════════════════════════════

daily_start_balance = None
current_balance     = None
trades_today        = 0
is_trading          = False
active_contract_id  = None

# ════════════════════════════════════════════════════════
#  MAIN BOT
# ════════════════════════════════════════════════════════

async def run_bot(ws_url):
    global daily_start_balance, current_balance, trades_today, is_trading, active_contract_id

    log(f"Connecting to Deriv WebSocket...")

    async with websockets.connect(ws_url) as ws:
        log("✅ Connected! Waiting for market data...")

        # Subscribe to ticks
        await ws.send(json.dumps({"ticks": SYMBOL, "subscribe": 1}))

        # Get initial balance
        await ws.send(json.dumps({"balance": 1, "subscribe": 1}))

        async for raw in ws:
            data = json.loads(raw)
            msg_type = data.get("msg_type")

            if msg_type == "tick":
                tick  = data["tick"]
                price = float(tick["quote"])
                price_history.append(price)

                if daily_start_balance is None:
                    continue

                daily_loss = daily_start_balance - (current_balance or daily_start_balance)
                if daily_loss >= MAX_DAILY_LOSS:
                    log(f"🛑 Daily loss limit hit (${daily_loss:.2f}). Paused.", "WARN")
                    continue

                if is_trading:
                    continue

                signal = get_signal()
                if signal:
                    log(f"🎯 Signal: {signal} at {price}", "TRADE")
                    await place_trade(ws, signal)

            elif msg_type == "balance":
                bal = float(data["balance"]["balance"])
                if daily_start_balance is None:
                    daily_start_balance = bal
                    log(f"💵 Starting balance: ${bal:.2f}")
                current_balance = bal

            elif msg_type == "buy":
                if "error" in data:
                    log(f"Trade failed: {data['error']['message']}", "ERROR")
                    is_trading = False
                else:
                    contract = data["buy"]
                    active_contract_id = contract["contract_id"]
                    trades_today += 1
                    log(f"Trade #{trades_today} placed | ID: {active_contract_id}", "TRADE")
                    await ws.send(json.dumps({
                        "proposal_open_contract": 1,
                        "contract_id": active_contract_id,
                        "subscribe": 1
                    }))

            elif msg_type == "proposal_open_contract":
                contract = data.get("proposal_open_contract", {})
                if contract.get("is_expired") or contract.get("status") == "sold":
                    profit = float(contract.get("profit", 0))
                    if profit > 0:
                        log(f"WIN! +${profit:.2f}", "WIN")
                    else:
                        log(f"LOSS. -${abs(profit):.2f}", "LOSS")
                    await ws.send(json.dumps({"balance": 1}))
                    is_trading = False
                    active_contract_id = None

            elif "error" in data:
                log(f"API Error: {data['error']['message']}", "ERROR")


async def place_trade(ws, direction):
    global is_trading
    is_trading = True
    await ws.send(json.dumps({
        "buy": 1,
        "price": STAKE,
        "parameters": {
            "amount":        STAKE,
            "basis":         "stake",
            "contract_type": direction,
            "currency":      "USD",
            "duration":      DURATION,
            "duration_unit": DURATION_UNIT,
            "symbol":        SYMBOL
        }
    }))

# ════════════════════════════════════════════════════════
#  ENTRY POINT
# ════════════════════════════════════════════════════════

if __name__ == "__main__":
    print("""
╔══════════════════════════════════════════════════╗
║          DERIV SCALPER BOT v2.0                  ║
║      EMA + RSI | New API | PAT Token Support     ║
╚══════════════════════════════════════════════════╝
    """)
    try:
        log("Step 1: Getting your Deriv account...")
        account_id = get_account_id()
        log("Step 2: Getting WebSocket access URL...")
        ws_url = get_websocket_url(account_id)
        log("Step 3: Starting trading bot...")
        asyncio.run(run_bot(ws_url))
    except KeyboardInterrupt:
        log("Bot stopped by user.", "WARN")
    except Exception as e:
        log(f"Error: {e}", "ERROR")
