"""
╔══════════════════════════════════════════════════════╗
║         DERIV SCALPER BOT v3.0 - FINAL              ║
║   Uses: wss://ws.derivws.com/websockets/v3           ║
║   Auth: {"authorize": "your_pat_token"}              ║
║   Works with pat_xxx tokens from home.deriv.com      ║
╚══════════════════════════════════════════════════════╝
"""

import asyncio
import json
from datetime import datetime
from collections import deque
import websockets

# ── CONFIG ───────────────────────────────────────────────────────────
API_TOKEN      = "pat_ae8ad69e725e649f9fdd43b1a5c8f8929a0995ab3fac0221fa13452446600545"   # from home.deriv.com
SYMBOL         = "1HZ50V"   # Volatility 50 (1s) Index
STAKE          = 0.35       # $ per trade
DURATION       = 5          # ticks
DURATION_UNIT  = "t"
MAX_DAILY_LOSS = 3.00       # stop if down $3 today
APP_ID         = 1089       # public test app ID — works for everyone

WS_URL = f"wss://ws.derivws.com/websockets/v3?app_id={APP_ID}"

# ── STATE ─────────────────────────────────────────────────────────────
price_history       = deque(maxlen=50)
daily_start_balance = None
current_balance     = None
trades_today        = 0
is_trading          = False
active_contract_id  = None

# ── LOGGING ───────────────────────────────────────────────────────────
def log(msg, level="INFO"):
    icons = {
        "INFO":  "ℹ️ ",
        "TRADE": "💰",
        "WIN":   "✅",
        "LOSS":  "❌",
        "WARN":  "⚠️ ",
        "ERROR": "🔴"
    }
    icon = icons.get(level, "•")
    ts   = datetime.now().strftime("%H:%M:%S")
    line = f"[{ts}] {icon} {msg}"
    print(line)
    with open("bot_log.txt", "a") as f:
        f.write(line + "\n")

# ── INDICATORS ────────────────────────────────────────────────────────
def ema(prices, period):
    if len(prices) < period:
        return None
    p = list(prices)
    k = 2 / (period + 1)
    val = sum(p[:period]) / period
    for price in p[period:]:
        val = price * k + val * (1 - k)
    return val

def rsi(prices, period=7):
    if len(prices) < period + 1:
        return None
    p     = list(prices)
    deltas = [p[i+1] - p[i] for i in range(len(p)-1)]
    d      = deltas[-period:]
    gains  = [x for x in d if x > 0]
    losses = [-x for x in d if x < 0]
    ag = sum(gains)  / period if gains  else 0
    al = sum(losses) / period if losses else 0
    if al == 0:
        return 100
    return 100 - (100 / (1 + ag / al))

def get_signal():
    if len(price_history) < 22:
        return None
    p = list(price_history)
    f1 = ema(deque(p),     5)
    s1 = ema(deque(p),    20)
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

# ── TRADE ─────────────────────────────────────────────────────────────
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

# ── MAIN BOT LOOP ─────────────────────────────────────────────────────
async def run():
    global daily_start_balance, current_balance
    global trades_today, is_trading, active_contract_id

    log(f"Connecting to Deriv... ({WS_URL})")

    async with websockets.connect(WS_URL) as ws:

        # Step 1: Authorize
        await ws.send(json.dumps({"authorize": API_TOKEN}))
        resp = json.loads(await ws.recv())

        if "error" in resp:
            log(f"Auth failed: {resp['error']['message']}", "ERROR")
            log("Check your API token in config at top of main.py", "ERROR")
            return

        balance = resp["authorize"]["balance"]
        loginid = resp["authorize"]["loginid"]
        daily_start_balance = float(balance)
        current_balance     = float(balance)
        log(f"✅ Authorized! Account: {loginid} | Balance: ${balance}", "INFO")

        # Step 2: Subscribe to ticks
        await ws.send(json.dumps({"ticks": SYMBOL, "subscribe": 1}))
        log(f"📡 Subscribed to {SYMBOL} ticks. Waiting for signals...", "INFO")
        log(f"Need 22 ticks before first signal (~22 seconds on 1s index)", "INFO")

        # Step 3: Listen
        async for raw in ws:
            data     = json.loads(raw)
            msg_type = data.get("msg_type")

            # ── Tick received ────────────────────────────────────────
            if msg_type == "tick":
                price = float(data["tick"]["quote"])
                price_history.append(price)

                # Daily loss guard
                daily_loss = daily_start_balance - current_balance
                if daily_loss >= MAX_DAILY_LOSS:
                    log(f"🛑 Daily loss limit hit (${daily_loss:.2f}). Bot paused.", "WARN")
                    continue

                if is_trading:
                    continue

                signal = get_signal()
                if signal:
                    log(f"🎯 Signal: {signal} | Price: {price}", "TRADE")
                    await place_trade(ws, signal)

            # ── Trade placed ─────────────────────────────────────────
            elif msg_type == "buy":
                if "error" in data:
                    log(f"Trade failed: {data['error']['message']}", "ERROR")
                    is_trading = False
                else:
                    active_contract_id = data["buy"]["contract_id"]
                    trades_today += 1
                    log(f"Trade #{trades_today} open | ID: {active_contract_id} | ${STAKE}", "TRADE")
                    await ws.send(json.dumps({
                        "proposal_open_contract": 1,
                        "contract_id": active_contract_id,
                        "subscribe": 1
                    }))

            # ── Contract result ──────────────────────────────────────
            elif msg_type == "proposal_open_contract":
                c = data.get("proposal_open_contract", {})
                if c.get("is_expired") or c.get("status") == "sold":
                    profit = float(c.get("profit", 0))
                    current_balance += profit
                    if profit > 0:
                        log(f"WIN! +${profit:.2f} | Balance: ${current_balance:.2f}", "WIN")
                    else:
                        log(f"LOSS -${abs(profit):.2f} | Balance: ${current_balance:.2f}", "LOSS")
                    is_trading = False
                    active_contract_id = None

            # ── Errors ──────────────────────────────────────────────
            elif "error" in data:
                log(f"API error: {data['error']['message']}", "ERROR")

# ── ENTRY ─────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print("""
╔══════════════════════════════════════════╗
║       DERIV SCALPER BOT v3.0            ║
║   EMA + RSI | Safe for $10 Accounts     ║
╚══════════════════════════════════════════╝
    """)
    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        log("Bot stopped.", "WARN")
    except Exception as e:
        log(f"Fatal error: {e}", "ERROR")
