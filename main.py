import os
import sys
import json
import time
import csv
import random
import asyncio
import aiohttp
import logging
from io import StringIO
from typing import List, Dict

import uvicorn
from dotenv import load_dotenv
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Header, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, HTMLResponse, FileResponse, StreamingResponse
from pydantic import BaseModel
import websockets

try:
    from SmartApi import SmartConnect
    import pyotp
    ANGELONE_SDK_AVAILABLE = True
except ImportError as e:
    import traceback
    print("--- DEBUG IMPORT ERROR ---")
    traceback.print_exc()
    print("--------------------------")
    ANGELONE_SDK_AVAILABLE = False



# Load environment variables
load_dotenv()

# IST Timezone Helper Functions
from datetime import datetime, timezone, timedelta

def get_ist_time() -> datetime:
    # IST is UTC + 5:30
    return datetime.now(timezone(timedelta(hours=5, minutes=30)))

def get_ist_time_str(fmt: str = "%Y-%m-%d %H:%M:%S") -> str:
    return get_ist_time().strftime(fmt)

# Setup Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("arbitrage-bot")

# Initialize FastAPI App
app = FastAPI(title="Spread Arbitrage Workstation Core")

# CORS Setup
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ----------------- Trading System State -----------------
class TradingSystem:
    def __init__(self):
        # State & Direction
        self.is_in_position = False
        self.position_direction = None  # "Expansion" or "Contraction"
        self.system_status = "Active"   # "Active", "In-Position", "Halted"
        
        # MCX Live Prices (LTP)
        self.gold_petal_ltp = 72000.0
        self.gold_mini_ltp = 71150.0
        self.spread = 850.0
        self.depth_buy_spread = 850.0
        self.depth_sell_spread = 850.0
        
        # Leg Entry Values
        self.petal_entry_price = 0.0
        self.mini_entry_price = 0.0
        self.entry_spread = 0.0
        
        # Real-time Leg P&Ls and Daily Performance
        self.petal_pnl = 0.0
        self.mini_pnl = 0.0
        self.unrealized_pnl = 0.0
        self.realized_pnl = 0.0
        self.total_pnl = 0.0
        
        # Configurable thresholds
        self.entry_threshold = 1000.0
        self.target_threshold = 1150.0
        self.sl_threshold = 600.0
        
        self.api_connected = False
        self.execution_in_progress = False

        self.auto_target_enabled = False
        self.auto_target_val = 5000.0    # Net PnL profit target in INR
        self.auto_sl_enabled = False
        self.auto_sl_val = -3000.0       # Net PnL stop loss in INR (negative)
        
        self.auto_square_off_enabled = False
        self.auto_square_off_time = "23:30"  # Market close auto-squareoff
        
        self.spread_buffer = 0.0
        self.auto_contraction_enabled = False
        self.auto_spread_exit_enabled = True
        
        self.paper_trading_mode = True
        self.auto_trading_enabled = False
        self.trade_quantity = 1
        
        # Multi-Broker Configurations
        self.broker = "AngelOne"
        
        # Angel One Integration properties
        self.api_key = os.getenv("ANGELONE_API_KEY", "e72eCDuy")
        self.client_id = os.getenv("ANGELONE_CLIENT_ID", "")
        self.password = os.getenv("ANGELONE_PASSWORD", "")
        self.totp_secret = os.getenv("ANGELONE_TOTP_SECRET", "")
        
        self.petal_symbol = "GOLDPETAL31JUL26"
        self.petal_token = "250000"
        self.mini_symbol = "GOLDM05AUG26"
        self.mini_token = "250001"
        
        self.smart_connect = None
        self.mcx_tokens_cache = {}
        
        # Volume & Depth attributes
        self.gold_petal_volume = 0
        self.gold_petal_buy_qty = 0
        self.gold_petal_sell_qty = 0
        self.gold_mini_volume = 0
        self.gold_mini_buy_qty = 0
        self.gold_mini_sell_qty = 0
        
        self.petal_depth = {"buy": [], "sell": []}
        self.mini_depth = {"buy": [], "sell": []}



        
        # Historical Trades & Analytics
        self.trade_history: List[Dict] = []
        self.trade_counter = 0
        self.total_trades = 0
        self.winning_trades = 0
        self.win_ratio = 0.0
        
        # Capital, Balance & Margin (Groww Style)
        self.total_capital = 500000.0
        self.used_margin = 0.0
        self.available_balance = 500000.0
        self.returns_percentage = 0.0
        
        # Monospace execution logs
        self.logs: List[str] = []
        
        self.manual_trades = []
        self.load_manual_trades()
        
        # Load trade history from persistence file
        self.load_trade_history()
        
    def load_trade_history(self):
        try:
            if os.path.exists("trade_history.json"):
                with open("trade_history.json", "r", encoding="utf-8") as f:
                    self.trade_history = json.load(f)
                
                # Recalculate counters and statistics
                self.trade_counter = len(self.trade_history)
                completed_trades = [t for t in self.trade_history if t.get("status") == "COMPLETED" or "status" not in t]
                self.total_trades = len(completed_trades)
                self.winning_trades = sum(1 for t in completed_trades if float(t.get("pnl", 0.0)) > 0)
                
                if self.total_trades > 0:
                    self.win_ratio = (self.winning_trades / self.total_trades) * 100.0
                else:
                    self.win_ratio = 0.0
                
                self.realized_pnl = sum(float(t.get("pnl", 0.0)) for t in completed_trades)
                self.total_pnl = self.realized_pnl
                self.log(f"[PERSISTENCE] Loaded {len(self.trade_history)} trades from trade_history.json. Realized PnL: INR {self.realized_pnl:.2f}")
            else:
                self.trade_history = []
                self.log("[PERSISTENCE] No trade history file found. Starting fresh.")
        except Exception as e:
            self.log(f"[PERSISTENCE ERROR] Failed to load trade history: {e}")
            self.trade_history = []

    def save_trade_history(self):
        try:
            with open("trade_history.json", "w", encoding="utf-8") as f:
                json.dump(self.trade_history, f, indent=4)
        except Exception as e:
            self.log(f"[PERSISTENCE ERROR] Failed to save trade history: {e}")

    def load_manual_trades(self):
        try:
            if os.path.exists("manual_trades.json"):
                with open("manual_trades.json", "r", encoding="utf-8") as f:
                    self.manual_trades = json.load(f)
                self.log(f"[PERSISTENCE] Loaded {len(self.manual_trades)} manual trades from manual_trades.json.")
            else:
                self.manual_trades = []
                self.log("[PERSISTENCE] No manual trades file found. Starting fresh.")
        except Exception as e:
            self.log(f"[PERSISTENCE ERROR] Failed to load manual trades: {e}")
            self.manual_trades = []

    def save_manual_trades(self):
        try:
            with open("manual_trades.json", "w", encoding="utf-8") as f:
                json.dump(self.manual_trades, f, indent=4)
        except Exception as e:
            self.log(f"[PERSISTENCE ERROR] Failed to save manual trades: {e}")

    def calculate_mcx_charges(self, direction: str, qty: int, petal_entry: float, mini_entry: float, petal_exit: float, mini_exit: float) -> float:
        # GOLDPETAL: 1g size, we trade 100 * qty. GOLDMINI: 100g size (price per 10g), multiplier is 10.
        petal_qty = 100 * qty
        mini_mult = 10 * qty
        
        if direction == "Expansion":
            petal_buy_val = petal_qty * petal_entry
            petal_sell_val = petal_qty * petal_exit
            mini_sell_val = mini_mult * mini_entry
            mini_buy_val = mini_mult * mini_exit
        else:
            petal_sell_val = petal_qty * petal_entry
            petal_buy_val = petal_qty * petal_exit
            mini_buy_val = mini_mult * mini_entry
            mini_sell_val = mini_mult * mini_exit
            
        total_buy_val = petal_buy_val + mini_buy_val
        total_sell_val = petal_sell_val + mini_sell_val
        total_turnover = total_buy_val + total_sell_val
        
        # 1. Brokerage: Flat Rs. 20 per order. 4 orders = Rs. 80.
        brokerage = 20.0 * 4
        
        # 2. Exchange Transaction Charges: 0.0021%
        exchange_charges = 0.000021 * total_turnover
        
        # 3. CTT: 0.01% on sell side
        ctt = 0.0001 * total_sell_val
        
        # 4. SEBI turnover fee: Rs 10 per crore (0.0000001)
        sebi_charges = 0.0000001 * total_turnover
        
        # 5. Stamp Duty: 0.002% on buy side
        stamp_duty = 0.00002 * total_buy_val
        
        # 6. GST: 18% on (brokerage + exchange transaction charges)
        gst = 0.18 * (brokerage + exchange_charges)
        
        return brokerage + exchange_charges + ctt + sebi_charges + stamp_duty + gst

    def log(self, message: str):
        timestamp = get_ist_time_str("%Y-%m-%d %H:%M:%S")
        log_entry = f"[{timestamp}] {message}"
        self.logs.append(log_entry)
        logger.info(message)
        if len(self.logs) > 100:
            self.logs.pop(0)

    def init_angelone_client(self):
        if not self.api_key:
            self.log("[ANGELONE API] Warning: ANGELONE_API_KEY is missing. Real execution will fail.")
            return
            
        import sys
        self.log(f"DEBUG info: Python Executable: {sys.executable}, Version: {sys.version}")
        try:
            from SmartApi import SmartConnect
            self.log("DEBUG info: SmartConnect import succeeded!")
        except Exception as e:
            import traceback
            self.log(f"DEBUG info: SmartConnect import failed: {e}")
            for line in traceback.format_exc().split("\n"):
                if line.strip():
                    self.log(f"DEBUG TRACE: {line.strip()}")
            
        # Auto-resolve tokens from the cached MCX master
        if hasattr(self, "mcx_tokens_cache") and self.mcx_tokens_cache:
            res_petal = self.mcx_tokens_cache.get(self.petal_symbol)
            if res_petal:
                self.petal_token = res_petal
                self.log(f"[SCRIP FINDER] Auto-resolved Leg 1 {self.petal_symbol} token to {self.petal_token}")
                
            res_mini = self.mcx_tokens_cache.get(self.mini_symbol)
            if res_mini:
                self.mini_token = res_mini
                self.log(f"[SCRIP FINDER] Auto-resolved Leg 2 {self.mini_symbol} token to {self.mini_token}")

        if not ANGELONE_SDK_AVAILABLE:
            self.log("[ANGELONE API] Warning: SmartAPI SDK is not installed. Using simulation mode.")
            return

        try:
            self.log("[ANGELONE API] Initializing SmartConnect client...")
            self.smart_connect = SmartConnect(api_key=self.api_key)
            
            if self.client_id and self.password and self.totp_secret:
                totp_strip = self.totp_secret.strip()
                if len(totp_strip) == 6 and totp_strip.isdigit():
                    self.log("[ANGELONE API] ERROR: You entered a temporary 6-digit passcode in the 'TOTP Secret' field. Please enter your 2FA Secret Key (Base32) instead. Bypassing login to protect session.")
                    self.smart_connect = None
                    return
                    
                self.log(f"[ANGELONE API] Authenticating client {self.client_id}...")
                totp = pyotp.TOTP(self.totp_secret).now()
                session = self.smart_connect.generateSession(self.client_id, self.password, totp)
                if session.get("status") == True:
                    self.log("[ANGELONE API] Authentication successful.")
                    # Auto-resolve tokens using searchScrip API only if not already resolved
                    if not self.petal_token or self.petal_token == "250000":
                        res_petal = self.resolve_scrip_token_via_api(self.petal_symbol)
                        if res_petal:
                            self.petal_token = res_petal
                    if not self.mini_token or self.mini_token == "250001":
                        res_mini = self.resolve_scrip_token_via_api(self.mini_symbol)
                        if res_mini:
                            self.mini_token = res_mini
                else:
                    self.log(f"[ANGELONE API] Authentication failed: {session.get('message')}. Using simulation.")
                    self.smart_connect = None
            else:
                self.log("[ANGELONE API] Warning: Missing login credentials (Client ID, Password, or TOTP Secret) for Angel One. Using simulation.")
                self.smart_connect = None
        except Exception as e:
            self.log(f"[ANGELONE API] Initialization failed: {e}. Falling back to simulation.")
            self.smart_connect = None

    def resolve_scrip_token_via_api(self, symbol: str) -> str:
        if not self.smart_connect:
            return ""
        try:
            self.log(f"[API LOOKUP] Searching token for '{symbol}' on MCX...")
            res = self.smart_connect.searchScrip(exchange="MCX", searchscrip=symbol)
            if res and res.get("status") == True:
                data = res.get("data", [])
                
                # Flexible matching helper
                def find_match(items):
                    for item in items:
                        ts = item.get("tradingsymbol", "")
                        if ts == symbol or ts == f"{symbol}FUT" or ts.startswith(symbol):
                            return item.get("symboltoken", ""), ts
                    return "", ""
                
                if isinstance(data, list):
                    token, actual_symbol = find_match(data)
                elif isinstance(data, dict):
                    token, actual_symbol = find_match([data])
                else:
                    token, actual_symbol = "", ""
                    
                if token:
                    self.log(f"[API LOOKUP] Auto-resolved '{symbol}' -> '{actual_symbol}' (Token: {token})")
                    # Update local properties to match the exact broker symbol
                    if symbol == self.petal_symbol:
                        self.petal_symbol = actual_symbol
                    elif symbol == self.mini_symbol:
                        self.mini_symbol = actual_symbol
                    return token
                    
            self.log(f"[API LOOKUP] Search returned no direct matches for '{symbol}' on MCX.")
        except Exception as e:
            self.log(f"[API LOOKUP] Search failed for '{symbol}': {e}")
        return ""



# Global State Instance
system_state = TradingSystem()

# Token verification helper
def verify_token(token: str = None, authorization: str = Header(None)):
    auth_token = os.getenv("AUTH_TOKEN", "secret_arbitrage_token_2026")
    provided_token = None
    if token:
        provided_token = token
    elif authorization and authorization.startswith("Bearer "):
        provided_token = authorization.split(" ")[1]
        
    if provided_token != auth_token:
        raise HTTPException(status_code=401, detail="Invalid Authentication Token")

# ----------------- WebSocket Connection Manager -----------------
class ConnectionManager:
    def __init__(self):
        self.active_connections: List[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        self.active_connections.append(websocket)

    def disconnect(self, websocket: WebSocket):
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)

    async def broadcast(self, message: dict):
        for connection in list(self.active_connections):
            try:
                await connection.send_json(message)
            except (Exception, BaseException):
                self.disconnect(connection)

manager = ConnectionManager()

async def broadcast_system_state():
    await manager.broadcast({
        "gold_petal_ltp": round(system_state.gold_petal_ltp, 2),
        "gold_mini_ltp": round(system_state.gold_mini_ltp, 2),
        "spread": round(system_state.spread, 2),
        "depth_buy_spread": round(system_state.depth_buy_spread, 2),
        "depth_sell_spread": round(system_state.depth_sell_spread, 2),
        
        "is_in_position": system_state.is_in_position,
        "position_direction": system_state.position_direction,
        "system_status": system_state.system_status,
        
        "petal_entry_price": round(system_state.petal_entry_price, 2),
        "mini_entry_price": round(system_state.mini_entry_price, 2),
        "entry_spread": round(system_state.entry_spread, 2),
        
        "petal_pnl": round(system_state.petal_pnl, 2),
        "mini_pnl": round(system_state.mini_pnl, 2),
        "unrealized_pnl": round(system_state.unrealized_pnl, 2),
        "realized_pnl": round(system_state.realized_pnl, 2),
        "total_pnl": round(system_state.total_pnl, 2),
        
        "total_capital": round(system_state.total_capital, 2),
        "used_margin": round(system_state.used_margin, 2),
        "available_balance": round(system_state.available_balance, 2),
        "returns_percentage": round(system_state.returns_percentage, 2),
        
        "entry_threshold": system_state.entry_threshold,
        "target_threshold": system_state.target_threshold,
        "sl_threshold": system_state.sl_threshold,
        
        "api_connected": system_state.api_connected,
        "petal_depth": system_state.petal_depth,
        "mini_depth": system_state.mini_depth,
        
        "auto_target_enabled": system_state.auto_target_enabled,
        "auto_target_val": system_state.auto_target_val,
        "auto_sl_enabled": system_state.auto_sl_enabled,
        "auto_sl_val": system_state.auto_sl_val,
        "auto_square_off_enabled": system_state.auto_square_off_enabled,
        "auto_square_off_time": system_state.auto_square_off_time,
        "spread_buffer": system_state.spread_buffer,
        "auto_contraction_enabled": system_state.auto_contraction_enabled,
        "auto_spread_exit_enabled": system_state.auto_spread_exit_enabled,
        "paper_trading_mode": system_state.paper_trading_mode,
        "auto_trading_enabled": system_state.auto_trading_enabled,
        "trade_quantity": system_state.trade_quantity,
        "broker": system_state.broker,
        "api_key": system_state.api_key,
        "client_id": system_state.client_id,
        "password": system_state.password,
        "totp_secret": system_state.totp_secret,
        "petal_symbol": system_state.petal_symbol,
        "petal_token": system_state.petal_token,
        "mini_symbol": system_state.mini_symbol,
        "mini_token": system_state.mini_token,
        "gold_petal_volume": system_state.gold_petal_volume,
        "gold_petal_buy_qty": system_state.gold_petal_buy_qty,
        "gold_petal_sell_qty": system_state.gold_petal_sell_qty,
        "gold_mini_volume": system_state.gold_mini_volume,
        "gold_mini_buy_qty": system_state.gold_mini_buy_qty,
        "gold_mini_sell_qty": system_state.gold_mini_sell_qty,

        
        "win_ratio": round(system_state.win_ratio, 2),
        "total_trades": system_state.total_trades,
        "trade_history": system_state.trade_history,
        "manual_trades": system_state.manual_trades,
        
        "logs": system_state.logs
    })

# ----------------- Order execution -----------------
# ----------------- Order execution -----------------
def is_liquidity_sufficient(petal_action: str, mini_action: str, qty: int) -> bool:
    required_petal = qty * 100
    required_mini = qty
    
    if petal_action == "BUY":
        available_petal = system_state.gold_petal_sell_qty
    else:
        available_petal = system_state.gold_petal_buy_qty
        
    if mini_action == "BUY":
        available_mini = system_state.gold_mini_sell_qty
    else:
        available_mini = system_state.gold_mini_buy_qty
        
    # If market depth volumes are 0 (e.g., system startup/simulation init), bypass check as failsafe
    if available_petal == 0 or available_mini == 0:
        return True
        
    if available_petal < required_petal or available_mini < required_mini:
        return False

    # 2. Bid-Ask Spread Check (Slippage Prevention)
    try:
        if (isinstance(system_state.petal_depth, dict) and 
                "buy" in system_state.petal_depth and len(system_state.petal_depth["buy"]) > 0 and
                "sell" in system_state.petal_depth and len(system_state.petal_depth["sell"]) > 0):
            petal_bid = float(system_state.petal_depth["buy"][0]["price"])
            petal_ask = float(system_state.petal_depth["sell"][0]["price"])
            if (petal_ask - petal_bid) > 15.0:
                system_state.log(f"[LIQUIDITY SHIELD] Trade skipped: GOLDPETAL Bid-Ask gap too wide ({petal_ask - petal_bid:.2f} > 15.0).")
                return False

        if (isinstance(system_state.mini_depth, dict) and 
                "buy" in system_state.mini_depth and len(system_state.mini_depth["buy"]) > 0 and
                "sell" in system_state.mini_depth and len(system_state.mini_depth["sell"]) > 0):
            mini_bid = float(system_state.mini_depth["buy"][0]["price"])
            mini_ask = float(system_state.mini_depth["sell"][0]["price"])
            if (mini_ask - mini_bid) > 150.0:
                system_state.log(f"[LIQUIDITY SHIELD] Trade skipped: GOLDMINI Bid-Ask gap too wide ({mini_ask - mini_bid:.2f} > 150.0).")
                return False
    except Exception as e:
        logger.warning(f"Error parsing depth for bid-ask gap check: {e}")

    return True

async def check_real_orders_status(order_ids: List[str]) -> Dict[str, str]:
    if not system_state.smart_connect:
        return {}
    try:
        loop = asyncio.get_running_loop()
        response = await loop.run_in_executor(
            None,
            lambda: system_state.smart_connect.orderBook()
        )
        if response and response.get("status") == True:
            order_list = response.get("data", [])
            status_map = {}
            for o in order_list:
                oid = o.get("orderid")
                if oid in order_ids:
                    status_map[oid] = o.get("status", "").upper()
            return status_map
    except Exception as e:
        system_state.log(f"[LIVE ORDER STATUS] Error checking order book: {e}")
    return {}

async def cancel_real_order(order_id: str, variety: str = "NORMAL"):
    if not system_state.smart_connect:
        return
    try:
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(
            None,
            lambda: system_state.smart_connect.cancelOrder(order_id, variety)
        )
        system_state.log(f"[LIVE ORDER] Cancelled order {order_id}")
    except Exception as e:
        system_state.log(f"[LIVE ORDER ERROR] Failed to cancel order {order_id}: {e}")

def record_failed_attempt(direction: str, status: str, reason: str, is_entry: bool):
    system_state.trade_counter += 1
    t_time = get_ist_time_str("%H:%M:%S")
    t_date = get_ist_time_str("%Y-%m-%d")
    
    trade_record = {
        "id": system_state.trade_counter,
        "date": t_date,
        "direction": direction,
        "status": status,
        "entry_time": t_time if is_entry else "--",
        "exit_time": t_time if not is_entry else "--",
        "petal_action": "--",
        "mini_action": "--",
        "petal_entry": 0.0,
        "mini_entry": 0.0,
        "petal_exit": 0.0,
        "mini_exit": 0.0,
        "entry_spread": 0.0,
        "actual_entry_spread": 0.0,
        "entry_slippage": 0.0,
        "exit_spread": 0.0,
        "actual_exit_spread": 0.0,
        "exit_slippage": 0.0,
        "petal_entry_type": "--",
        "mini_entry_type": "--",
        "petal_exit_type": "--",
        "mini_exit_type": "--",
        "petal_pnl": 0.0,
        "mini_pnl": 0.0,
        "pnl": 0.0,
        "reason": reason,
        "details": f"{'Entry' if is_entry else 'Exit'} attempt failed/cancelled: {reason}"
    }
    system_state.trade_history.append(trade_record)
    system_state.save_trade_history()

async def execute_trade(petal_action: str, mini_action: str, check_liquidity: bool = True, is_entry: bool = True, qty: int = None) -> dict:
    if qty is None:
        qty = system_state.trade_quantity
    required_petal = qty * 100
    required_mini = qty
    
    direction = system_state.position_direction if not is_entry else ("Expansion" if petal_action == "BUY" else "Contraction")
    
    # 1. Option A: Enforce Depth Guard Check (Must have valid depth for execution sides)
    petal_side = "sell" if petal_action == "BUY" else "buy"
    mini_side = "sell" if mini_action == "BUY" else "buy"
    
    if (not isinstance(system_state.petal_depth, dict) or 
            petal_side not in system_state.petal_depth or 
            not system_state.petal_depth[petal_side] or 
            len(system_state.petal_depth[petal_side]) == 0):
        msg = f"Missing GOLDPETAL depth for {petal_side} side."
        system_state.log(f"[DEPTH GUARD] Trade skipped: {msg}")
        record_failed_attempt(direction, "FAILED", f"Depth Guard: {msg}", is_entry)
        return {"success": False, "status": "FAILED", "reason": f"Depth Guard: {msg}"}

    if (not isinstance(system_state.mini_depth, dict) or 
            mini_side not in system_state.mini_depth or 
            not system_state.mini_depth[mini_side] or 
            len(system_state.mini_depth[mini_side]) == 0):
        msg = f"Missing GOLDMINI depth for {mini_side} side."
        system_state.log(f"[DEPTH GUARD] Trade skipped: {msg}")
        record_failed_attempt(direction, "FAILED", f"Depth Guard: {msg}", is_entry)
        return {"success": False, "status": "FAILED", "reason": f"Depth Guard: {msg}"}
        
    # 2. Liquidity Shield (Bid-Ask Gap Check)
    if check_liquidity:
        if not is_liquidity_sufficient(petal_action, mini_action, qty):
            available_p = system_state.gold_petal_sell_qty if petal_action == "BUY" else system_state.gold_petal_buy_qty
            available_m = system_state.gold_mini_sell_qty if mini_action == "BUY" else system_state.gold_mini_buy_qty
            msg = f"Insufficient depth volume or Bid-Ask gap too wide. Petal: Need {required_petal}, Got {available_p}. Mini: Need {required_mini}, Got {available_m}."
            system_state.log(f"[LIQUIDITY SHIELD] Trade skipped: {msg}")
            record_failed_attempt(direction, "FAILED", f"Liquidity Pre-check: {msg}", is_entry)
            return {"success": False, "status": "FAILED", "reason": f"Liquidity Pre-check: {msg}"}

    # 3. Calculate VWAP (Volume-Weighted Average Price) from Depth
    petal_price = get_depth_average_price(system_state.petal_depth, petal_side, required_petal, system_state.gold_petal_ltp)
    mini_price = get_depth_average_price(system_state.mini_depth, mini_side, required_mini, system_state.gold_mini_ltp)

    system_state.log(f"[MARKET EXECUTION] Dispatching market orders: GOLDPETAL {petal_action} @ {petal_price:.2f}, GOLDMINI {mini_action} @ {mini_price:.2f}")

    if system_state.paper_trading_mode:
        # Paper Trading execution: fill instantly at average price
        system_state.log(f"[PAPER MARKET FILL] GOLDPETAL {petal_action} filled @ MARKET {petal_price:.2f}")
        system_state.log(f"[PAPER MARKET FILL] GOLDMINI {mini_action} filled @ MARKET {mini_price:.2f}")
        
        return {
            "success": True,
            "status": "COMPLETED",
            "reason": "Matched on Depth Market (VWAP)",
            "petal_fill_price": petal_price,
            "mini_fill_price": mini_price,
            "petal_order_type": "MARKET",
            "mini_order_type": "MARKET"
        }
    else:
        # Live MCX Broker Execution using AngelOne SmartAPI
        if not ANGELONE_SDK_AVAILABLE or not system_state.smart_connect:
            system_state.log("[ANGELONE API] Error: Client not initialized. Cannot place live orders.")
            record_failed_attempt(direction, "FAILED", "AngelOne client not initialized", is_entry)
            return {"success": False, "status": "FAILED", "reason": "AngelOne client not initialized"}
            
        async def place_real_market_order(symbol: str, token: str, action: str, order_qty: int):
            order_params = {
                "variety": "NORMAL",
                "tradingsymbol": symbol,
                "symboltoken": token,
                "transactiontype": action,
                "exchange": "MCX",
                "ordertype": "MARKET",
                "producttype": "CARRYFORWARD",
                "duration": "DAY",
                "quantity": str(order_qty)
            }
            try:
                loop = asyncio.get_running_loop()
                response = await loop.run_in_executor(
                    None,
                    lambda: system_state.smart_connect.placeOrder(order_params)
                )
                if isinstance(response, str):
                    return response
                elif isinstance(response, dict):
                    return response.get("data", {}).get("orderid", "")
                return str(response)
            except Exception as e:
                system_state.log(f"[LIVE MARKET ORDER ERROR] Failed to place order for {symbol}: {e}")
                return ""

        # Place the market orders concurrently
        petal_order_id = await place_real_market_order(system_state.petal_symbol, system_state.petal_token, petal_action, required_petal)
        mini_order_id = await place_real_market_order(system_state.mini_symbol, system_state.mini_token, mini_action, required_mini)
        
        if not petal_order_id and not mini_order_id:
            system_state.log("[LIVE ORDER ERROR] Both market order placements failed.")
            record_failed_attempt(direction, "FAILED", "Market order placements failed", is_entry)
            return {"success": False, "status": "FAILED", "reason": "Market order placements failed"}
            
        petal_filled = not petal_order_id
        mini_filled = not mini_order_id
        
        petal_fill_price = petal_price
        mini_fill_price = mini_price
        petal_type = "MARKET"
        mini_type = "MARKET"
        
        timeout = 5.0
        elapsed = 0.0
        interval = 0.2
        
        while elapsed < timeout:
            await asyncio.sleep(interval)
            elapsed += interval
            
            # Query status
            status_map = await check_real_orders_status([petal_order_id, mini_order_id])
            
            if petal_order_id and not petal_filled:
                status = status_map.get(petal_order_id)
                if status == "COMPLETE":
                    petal_filled = True
                    petal_fill_price = system_state.gold_petal_ltp
                    system_state.log(f"[LIVE MARKET FILL] Leg 1: GOLDPETAL filled @ MARKET {petal_fill_price:.2f}")
                elif status in ["REJECTED", "CANCELLED"]:
                    system_state.log(f"[LIVE ORDER CANCEL/REJECT] Leg 1: GOLDPETAL order {status.lower()}")
                    break
                    
            if mini_order_id and not mini_filled:
                status = status_map.get(mini_order_id)
                if status == "COMPLETE":
                    mini_filled = True
                    mini_fill_price = system_state.gold_mini_ltp
                    system_state.log(f"[LIVE MARKET FILL] Leg 2: GOLDMINI filled @ MARKET {mini_fill_price:.2f}")
                elif status in ["REJECTED", "CANCELLED"]:
                    system_state.log(f"[LIVE ORDER CANCEL/REJECT] Leg 2: GOLDMINI order {status.lower()}")
                    break
                    
            if petal_filled and mini_filled:
                break

        # Cancel/reverse if not completed
        if not petal_filled and not mini_filled:
            # Market orders usually execute instantly, but if stuck in pending (rare), cancel them.
            if petal_order_id:
                await cancel_real_order(petal_order_id)
            if mini_order_id:
                await cancel_real_order(mini_order_id)
            system_state.log("[LIVE TIMEOUT] Both orders timed out without fill. Orders cancelled.")
            record_failed_attempt(direction, "CANCELLED", "Timeout - no legs filled", is_entry)
            return {"success": False, "status": "CANCELLED", "reason": "Timeout - no legs filled"}
            
        # Emergency rollback if partial fill failed to complete
        if petal_filled and not mini_filled:
            system_state.log("[EMERGENCY ROLLBACK] Leg 1 filled, Leg 2 failed. Reversing Leg 1...")
            if petal_order_id:
                await cancel_real_order(petal_order_id)
            rollback_action = "SELL" if petal_action == "BUY" else "BUY"
            await place_real_market_order(system_state.petal_symbol, system_state.petal_token, rollback_action, required_petal)
            record_failed_attempt(direction, "FAILED", "Leg 1 filled, Leg 2 failed. Rolled back.", is_entry)
            return {"success": False, "status": "FAILED", "reason": "Partial fill Leg 2 failure"}
            
        if mini_filled and not petal_filled:
            system_state.log("[EMERGENCY ROLLBACK] Leg 2 filled, Leg 1 failed. Reversing Leg 2...")
            if mini_order_id:
                await cancel_real_order(mini_order_id)
            rollback_action = "SELL" if mini_action == "BUY" else "BUY"
            await place_real_market_order(system_state.mini_symbol, system_state.mini_token, rollback_action, required_mini)
            record_failed_attempt(direction, "FAILED", "Leg 2 filled, Leg 1 failed. Rolled back.", is_entry)
            return {"success": False, "status": "FAILED", "reason": "Partial fill Leg 1 failure"}
            
        return {
            "success": True,
            "status": "COMPLETED",
            "reason": "Matched on Depth Market (VWAP)",
            "petal_fill_price": petal_fill_price,
            "mini_fill_price": mini_fill_price,
            "petal_order_type": petal_type,
            "mini_order_type": mini_type
        }

async def execute_position_exit(exit_reason: str):
    global system_state
    
    direction = system_state.position_direction
    
    # Reverse actions
    petal_action = "SELL" if direction == "Expansion" else "BUY"
    mini_action = "BUY" if direction == "Expansion" else "SELL"
    
    result = await execute_trade(petal_action, mini_action, check_liquidity=False, is_entry=False)
    if not result["success"]:
        system_state.log(f"[EXIT ERROR] Square off trade execution failed: {result['reason']}")
        return
        
    petal_exit = result["petal_fill_price"]
    mini_exit = result["mini_fill_price"]
    petal_exit_type = result["petal_order_type"]
    mini_exit_type = result["mini_order_type"]
    
    actual_exit_spread = (petal_exit * 10.0) - mini_exit
    expected_exit_spread = system_state.expected_exit_spread
    
    # P&L Formulas based on physical multipliers (100x Petal, 10x Mini)
    qty = system_state.trade_quantity
    if direction == "Expansion":  # Buy Petal, Sell Mini
        p_pnl = (petal_exit - system_state.petal_entry_price) * 100.0 * qty
        m_pnl = (system_state.mini_entry_price - mini_exit) * 10.0 * qty
        exit_slippage = expected_exit_spread - actual_exit_spread
    else:  # Sell Petal, Buy Mini
        p_pnl = (system_state.petal_entry_price - petal_exit) * 100.0 * qty
        m_pnl = (mini_exit - system_state.mini_entry_price) * 10.0 * qty
        exit_slippage = actual_exit_spread - expected_exit_spread
        
    trade_pnl = p_pnl + m_pnl
    charges = system_state.calculate_mcx_charges(
        direction, qty, system_state.petal_entry_price, system_state.mini_entry_price, petal_exit, mini_exit
    )
    net_pnl = trade_pnl - charges
    system_state.realized_pnl += net_pnl
    
    # Calculate stats
    system_state.total_trades += 1
    if net_pnl > 0:
        system_state.winning_trades += 1
    system_state.win_ratio = (system_state.winning_trades / system_state.total_trades) * 100.0
    
    # Create History record
    system_state.trade_counter += 1
    exit_time = get_ist_time_str("%H:%M:%S")
    exit_date = get_ist_time_str("%Y-%m-%d")
    
    details_str = f"Target Spread: {expected_exit_spread:.2f}, Filled Spread: {actual_exit_spread:.2f} (Slippage: {exit_slippage:+.2f}). Charges: INR {charges:.2f} (Gross: {trade_pnl:.2f}, Net: {net_pnl:.2f}). Entry Type: [P:{system_state.petal_entry_type}/M:{system_state.mini_entry_type}], Exit Type: [P:{petal_exit_type}/M:{mini_exit_type}]."
    
    trade_record = {
        "id": system_state.trade_counter,
        "date": system_state.entry_date if system_state.entry_date != "--" else exit_date,
        "direction": direction,
        "status": "COMPLETED",
        "entry_time": system_state.entry_time,
        "exit_time": exit_time,
        "petal_action": "BUY" if direction == "Expansion" else "SELL",
        "mini_action": "SELL" if direction == "Expansion" else "BUY",
        "petal_entry": round(system_state.petal_entry_price, 2),
        "mini_entry": round(system_state.mini_entry_price, 2),
        "petal_exit": round(petal_exit, 2),
        "mini_exit": round(mini_exit, 2),
        "entry_spread": round(system_state.expected_entry_spread, 2),
        "actual_entry_spread": round(system_state.entry_spread, 2),
        "entry_slippage": round(system_state.entry_slippage, 2),
        "exit_spread": round(expected_exit_spread, 2),
        "actual_exit_spread": round(actual_exit_spread, 2),
        "exit_slippage": round(exit_slippage, 2),
        "petal_entry_type": system_state.petal_entry_type,
        "mini_entry_type": system_state.mini_entry_type,
        "petal_exit_type": petal_exit_type,
        "mini_exit_type": mini_exit_type,
        "petal_pnl": round(p_pnl, 2),
        "mini_pnl": round(m_pnl, 2),
        "gross_pnl": round(trade_pnl, 2),
        "charges": round(charges, 2),
        "pnl": round(net_pnl, 2),
        "reason": exit_reason,
        "details": details_str
    }
    system_state.trade_history.append(trade_record)
    system_state.save_trade_history()
    
    system_state.log(f"POSITION SQUARED OFF ({exit_reason}): Net PnL: INR {trade_pnl:+.2f} (Petal: {p_pnl:+.2f}, Mini: {m_pnl:+.2f}). Slippage: {exit_slippage:+.2f}")
    
    # Reset Position state
    system_state.is_in_position = False
    system_state.position_direction = None
    system_state.petal_entry_price = 0.0
    system_state.mini_entry_price = 0.0
    system_state.entry_spread = 0.0
    system_state.expected_entry_spread = 0.0
    system_state.expected_exit_spread = 0.0
    system_state.petal_entry_type = "--"
    system_state.mini_entry_type = "--"
    system_state.entry_slippage = 0.0
    system_state.entry_time = "--"
    system_state.entry_date = "--"
    system_state.entry_reason = "--"
    
    system_state.petal_pnl = 0.0
    system_state.mini_pnl = 0.0
    system_state.unrealized_pnl = 0.0
    
    if system_state.system_status != "Halted":
        system_state.system_status = "Active"
        
    await broadcast_system_state()

# ----------------- Trading Engine and Live Tickers -----------------
def get_depth_average_price(depth: dict, side: str, required_qty: int, default_price: float) -> float:
    levels = depth.get(side, [])
    if not levels:
        return default_price
    
    accum_qty = 0
    total_cost = 0.0
    for level in levels:
        try:
            p = float(level.get("price", 0.0))
            q = int(level.get("quantity", 0))
        except (ValueError, TypeError):
            continue
            
        if p <= 0 or q <= 0:
            continue
            
        needed = required_qty - accum_qty
        if q >= needed:
            total_cost += needed * p
            accum_qty += needed
            break
        else:
            total_cost += q * p
            accum_qty += q
            
    if accum_qty < required_qty:
        if accum_qty > 0 and len(levels) > 0:
            try:
                last_price = float(levels[-1].get("price", default_price))
            except (ValueError, TypeError):
                last_price = default_price
            total_cost += (required_qty - accum_qty) * last_price
            accum_qty = required_qty
        else:
            return default_price
            
    return total_cost / required_qty

async def run_auto_entry(direction: str, petal_action: str, mini_action: str, expected_entry_spread: float):
    if system_state.execution_in_progress:
        return
    system_state.execution_in_progress = True
    try:
        result = await execute_trade(petal_action, mini_action, check_liquidity=True, is_entry=True)
        if result["success"]:
            system_state.is_in_position = True
            system_state.position_direction = direction
            system_state.system_status = "In-Position"
            
            system_state.petal_entry_price = result["petal_fill_price"]
            system_state.mini_entry_price = result["mini_fill_price"]
            system_state.petal_entry_type = result["petal_order_type"]
            system_state.mini_entry_type = result["mini_order_type"]
            system_state.entry_spread = (system_state.petal_entry_price * 10.0) - system_state.mini_entry_price
            
            system_state.expected_entry_spread = expected_entry_spread
            if direction == "Expansion":
                system_state.entry_slippage = system_state.entry_spread - system_state.expected_entry_spread
            else:
                system_state.entry_slippage = system_state.expected_entry_spread - system_state.entry_spread
                
            system_state.entry_time = get_ist_time_str("%H:%M:%S")
            system_state.entry_date = get_ist_time_str("%Y-%m-%d")
            system_state.entry_reason = f"Auto-Entry ({direction})"
            
            system_state.log(f"AUTO ENTRY ({direction}): Expected Spread {system_state.expected_entry_spread:.2f}, Filled Spread {system_state.entry_spread:.2f} (Slippage: {system_state.entry_slippage:+.2f}). Fill price Petal {system_state.petal_entry_price:.2f}, Mini {system_state.mini_entry_price:.2f}")
    except Exception as e:
        system_state.log(f"[AUTO ENTRY ERROR] {e}")
    finally:
        system_state.execution_in_progress = False
        await broadcast_system_state()

async def run_auto_exit(exit_reason: str, expected_exit_spread: float):
    if system_state.execution_in_progress:
        return
    system_state.execution_in_progress = True
    try:
        system_state.expected_exit_spread = expected_exit_spread
        await execute_position_exit(exit_reason)
    except Exception as e:
        system_state.log(f"[AUTO EXIT ERROR] {e}")
    finally:
        system_state.execution_in_progress = False
        await broadcast_system_state()

async def execute_netting_manual_trades(new_direction: str, qty: int, expected_entry_spread: float, pending_trade: dict = None) -> dict:
    global system_state
    
    # 1. Calculate how much quantity we can net
    opposite_trades = [t for t in system_state.manual_trades if t.get("status") == "Open" and t.get("direction") != new_direction]
    opposite_qty = sum(t.get("quantity", 0) for t in opposite_trades)
    
    net_qty = min(qty, opposite_qty)
    open_qty = qty - net_qty
    
    petal_action = "BUY" if new_direction == "Expansion" else "SELL"
    mini_action = "SELL" if new_direction == "Expansion" else "BUY"
    
    net_success = True
    open_success = True
    net_reason = ""
    open_reason = ""
    
    # 2. Process Netting Portion
    if net_qty > 0:
        system_state.log(f"[NETTING] Executing offsetting orders for quantity {net_qty} in direction {new_direction}...")
        result = await execute_trade(petal_action, mini_action, check_liquidity=True, is_entry=True, qty=net_qty)
        if result["success"]:
            petal_exit = result["petal_fill_price"]
            mini_exit = result["mini_fill_price"]
            petal_exit_type = result["petal_order_type"]
            mini_exit_type = result["mini_order_type"]
            actual_exit_spread = (petal_exit * 10.0) - mini_exit
            expected_exit_spread = system_state.depth_sell_spread if new_direction == "Contraction" else system_state.depth_buy_spread
            
            remaining_net_qty = net_qty
            for t in opposite_trades:
                t_qty = t.get("quantity", 0)
                t_dir = t.get("direction")
                
                if t_qty <= remaining_net_qty:
                    # Fully close this trade
                    t["status"] = "Closed"
                    t["petal_exit_price"] = petal_exit
                    t["mini_exit_price"] = mini_exit
                    t["petal_exit_type"] = petal_exit_type
                    t["mini_exit_type"] = mini_exit_type
                    t["exit_spread"] = expected_exit_spread
                    t["actual_exit_spread"] = actual_exit_spread
                    
                    if t_dir == "Expansion":
                        p_pnl = (petal_exit - t.get("petal_entry_price", 0.0)) * 100.0 * t_qty
                        m_pnl = (t.get("mini_entry_price", 0.0) - mini_exit) * 10.0 * t_qty
                        exit_slippage = expected_exit_spread - actual_exit_spread
                    else:
                        p_pnl = (t.get("petal_entry_price", 0.0) - petal_exit) * 100.0 * t_qty
                        m_pnl = (mini_exit - t.get("mini_entry_price", 0.0)) * 10.0 * t_qty
                        exit_slippage = actual_exit_spread - expected_exit_spread
                        
                    trade_pnl = p_pnl + m_pnl
                    charges = system_state.calculate_mcx_charges(
                        t_dir, t_qty, t.get("petal_entry_price", 0.0), t.get("mini_entry_price", 0.0), petal_exit, mini_exit
                    )
                    net_pnl = trade_pnl - charges
                    
                    t["petal_pnl"] = p_pnl
                    t["mini_pnl"] = m_pnl
                    t["pnl"] = net_pnl
                    t["charges"] = charges
                    t["exit_time"] = time.strftime("%H:%M:%S")
                    t["exit_date"] = time.strftime("%Y-%m-%d")
                    t["exit_slippage"] = exit_slippage
                    
                    system_state.realized_pnl += net_pnl
                    system_state.total_trades += 1
                    if net_pnl > 0:
                        system_state.winning_trades += 1
                        
                    history_record = {
                        "id": len(system_state.trade_history) + 1,
                        "date": t.get("entry_date", time.strftime("%Y-%m-%d")),
                        "direction": t_dir,
                        "status": "COMPLETED",
                        "entry_time": t.get("entry_time"),
                        "exit_time": t["exit_time"],
                        "petal_action": "BUY" if t_dir == "Expansion" else "SELL",
                        "mini_action": "SELL" if t_dir == "Expansion" else "BUY",
                        "petal_entry": round(t.get("petal_entry_price", 0.0), 2),
                        "mini_entry": round(t.get("mini_entry_price", 0.0), 2),
                        "petal_exit": round(petal_exit, 2),
                        "mini_exit": round(mini_exit, 2),
                        "entry_spread": round(t.get("expected_entry_spread", 0.0), 2),
                        "actual_entry_spread": round(t.get("entry_spread", 0.0), 2),
                        "entry_slippage": round(t.get("entry_slippage", 0.0), 2),
                        "exit_spread": round(expected_exit_spread, 2),
                        "actual_exit_spread": round(actual_exit_spread, 2),
                        "exit_slippage": round(exit_slippage, 2),
                        "petal_entry_type": t.get("petal_entry_type", "--"),
                        "mini_entry_type": t.get("mini_entry_type", "--"),
                        "petal_exit_type": petal_exit_type,
                        "mini_exit_type": mini_exit_type,
                        "petal_pnl": round(p_pnl, 2),
                        "mini_pnl": round(m_pnl, 2),
                        "gross_pnl": round(trade_pnl, 2),
                        "charges": round(charges, 2),
                        "pnl": round(net_pnl, 2),
                        "reason": "Netting-Close",
                        "details": f"Manual Trade {t['id']} fully offset by new trade. Net P&L: {net_pnl:.2f}."
                    }
                    system_state.trade_history.append(history_record)
                    system_state.log(f"MANUAL POSITION NETTING CLOSE: ID {t['id']} fully closed. Net: {net_pnl:.2f}")
                    remaining_net_qty -= t_qty
                else:
                    # Partially close this trade
                    t["quantity"] = t_qty - remaining_net_qty
                    closed_qty = remaining_net_qty
                    
                    if t_dir == "Expansion":
                        p_pnl = (petal_exit - t.get("petal_entry_price", 0.0)) * 100.0 * closed_qty
                        m_pnl = (t.get("mini_entry_price", 0.0) - mini_exit) * 10.0 * closed_qty
                        exit_slippage = expected_exit_spread - actual_exit_spread
                    else:
                        p_pnl = (t.get("petal_entry_price", 0.0) - petal_exit) * 100.0 * closed_qty
                        m_pnl = (mini_exit - t.get("mini_entry_price", 0.0)) * 10.0 * closed_qty
                        exit_slippage = actual_exit_spread - expected_exit_spread
                        
                    trade_pnl = p_pnl + m_pnl
                    charges = system_state.calculate_mcx_charges(
                        t_dir, closed_qty, t.get("petal_entry_price", 0.0), t.get("mini_entry_price", 0.0), petal_exit, mini_exit
                    )
                    net_pnl = trade_pnl - charges
                    
                    system_state.realized_pnl += net_pnl
                    system_state.total_trades += 1
                    if net_pnl > 0:
                        system_state.winning_trades += 1
                        
                    history_record = {
                        "id": len(system_state.trade_history) + 1,
                        "date": t.get("entry_date", time.strftime("%Y-%m-%d")),
                        "direction": t_dir,
                        "status": "COMPLETED",
                        "entry_time": t.get("entry_time"),
                        "exit_time": time.strftime("%H:%M:%S"),
                        "petal_action": "BUY" if t_dir == "Expansion" else "SELL",
                        "mini_action": "SELL" if t_dir == "Expansion" else "BUY",
                        "petal_entry": round(t.get("petal_entry_price", 0.0), 2),
                        "mini_entry": round(t.get("mini_entry_price", 0.0), 2),
                        "petal_exit": round(petal_exit, 2),
                        "mini_exit": round(mini_exit, 2),
                        "entry_spread": round(t.get("expected_entry_spread", 0.0), 2),
                        "actual_entry_spread": round(t.get("entry_spread", 0.0), 2),
                        "entry_slippage": round(t.get("entry_slippage", 0.0), 2),
                        "exit_spread": round(expected_exit_spread, 2),
                        "actual_exit_spread": round(actual_exit_spread, 2),
                        "exit_slippage": round(exit_slippage, 2),
                        "petal_entry_type": t.get("petal_entry_type", "--"),
                        "mini_entry_type": t.get("mini_entry_type", "--"),
                        "petal_exit_type": petal_exit_type,
                        "mini_exit_type": mini_exit_type,
                        "petal_pnl": round(p_pnl, 2),
                        "mini_pnl": round(m_pnl, 2),
                        "gross_pnl": round(trade_pnl, 2),
                        "charges": round(charges, 2),
                        "pnl": round(net_pnl, 2),
                        "reason": "Netting-Close-Partial",
                        "details": f"Manual Trade {t['id']} partially offset (Qty {closed_qty}). Net P&L: {net_pnl:.2f}."
                    }
                    system_state.trade_history.append(history_record)
                    system_state.log(f"MANUAL POSITION NETTING CLOSE: ID {t['id']} partially closed ({closed_qty} lots). Net: {net_pnl:.2f}")
                    remaining_net_qty = 0
                    
                if remaining_net_qty <= 0:
                    break
            
            if net_qty > 0 and open_qty == 0:
                if pending_trade:
                    pending_trade["status"] = "Closed"
                    pending_trade["reason"] = "Triggered (Netted)"
                    pending_trade["petal_entry_price"] = petal_exit
                    pending_trade["mini_entry_price"] = mini_exit
                    pending_trade["entry_spread"] = actual_exit_spread
                    pending_trade["expected_entry_spread"] = expected_entry_spread
                    if new_direction == "Expansion":
                        pending_trade["entry_slippage"] = actual_exit_spread - expected_entry_spread
                    else:
                        pending_trade["entry_slippage"] = expected_entry_spread - actual_exit_spread
                    pending_trade["petal_entry_type"] = petal_exit_type
                    pending_trade["mini_entry_type"] = mini_exit_type
                    system_state.log(f"MANUAL TRIGGER FILLED (NETTED IN-PLACE): ID {pending_trade['id']}, Dir {new_direction}, Qty {net_qty}, Spread {actual_exit_spread:.2f}")
            
            system_state.win_ratio = (system_state.winning_trades / system_state.total_trades) * 100.0 if system_state.total_trades > 0 else 0.0
            system_state.save_trade_history()
            system_state.save_manual_trades()
        else:
            net_success = False
            net_reason = result.get("reason", "Netting execution failed")
            system_state.log(f"[NETTING ERROR] Offset execution failed: {net_reason}")
            
    # 3. Process Remaining New Open Portion
    if net_success and open_qty > 0:
        system_state.log(f"[NETTING] Opening remaining quantity {open_qty} in direction {new_direction}...")
        result_open = await execute_trade(petal_action, mini_action, check_liquidity=True, is_entry=True, qty=open_qty)
        if result_open["success"]:
            petal_price = result_open["petal_fill_price"]
            mini_price = result_open["mini_fill_price"]
            entry_spread = (petal_price * 10.0) - mini_price
            
            if new_direction == "Expansion":
                entry_slippage = entry_spread - expected_entry_spread
            else:
                entry_slippage = expected_entry_spread - entry_spread
                
            if pending_trade:
                pending_trade["status"] = "Open"
                pending_trade["quantity"] = open_qty
                pending_trade["petal_entry_price"] = petal_price
                pending_trade["mini_entry_price"] = mini_price
                pending_trade["entry_spread"] = entry_spread
                pending_trade["expected_entry_spread"] = expected_entry_spread
                pending_trade["entry_slippage"] = entry_slippage
                pending_trade["petal_entry_type"] = result_open["petal_order_type"]
                pending_trade["mini_entry_type"] = result_open["mini_order_type"]
                system_state.log(f"MANUAL TRIGGER FILLED (IN-PLACE): ID {pending_trade['id']}, Dir {new_direction}, Qty {open_qty}, Spread {entry_spread:.2f}")
            else:
                trade_id = len(system_state.manual_trades) + 1
                new_trade = {
                    "id": trade_id,
                    "direction": new_direction,
                    "quantity": open_qty,
                    "trigger_diff": None,
                    "status": "Open",
                    "entry_time": time.strftime("%H:%M:%S"),
                    "entry_date": time.strftime("%Y-%m-%d"),
                    "petal_entry_price": petal_price,
                    "mini_entry_price": mini_price,
                    "entry_spread": entry_spread,
                    "expected_entry_spread": expected_entry_spread,
                    "entry_slippage": entry_slippage,
                    "petal_entry_type": result_open["petal_order_type"],
                    "mini_entry_type": result_open["mini_order_type"],
                    "petal_exit_price": 0.0,
                    "mini_exit_price": 0.0,
                    "exit_spread": 0.0,
                    "actual_exit_spread": 0.0,
                    "exit_slippage": 0.0,
                    "petal_exit_type": "--",
                    "mini_exit_type": "--",
                    "exit_time": "--",
                    "exit_date": "--",
                    "petal_pnl": 0.0,
                    "mini_pnl": 0.0,
                    "unrealized_pnl": 0.0,
                    "pnl": 0.0,
                    "charges": 0.0,
                    "reason": ""
                }
                system_state.manual_trades.append(new_trade)
                system_state.log(f"MANUAL ENTRY FILLED (REMAINDER): ID {trade_id}, Dir {new_direction}, Qty {open_qty}, Spread {entry_spread:.2f}")
            system_state.save_manual_trades()
        else:
            open_success = False
            open_reason = result_open.get("reason", "Remainder execution failed")
            system_state.log(f"[NETTING ERROR] Remainder execution failed: {open_reason}")
            
    if not net_success:
        return {"success": False, "reason": net_reason}
    if not open_success:
        return {"success": False, "reason": open_reason}
    return {"success": True}

async def trigger_manual_trade_execution(trade: dict):
    global system_state
    direction = trade["direction"]
    expected_entry_spread = system_state.depth_buy_spread if direction == "Expansion" else system_state.depth_sell_spread
    
    system_state.log(f"[MANUAL TRIGGER] Pending manual trade ID {trade['id']} triggered. Processing netting/entry...")
    
    result = await execute_netting_manual_trades(direction, trade["quantity"], expected_entry_spread, pending_trade=trade)
    if not result["success"]:
        trade["status"] = "Failed"
        trade["reason"] = result.get("reason", "Unknown execution error")
        system_state.log(f"[MANUAL TRIGGER ERROR] Pending trade ID {trade['id']} execution failed: {trade['reason']}")
        system_state.save_manual_trades()
        
    await broadcast_system_state()

# ----------------- Trading Engine and Live Tickers -----------------
async def process_market_data(data: dict):
    global system_state
    
    petal_ltp = data["petal_ltp"]
    mini_ltp = data["mini_ltp"]
    spread = (petal_ltp * 10.0) - mini_ltp
    
    system_state.gold_petal_ltp = petal_ltp
    system_state.gold_mini_ltp = mini_ltp
    system_state.spread = spread
    
    # Auto-record live price tick to CSV for historical backtesting
    try:
        csv_file = "historical_market_data.csv"
        file_exists = os.path.exists(csv_file)
        with open(csv_file, "a", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            if not file_exists:
                writer.writerow(["Timestamp", "GOLDPETAL_LTP", "GOLDMINI_LTP"])
            writer.writerow([get_ist_time_str("%Y-%m-%d %H:%M:%S"), petal_ltp, mini_ltp])
    except Exception as csv_err:
        logger.error(f"Failed to record live price tick to CSV: {csv_err}")
        
    qty = system_state.trade_quantity
    
    # Calculate depth-based spreads
    # depth_buy_spread (We Buy Petal, Sell Mini):
    # - Petal buy: we buy from Ask (sell side) for 100 * qty units.
    # - Mini sell: we sell to Bid (buy side) for qty units.
    avg_petal_buy = get_depth_average_price(system_state.petal_depth, "sell", 100 * qty, petal_ltp)
    avg_mini_sell = get_depth_average_price(system_state.mini_depth, "buy", qty, mini_ltp)
    system_state.depth_buy_spread = (avg_petal_buy * 10.0) - avg_mini_sell
    
    # depth_sell_spread (We Sell Petal, Buy Mini):
    # - Petal sell: we sell to Bid (buy side) for 100 * qty units.
    # - Mini buy: we buy from Ask (sell side) for qty units.
    avg_petal_sell = get_depth_average_price(system_state.petal_depth, "buy", 100 * qty, petal_ltp)
    avg_mini_buy = get_depth_average_price(system_state.mini_depth, "sell", qty, mini_ltp)
    system_state.depth_sell_spread = (avg_petal_sell * 10.0) - avg_mini_buy
    
    # Live leg and portfolio P&L calculations (Corrected to physical multipliers: 100x for Petal, 10x for Mini)
    if system_state.is_in_position:
        direction = system_state.position_direction
        if direction == "Expansion":  # Buy Petal, Sell Mini
            system_state.petal_pnl = (petal_ltp - system_state.petal_entry_price) * 100.0 * qty
            system_state.mini_pnl = (system_state.mini_entry_price - mini_ltp) * 10.0 * qty
        else:  # Sell Petal, Buy Mini
            system_state.petal_pnl = (system_state.petal_entry_price - petal_ltp) * 100.0 * qty
            system_state.mini_pnl = (mini_ltp - system_state.mini_entry_price) * 10.0 * qty
            
        system_state.unrealized_pnl = system_state.petal_pnl + system_state.mini_pnl
        system_state.used_margin = 50000.0 * qty
    else:
        system_state.petal_pnl = 0.0
        system_state.mini_pnl = 0.0
        system_state.unrealized_pnl = 0.0
        system_state.used_margin = 0.0
        
    # Calculate live manual trades P&Ls and margins
    manual_unrealized_pnl = 0.0
    manual_used_margin = 0.0
    for trade in system_state.manual_trades:
        if trade.get("status") == "Open":
            t_qty = trade.get("quantity", 1)
            t_dir = trade.get("direction")
            if t_dir == "Expansion":
                t_petal_pnl = (petal_ltp - trade.get("petal_entry_price", 0.0)) * 100.0 * t_qty
                t_mini_pnl = (trade.get("mini_entry_price", 0.0) - mini_ltp) * 10.0 * t_qty
            else:
                t_petal_pnl = (trade.get("petal_entry_price", 0.0) - petal_ltp) * 100.0 * t_qty
                t_mini_pnl = (mini_ltp - trade.get("mini_entry_price", 0.0)) * 10.0 * t_qty
            trade["petal_pnl"] = t_petal_pnl
            trade["mini_pnl"] = t_mini_pnl
            trade["unrealized_pnl"] = t_petal_pnl + t_mini_pnl
            manual_unrealized_pnl += trade["unrealized_pnl"]
            manual_used_margin += 50000.0 * t_qty
            
    system_state.used_margin += manual_used_margin
    system_state.total_pnl = system_state.realized_pnl + system_state.unrealized_pnl + manual_unrealized_pnl
    system_state.available_balance = system_state.total_capital - system_state.used_margin + system_state.total_pnl
    if system_state.total_capital > 0:
        system_state.returns_percentage = (system_state.total_pnl / system_state.total_capital) * 100.0
    else:
        system_state.returns_percentage = 0.0
        
    # Process pending manual trade triggers
    for trade in system_state.manual_trades:
        if trade.get("status") == "Pending":
            triggered = False
            if trade.get("direction") == "Expansion":
                if system_state.depth_buy_spread <= trade.get("trigger_diff", 0.0):
                    triggered = True
            elif trade.get("direction") == "Contraction":
                if system_state.depth_sell_spread >= trade.get("trigger_diff", 0.0):
                    triggered = True
            
            if triggered:
                trade["status"] = "Executing"
                asyncio.create_task(trigger_manual_trade_execution(trade))
        
    # Check execution lock or halted state: skip automations to prevent overlaps
    if system_state.execution_in_progress or system_state.system_status == "Halted":
        await broadcast_system_state()
        return
 
    # Check Automated Entry / Square-off Conditions
    if system_state.is_in_position:
        net_pnl = system_state.unrealized_pnl
        
        # 1. Target Trigger
        if system_state.auto_target_enabled and net_pnl >= system_state.auto_target_val:
            system_state.log(f"Auto Target Triggered: Net PnL {net_pnl:.2f} >= Target {system_state.auto_target_val:.2f}")
            expected_exit_spread = system_state.depth_sell_spread if system_state.position_direction == "Expansion" else system_state.depth_buy_spread
            asyncio.create_task(run_auto_exit("Auto-Target", expected_exit_spread))
            
        # 2. Stop Loss Trigger
        elif system_state.auto_sl_enabled and net_pnl <= system_state.auto_sl_val:
            system_state.log(f"Auto Stop Loss Triggered: Net PnL {net_pnl:.2f} <= Stop Loss {system_state.auto_sl_val:.2f}")
            expected_exit_spread = system_state.depth_sell_spread if system_state.position_direction == "Expansion" else system_state.depth_buy_spread
            asyncio.create_task(run_auto_exit("Auto-SL", expected_exit_spread))
            
        # 3. Market close time square-off
        elif system_state.auto_square_off_enabled:
            current_time = get_ist_time_str("%H:%M")
            if current_time >= system_state.auto_square_off_time:
                system_state.log(f"Market Auto Square-Off time reached ({current_time} >= {system_state.auto_square_off_time})")
                expected_exit_spread = system_state.depth_sell_spread if system_state.position_direction == "Expansion" else system_state.depth_buy_spread
                asyncio.create_task(run_auto_exit("Auto-Time-Close", expected_exit_spread))
                
        # 4. Spread-Based Auto Exit
        elif system_state.auto_spread_exit_enabled:
            direction = system_state.position_direction
            buffer = system_state.spread_buffer
            
            # Expansion Exit: we are in Buy Petal / Sell Mini. We exit when we sell Petal / buy Mini.
            # So the exit spread is depth_sell_spread. We want depth_sell_spread >= target_threshold - buffer
            if direction == "Expansion" and system_state.depth_sell_spread >= (system_state.target_threshold - buffer):
                system_state.log(f"Auto Spread Target Reached: Depth Sell Spread {system_state.depth_sell_spread:.2f} >= Target {system_state.target_threshold - buffer:.2f} (Target: {system_state.target_threshold:.2f}, Buffer: {buffer:.2f})")
                asyncio.create_task(run_auto_exit("Auto-Spread-Target", system_state.target_threshold))
                
            # Contraction Exit: we are in Sell Petal / Buy Mini. We exit when we buy Petal / sell Mini.
            # So the exit spread is depth_buy_spread. We want depth_buy_spread <= entry_threshold + buffer
            elif direction == "Contraction" and system_state.depth_buy_spread <= (system_state.entry_threshold + buffer):
                system_state.log(f"Auto Spread Target Reached: Depth Buy Spread {system_state.depth_buy_spread:.2f} <= Target {system_state.entry_threshold + buffer:.2f} (Target: {system_state.entry_threshold:.2f}, Buffer: {buffer:.2f})")
                asyncio.create_task(run_auto_exit("Auto-Spread-Target", system_state.entry_threshold))
    else:
        # Not in position: Check Auto Trading triggers
        if system_state.auto_trading_enabled:
            buffer = system_state.spread_buffer
            
            # Expansion Entry Condition: depth_buy_spread <= entry_threshold + buffer
            if system_state.depth_buy_spread <= (system_state.entry_threshold + buffer):
                asyncio.create_task(run_auto_entry("Expansion", "BUY", "SELL", system_state.entry_threshold))
                
            # Contraction Entry Condition: depth_sell_spread >= target_threshold - buffer (Only if Contraction is enabled)
            elif system_state.auto_contraction_enabled and (system_state.depth_sell_spread >= (system_state.target_threshold - buffer)):
                asyncio.create_task(run_auto_entry("Contraction", "SELL", "BUY", system_state.target_threshold))
 
    await broadcast_system_state()

def generate_simulated_depth(ltp: float) -> dict:
    buy_levels = []
    sell_levels = []
    for i in range(1, 6):
        buy_levels.append({
            "price": round(ltp - i * 1.5, 2),
            "quantity": random.randint(10, 250),
            "orders": random.randint(1, 8)
        })
        sell_levels.append({
            "price": round(ltp + i * 1.5, 2),
            "quantity": random.randint(10, 250),
            "orders": random.randint(1, 8)
        })
    return {"buy": buy_levels, "sell": sell_levels}

# ----------------- Dynamic Random Walk Ticker (Live Ticks) -----------------
async def live_mcx_ticker_task():
    # Fluctuate realistic prices resembling MCX commodity values
    petal_base = 7200.0
    mini_base = 71150.0
    
    while True:
        try:
            # 1. Try to fetch from Active Broker API
            if system_state.broker == "AngelOne":
                if not system_state.smart_connect:
                    system_state.api_connected = False
                else:
                    try:
                        loop = asyncio.get_running_loop()
                        market_quotes = await loop.run_in_executor(
                            None,
                            lambda: system_state.smart_connect.getMarketData(
                                mode="FULL",
                                exchangeTokens={"MCX": [system_state.petal_token, system_state.mini_token]}
                            )
                        )
                        
                        if market_quotes:
                            msg = market_quotes.get("message", "")
                            err_code = market_quotes.get("errorCode", "")
                            if msg in ["Token missing", "Invalid Token"] or err_code in ["AG8001", "AG8003"]:
                                system_state.log(f"[ANGELONE API] Token invalid/missing (Code: {err_code}, Msg: {msg}). Triggering daily session re-authentication...")
                                system_state.init_angelone_client()
                                system_state.api_connected = False
                                await asyncio.sleep(2.0)
                                continue
 
                        petal_quote = {}
                        mini_quote = {}
                        if market_quotes and market_quotes.get("status") == True:
                            fetched_list = market_quotes.get("data", {}).get("fetched", [])
                            if isinstance(fetched_list, list):
                                for item in fetched_list:
                                    if isinstance(item, dict):
                                        if item.get("symbolToken") == system_state.petal_token:
                                            petal_quote = item
                                        elif item.get("symbolToken") == system_state.mini_token:
                                            mini_quote = item
 
                        petal_ltp = float(petal_quote.get("ltp", 0.0))
                        mini_ltp = float(mini_quote.get("ltp", 0.0))
                        
                        if petal_ltp > 0 and mini_ltp > 0:
                            system_state.api_connected = True
                            system_state.gold_petal_volume = int(petal_quote.get("volume", 0))
                            system_state.gold_petal_buy_qty = int(petal_quote.get("totalBuyQty", 0))
                            system_state.gold_petal_sell_qty = int(petal_quote.get("totalSellQty", 0))
                            
                            system_state.gold_mini_volume = int(mini_quote.get("volume", 0))
                            system_state.gold_mini_buy_qty = int(mini_quote.get("totalBuyQty", 0))
                            system_state.gold_mini_sell_qty = int(mini_quote.get("totalSellQty", 0))
 
                            # Extract or simulate depth
                            petal_depth_raw = petal_quote.get("depth", {})
                            if petal_depth_raw and petal_depth_raw.get("buy") and petal_depth_raw.get("sell"):
                                system_state.petal_depth = petal_depth_raw
                            else:
                                system_state.petal_depth = generate_simulated_depth(petal_ltp)
                                
                            mini_depth_raw = mini_quote.get("depth", {})
                            if mini_depth_raw and mini_depth_raw.get("buy") and mini_depth_raw.get("sell"):
                                system_state.mini_depth = mini_depth_raw
                            else:
                                system_state.mini_depth = generate_simulated_depth(mini_ltp)
 
                            await process_market_data({
                                "petal_ltp": petal_ltp,
                                "mini_ltp": mini_ltp
                            })
                            await asyncio.sleep(1.0)
                            continue
                        else:
                            system_state.api_connected = False
                    except Exception as e:
                        system_state.api_connected = False
                        system_state.log(f"[ANGELONE API] Live ticker query failed: {e}. API server not connected.")
 
                if not system_state.api_connected:
                    # Halt simulation and wait (no fake values!)
                    await broadcast_system_state()
                    await asyncio.sleep(2.0)
                    continue
 
            # 2. Simulator Fallback / Simulation Mode Ticks
            petal_step = random.uniform(-6.0, 6.0)
            mini_step = random.uniform(-6.0, 6.0)
            petal_base = max(7000.0, min(7500.0, petal_base + petal_step))
            mini_base = max(69000.0, min(74000.0, mini_base + mini_step))
            
            # Simulate volumes & depth dynamically
            if system_state.gold_petal_volume == 0:
                system_state.gold_petal_volume = 85000
                system_state.gold_mini_volume = 32000
            
            system_state.gold_petal_volume += random.randint(1, 10)
            system_state.gold_petal_buy_qty = random.randint(18000, 24000)
            system_state.gold_petal_sell_qty = random.randint(18000, 24000)
            
            system_state.gold_mini_volume += random.randint(1, 5)
            system_state.gold_mini_buy_qty = random.randint(6000, 9500)
            system_state.gold_mini_sell_qty = random.randint(6000, 9500)

            # Generate simulated depth
            system_state.petal_depth = generate_simulated_depth(round(petal_base, 2))
            system_state.mini_depth = generate_simulated_depth(round(mini_base, 2))

            await process_market_data({
                "petal_ltp": round(petal_base, 2),
                "mini_ltp": round(mini_base, 2)
            })
        except Exception as e:
            logger.error(f"Error in Live Ticker thread: {e}")
            
        await asyncio.sleep(1.0)  # Live MCX ticks every 1 second

# Scrip Finder helper
async def search_active_mcx_tokens():
    import urllib.request
    import json
    url = "https://margincalculator.angelone.in/OpenAPI_File/files/OpenAPIScripMaster.json"
    system_state.log("[SCRIP FINDER] Fetching active MCX contracts from Angel One Scrip Master...")
    try:
        loop = asyncio.get_running_loop()
        def download_and_parse():
            req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
            with urllib.request.urlopen(req, timeout=10) as response:
                data = json.loads(response.read().decode('utf-8'))
            results = []
            for item in data:
                exch = item.get("exch_seg", "")
                symbol = item.get("symbol", "")
                token = item.get("token", "")
                if exch == "MCX" and symbol and token:
                    # Cache all MCX tokens
                    system_state.mcx_tokens_cache[symbol] = token
                    if symbol.startswith("GOLDPETAL") or symbol.startswith("GOLDM"):
                        results.append({
                            "symbol": symbol,
                            "token": token,
                            "expiry": item.get("expiry"),
                            "name": item.get("name")
                        })
            return results

        mcx_symbols = await loop.run_in_executor(None, download_and_parse)
        system_state.log(f"[SCRIP FINDER] Cached MCX tokens. Found {len(mcx_symbols)} active Gold contracts:")
        mcx_symbols.sort(key=lambda x: x["symbol"])
        for res in mcx_symbols[:30]:
            system_state.log(f"-> Symbol: {res['symbol']} | Token: {res['token']} | Expiry: {res['expiry']}")
            
        if system_state.broker == "AngelOne":
            system_state.init_angelone_client()
    except Exception as e:
        system_state.log(f"[SCRIP FINDER] Error searching scrip master: {e}")

# Start background task on startup
@app.on_event("startup")
async def startup_event():
    import sys
    system_state.log(f"DEBUG info: Python Executable: {sys.executable}, Version: {sys.version}")
    try:
        from SmartApi import SmartConnect
        system_state.log("DEBUG info: SmartConnect import succeeded!")
    except Exception as e:
        import traceback
        system_state.log(f"DEBUG info: SmartConnect import failed: {e}")
        for line in traceback.format_exc().split("\n"):
            if line.strip():
                system_state.log(f"DEBUG TRACE: {line.strip()}")
    system_state.init_angelone_client()
    asyncio.create_task(search_active_mcx_tokens())
    asyncio.create_task(live_mcx_ticker_task())
    system_state.log("Live MCX market tick simulator running.")

# ----------------- REST and WebSocket Endpoints -----------------

# WebSocket endpoint (displays all fields live)
@app.websocket("/ws/live-data")
async def live_data_endpoint(websocket: WebSocket):
    token = websocket.query_params.get("token")
    auth_token = os.getenv("AUTH_TOKEN", "secret_arbitrage_token_2026")
    if token != auth_token:
        await websocket.close(code=3000)
        return
        
    await websocket.accept()
    await manager.connect(websocket)
    
    try:
        # Push initial data payload
        await websocket.send_json({
            "gold_petal_ltp": round(system_state.gold_petal_ltp, 2),
            "gold_mini_ltp": round(system_state.gold_mini_ltp, 2),
            "spread": round(system_state.spread, 2),
            "depth_buy_spread": round(system_state.depth_buy_spread, 2),
            "depth_sell_spread": round(system_state.depth_sell_spread, 2),
            
            "is_in_position": system_state.is_in_position,
            "position_direction": system_state.position_direction,
            "system_status": system_state.system_status,
            
            "petal_entry_price": round(system_state.petal_entry_price, 2),
            "mini_entry_price": round(system_state.mini_entry_price, 2),
            "entry_spread": round(system_state.entry_spread, 2),
            
            "petal_pnl": round(system_state.petal_pnl, 2),
            "mini_pnl": round(system_state.mini_pnl, 2),
            "unrealized_pnl": round(system_state.unrealized_pnl, 2),
            "realized_pnl": round(system_state.realized_pnl, 2),
            "total_pnl": round(system_state.total_pnl, 2),
            
            "total_capital": round(system_state.total_capital, 2),
            "used_margin": round(system_state.used_margin, 2),
            "available_balance": round(system_state.available_balance, 2),
            "returns_percentage": round(system_state.returns_percentage, 2),
            
            "entry_threshold": system_state.entry_threshold,
            "target_threshold": system_state.target_threshold,
            "sl_threshold": system_state.sl_threshold,
            
            "api_connected": system_state.api_connected,
            "petal_depth": system_state.petal_depth,
            "mini_depth": system_state.mini_depth,
            
            "auto_target_enabled": system_state.auto_target_enabled,
            "auto_target_val": system_state.auto_target_val,
            "auto_sl_enabled": system_state.auto_sl_enabled,
            "auto_sl_val": system_state.auto_sl_val,
            "auto_square_off_enabled": system_state.auto_square_off_enabled,
            "auto_square_off_time": system_state.auto_square_off_time,
            "spread_buffer": system_state.spread_buffer,
            "auto_contraction_enabled": system_state.auto_contraction_enabled,
            "auto_spread_exit_enabled": system_state.auto_spread_exit_enabled,
            "paper_trading_mode": system_state.paper_trading_mode,
            "auto_trading_enabled": system_state.auto_trading_enabled,
            "trade_quantity": system_state.trade_quantity,
            "broker": system_state.broker,
            "api_key": system_state.api_key,
            "client_id": system_state.client_id,
            "password": system_state.password,
            "totp_secret": system_state.totp_secret,
            "petal_symbol": system_state.petal_symbol,
            "petal_token": system_state.petal_token,
            "mini_symbol": system_state.mini_symbol,
            "mini_token": system_state.mini_token,
            "gold_petal_volume": system_state.gold_petal_volume,
            "gold_petal_buy_qty": system_state.gold_petal_buy_qty,
            "gold_petal_sell_qty": system_state.gold_petal_sell_qty,
            "gold_mini_volume": system_state.gold_mini_volume,
            "gold_mini_buy_qty": system_state.gold_mini_buy_qty,
            "gold_mini_sell_qty": system_state.gold_mini_sell_qty,

            
            "win_ratio": round(system_state.win_ratio, 2),
            "total_trades": system_state.total_trades,
            "trade_history": system_state.trade_history,
            
            "logs": system_state.logs
        })
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        manager.disconnect(websocket)

# REST manual position entry endpoint
class EntryPayload(BaseModel):
    direction: str
    trigger_diff: float = None
    quantity: int = None

@app.post("/api/entry")
async def api_entry(payload: EntryPayload, token: str = None, authorization: str = Header(None)):
    verify_token(token, authorization)
    
    if system_state.system_status == "Halted":
        raise HTTPException(status_code=400, detail="Terminal is Halted due to Kill Switch or Stop Loss.")
        
    if payload.direction not in ["Expansion", "Contraction"]:
        raise HTTPException(status_code=400, detail="Invalid trade direction selected.")
        
    qty = payload.quantity if payload.quantity is not None else system_state.trade_quantity
    
    if payload.trigger_diff is not None:
        # Create pending manual trade
        trade_id = len(system_state.manual_trades) + 1
        new_trade = {
            "id": trade_id,
            "direction": payload.direction,
            "quantity": qty,
            "trigger_diff": payload.trigger_diff,
            "status": "Pending",
            "entry_time": time.strftime("%H:%M:%S"),
            "entry_date": time.strftime("%Y-%m-%d"),
            "petal_entry_price": 0.0,
            "mini_entry_price": 0.0,
            "entry_spread": 0.0,
            "expected_entry_spread": 0.0,
            "entry_slippage": 0.0,
            "petal_entry_type": "--",
            "mini_entry_type": "--",
            "petal_exit_price": 0.0,
            "mini_exit_price": 0.0,
            "exit_spread": 0.0,
            "actual_exit_spread": 0.0,
            "exit_slippage": 0.0,
            "petal_exit_type": "--",
            "mini_exit_type": "--",
            "exit_time": "--",
            "exit_date": "--",
            "petal_pnl": 0.0,
            "mini_pnl": 0.0,
            "unrealized_pnl": 0.0,
            "pnl": 0.0,
            "charges": 0.0,
            "reason": ""
        }
        system_state.manual_trades.append(new_trade)
        system_state.save_manual_trades()
        system_state.log(f"MANUAL PENDING ENTRY CREATED: ID {trade_id}, Dir {payload.direction}, Trigger Diff {payload.trigger_diff}, Qty {qty}")
        await broadcast_system_state()
        return {"status": "SUCCESS", "message": f"Pending manual trade ID {trade_id} created."}
        
    # Immediate execution
    expected_entry_spread = system_state.depth_buy_spread if payload.direction == "Expansion" else system_state.depth_sell_spread
    
    result = await execute_netting_manual_trades(payload.direction, qty, expected_entry_spread)
    if not result["success"]:
        raise HTTPException(status_code=400, detail=f"Trade execution failed: {result['reason']}")
        
    await broadcast_system_state()
    return {"status": "SUCCESS", "message": "Manual trade processed successfully."}
 
# REST manual position exit square-off endpoint
@app.post("/api/exit")
async def api_exit(token: str = None, authorization: str = Header(None)):
    verify_token(token, authorization)
    
    if not system_state.is_in_position:
        raise HTTPException(status_code=400, detail="No active position to exit.")
        
    if system_state.position_direction == "Expansion":
        system_state.expected_exit_spread = system_state.depth_sell_spread
    else:
        system_state.expected_exit_spread = system_state.depth_buy_spread
        
    await execute_position_exit(exit_reason="Manual-Exit")
    return {"status": "SUCCESS", "message": "Position closed successfully."}

class ExitManualPayload(BaseModel):
    trade_id: int

@app.post("/api/exit-manual")
async def api_exit_manual(payload: ExitManualPayload, token: str = None, authorization: str = Header(None)):
    verify_token(token, authorization)
    
    trade = None
    for t in system_state.manual_trades:
        if t["id"] == payload.trade_id:
            trade = t
            break
            
    if not trade:
        raise HTTPException(status_code=404, detail=f"Manual trade ID {payload.trade_id} not found.")
        
    if trade["status"] == "Pending":
        trade["status"] = "Cancelled"
        system_state.log(f"MANUAL PENDING ENTRY ID {trade['id']} CANCELLED.")
        system_state.save_manual_trades()
        await broadcast_system_state()
        return {"status": "SUCCESS", "message": f"Pending trade ID {trade['id']} cancelled."}
        
    if trade["status"] != "Open":
        raise HTTPException(status_code=400, detail=f"Trade is not active (Status: {trade['status']}).")
        
    direction = trade["direction"]
    petal_action = "SELL" if direction == "Expansion" else "BUY"
    mini_action = "BUY" if direction == "Expansion" else "SELL"
    
    system_state.log(f"[MANUAL EXIT] Squaring off manual trade ID {trade['id']} ({direction})...")
    
    result = await execute_trade(petal_action, mini_action, check_liquidity=False, is_entry=False, qty=trade["quantity"])
    if not result["success"]:
        raise HTTPException(status_code=400, detail=f"Square off trade execution failed: {result['reason']}")
        
    petal_exit = result["petal_fill_price"]
    mini_exit = result["mini_fill_price"]
    petal_exit_type = result["petal_order_type"]
    mini_exit_type = result["mini_order_type"]
    
    actual_exit_spread = (petal_exit * 10.0) - mini_exit
    expected_exit_spread = system_state.depth_sell_spread if direction == "Expansion" else system_state.depth_buy_spread
    
    qty = trade["quantity"]
    if direction == "Expansion":
        p_pnl = (petal_exit - trade["petal_entry_price"]) * 100.0 * qty
        m_pnl = (trade["mini_entry_price"] - mini_exit) * 10.0 * qty
        exit_slippage = expected_exit_spread - actual_exit_spread
    else:
        p_pnl = (trade["petal_entry_price"] - petal_exit) * 100.0 * qty
        m_pnl = (mini_exit - trade["mini_entry_price"]) * 10.0 * qty
        exit_slippage = actual_exit_spread - expected_exit_spread
        
    trade_pnl = p_pnl + m_pnl
    charges = system_state.calculate_mcx_charges(
        direction, qty, trade["petal_entry_price"], trade["mini_entry_price"], petal_exit, mini_exit
    )
    net_pnl = trade_pnl - charges
    
    trade["status"] = "Closed"
    trade["petal_exit_price"] = petal_exit
    trade["mini_exit_price"] = mini_exit
    trade["petal_exit_type"] = petal_exit_type
    trade["mini_exit_type"] = mini_exit_type
    trade["exit_spread"] = expected_exit_spread
    trade["actual_exit_spread"] = actual_exit_spread
    trade["exit_slippage"] = exit_slippage
    trade["petal_pnl"] = p_pnl
    trade["mini_pnl"] = m_pnl
    trade["pnl"] = net_pnl
    trade["charges"] = charges
    trade["exit_time"] = time.strftime("%H:%M:%S")
    trade["exit_date"] = time.strftime("%Y-%m-%d")
    
    system_state.realized_pnl += net_pnl
    system_state.total_trades += 1
    if net_pnl > 0:
        system_state.winning_trades += 1
    system_state.win_ratio = (system_state.winning_trades / system_state.total_trades) * 100.0
    
    history_record = {
        "id": len(system_state.trade_history) + 1,
        "date": trade["entry_date"] if trade.get("entry_date") else trade["exit_date"],
        "direction": direction,
        "status": "COMPLETED",
        "entry_time": trade["entry_time"],
        "exit_time": trade["exit_time"],
        "petal_action": "BUY" if direction == "Expansion" else "SELL",
        "mini_action": "SELL" if direction == "Expansion" else "BUY",
        "petal_entry": round(trade["petal_entry_price"], 2),
        "mini_entry": round(trade["mini_entry_price"], 2),
        "petal_exit": round(petal_exit, 2),
        "mini_exit": round(mini_exit, 2),
        "entry_spread": round(trade["expected_entry_spread"], 2),
        "actual_entry_spread": round(trade["entry_spread"], 2),
        "entry_slippage": round(trade["entry_slippage"], 2),
        "exit_spread": round(expected_exit_spread, 2),
        "actual_exit_spread": round(actual_exit_spread, 2),
        "exit_slippage": round(exit_slippage, 2),
        "petal_entry_type": trade["petal_entry_type"],
        "mini_entry_type": trade["mini_entry_type"] if "mini_entry_type" in trade else "--",
        "petal_exit_type": petal_exit_type,
        "mini_exit_type": mini_exit_type,
        "petal_pnl": round(p_pnl, 2),
        "mini_pnl": round(m_pnl, 2),
        "gross_pnl": round(trade_pnl, 2),
        "charges": round(charges, 2),
        "pnl": round(net_pnl, 2),
        "reason": "Manual-Exit",
        "details": f"Manual Trade {trade['id']} closed. Net P&L: {net_pnl:.2f}."
    }
    system_state.trade_history.append(history_record)
    system_state.save_trade_history()
    system_state.save_manual_trades()
    
    system_state.log(f"MANUAL POSITION SQUARED OFF: ID {trade['id']}, Net PnL: INR {net_pnl:+.2f}")
    await broadcast_system_state()
    return {"status": "SUCCESS", "message": "Position closed successfully."}

class DismissManualPayload(BaseModel):
    trade_id: int

@app.post("/api/dismiss-manual")
async def api_dismiss_manual(payload: DismissManualPayload, token: str = None, authorization: str = Header(None)):
    verify_token(token, authorization)
    system_state.manual_trades = [t for t in system_state.manual_trades if t["id"] != payload.trade_id]
    system_state.save_manual_trades()
    await broadcast_system_state()
    return {"status": "SUCCESS", "message": f"Manual trade ID {payload.trade_id} dismissed."}

# REST Emergency Kill Switch endpoint
@app.post("/api/kill-switch")
async def api_kill_switch(token: str = None, authorization: str = Header(None)):
    verify_token(token, authorization)
    
    system_state.log("EMERGENCY: Kill Switch activated! Closing positions immediately.")
    
    # 1. Close auto position
    if system_state.is_in_position:
        if system_state.position_direction == "Expansion":
            system_state.expected_exit_spread = system_state.depth_sell_spread
        else:
            system_state.expected_exit_spread = system_state.depth_buy_spread
        await execute_position_exit(exit_reason="KILL-SWITCH")
        
    # 2. Cancel and square off manual trades
    for trade in list(system_state.manual_trades):
        if trade.get("status") == "Pending":
            trade["status"] = "Cancelled"
            system_state.log(f"MANUAL PENDING ENTRY ID {trade['id']} CANCELLED due to Kill Switch.")
        elif trade.get("status") == "Open":
            direction = trade["direction"]
            petal_action = "SELL" if direction == "Expansion" else "BUY"
            mini_action = "BUY" if direction == "Expansion" else "SELL"
            qty = trade["quantity"]
            
            result = await execute_trade(petal_action, mini_action, check_liquidity=False, is_entry=False, qty=qty)
            if result["success"]:
                petal_exit = result["petal_fill_price"]
                mini_exit = result["mini_fill_price"]
                actual_exit_spread = (petal_exit * 10.0) - mini_exit
                expected_exit_spread = system_state.depth_sell_spread if direction == "Expansion" else system_state.depth_buy_spread
                
                if direction == "Expansion":
                    p_pnl = (petal_exit - trade["petal_entry_price"]) * 100.0 * qty
                    m_pnl = (trade["mini_entry_price"] - mini_exit) * 10.0 * qty
                    exit_slippage = expected_exit_spread - actual_exit_spread
                else:
                    p_pnl = (trade["petal_entry_price"] - petal_exit) * 100.0 * qty
                    m_pnl = (mini_exit - trade["mini_entry_price"]) * 10.0 * qty
                    exit_slippage = actual_exit_spread - expected_exit_spread
                    
                trade_pnl = p_pnl + m_pnl
                charges = system_state.calculate_mcx_charges(
                    direction, qty, trade["petal_entry_price"], trade["mini_entry_price"], petal_exit, mini_exit
                )
                net_pnl = trade_pnl - charges
                
                trade["status"] = "Closed"
                trade["petal_exit_price"] = petal_exit
                trade["mini_exit_price"] = mini_exit
                trade["pnl"] = net_pnl
                trade["charges"] = charges
                trade["exit_time"] = time.strftime("%H:%M:%S")
                trade["exit_date"] = time.strftime("%Y-%m-%d")
                
                system_state.realized_pnl += net_pnl
                system_state.total_trades += 1
                if net_pnl > 0:
                    system_state.winning_trades += 1
                system_state.win_ratio = (system_state.winning_trades / system_state.total_trades) * 100.0
                
                history_record = {
                    "id": len(system_state.trade_history) + 1,
                    "date": trade["entry_date"] if trade.get("entry_date") else trade["exit_date"],
                    "direction": direction,
                    "status": "COMPLETED",
                    "entry_time": trade["entry_time"],
                    "exit_time": trade["exit_time"],
                    "petal_action": "BUY" if direction == "Expansion" else "SELL",
                    "mini_action": "SELL" if direction == "Expansion" else "BUY",
                    "petal_entry": round(trade["petal_entry_price"], 2),
                    "mini_entry": round(trade["mini_entry_price"], 2),
                    "petal_exit": round(petal_exit, 2),
                    "mini_exit": round(mini_exit, 2),
                    "entry_spread": round(trade["expected_entry_spread"], 2),
                    "actual_entry_spread": round(trade["entry_spread"], 2),
                    "entry_slippage": round(trade["entry_slippage"], 2),
                    "exit_spread": round(expected_exit_spread, 2),
                    "actual_exit_spread": round(actual_exit_spread, 2),
                    "exit_slippage": round(exit_slippage, 2),
                    "petal_pnl": round(p_pnl, 2),
                    "mini_pnl": round(m_pnl, 2),
                    "gross_pnl": round(trade_pnl, 2),
                    "charges": round(charges, 2),
                    "pnl": round(net_pnl, 2),
                    "reason": "KILL-SWITCH",
                    "details": f"Manual Trade {trade['id']} closed due to Kill Switch. Net: {net_pnl:.2f}."
                }
                system_state.trade_history.append(history_record)
            else:
                trade["status"] = "Failed"
                system_state.log(f"MANUAL POSITION ID {trade['id']} EXIT FAILED on Kill Switch: {result.get('reason')}")
                
    system_state.save_manual_trades()
    system_state.save_trade_history()
    system_state.system_status = "Halted"
    await broadcast_system_state()
    return {"status": "SUCCESS", "message": "Positions cleared and system halted."}

# REST Strategy parameters update form submission endpoint
class UpdateParamsPayload(BaseModel):
    entry_threshold: float
    target_threshold: float
    stop_loss_threshold: float
    total_capital: float
    paper_trading_mode: bool
    trade_quantity: int
    auto_target_enabled: bool
    auto_target_val: float
    auto_sl_enabled: bool
    auto_sl_val: float
    auto_square_off_enabled: bool
    auto_square_off_time: str
    auto_trading_enabled: bool
    spread_buffer: float
    auto_contraction_enabled: bool
    auto_spread_exit_enabled: bool
    broker: str
    api_key: str
    client_id: str
    password: str
    totp_secret: str
    petal_symbol: str
    petal_token: str
    mini_symbol: str
    mini_token: str

@app.post("/api/update-rules")
async def api_update_rules(payload: UpdateParamsPayload, token: str = None, authorization: str = Header(None)):
    verify_token(token, authorization)
    
    system_state.entry_threshold = payload.entry_threshold
    system_state.target_threshold = payload.target_threshold
    system_state.sl_threshold = payload.stop_loss_threshold
    system_state.total_capital = payload.total_capital
    system_state.trade_quantity = max(1, payload.trade_quantity)
    
    system_state.spread_buffer = payload.spread_buffer
    system_state.auto_contraction_enabled = payload.auto_contraction_enabled
    system_state.auto_spread_exit_enabled = payload.auto_spread_exit_enabled
    
    system_state.paper_trading_mode = True  # Forced to True for virtual trading safety
    system_state.auto_trading_enabled = payload.auto_trading_enabled
    system_state.auto_target_enabled = payload.auto_target_enabled
    system_state.auto_target_val = payload.auto_target_val
    system_state.auto_sl_enabled = payload.auto_sl_enabled
    system_state.auto_sl_val = payload.auto_sl_val
    
    system_state.auto_square_off_enabled = payload.auto_square_off_enabled
    system_state.auto_square_off_time = payload.auto_square_off_time
    
    # Save broker fields
    system_state.broker = payload.broker
    system_state.api_key = payload.api_key
    system_state.client_id = payload.client_id
    system_state.password = payload.password
    system_state.totp_secret = payload.totp_secret
    # Reset tokens for re-resolution if the symbol has changed on the UI
    if system_state.petal_symbol != payload.petal_symbol:
        system_state.petal_symbol = payload.petal_symbol
        system_state.petal_token = "250000"
    else:
        system_state.petal_token = payload.petal_token

    if system_state.mini_symbol != payload.mini_symbol:
        system_state.mini_symbol = payload.mini_symbol
        system_state.mini_token = "250001"
    else:
        system_state.mini_token = payload.mini_token
    
    # Trigger dynamic SDK connection if client updates keys
    if system_state.broker == "AngelOne":
        system_state.init_angelone_client()
    
    # Reactivate from Halted status if rules are saved
    if system_state.system_status == "Halted":
        system_state.system_status = "Active"
        system_state.log("Parameters saved. System reactivated and reset to Active.")
    else:
        system_state.log("Parameters updated successfully.")
        
    await broadcast_system_state()
    return {"status": "SUCCESS", "message": "Parameters updated successfully."}

# REST download hardcoded rules strategy_rules.json
@app.get("/api/download-logic")
async def download_logic(token: str = None, authorization: str = Header(None)):
    verify_token(token, authorization)
    
    rules = {
        "strategy_name": "Spread Arbitrage",
        "symbols": {"leg1": "GOLD_PETAL", "leg2": "GOLD_MINI"},
        "multipliers": {"GOLD_PETAL": 100, "GOLD_MINI": 10},
        "parameters": {
            "entry_threshold": system_state.entry_threshold,
            "target_threshold": system_state.target_threshold,
            "stop_loss_threshold": system_state.sl_threshold,
            "auto_target_enabled": system_state.auto_target_enabled,
            "auto_target_val": system_state.auto_target_val,
            "auto_sl_enabled": system_state.auto_sl_enabled,
            "auto_sl_val": system_state.auto_sl_val
        },
        "safety": {
            "paper_trading_mode": system_state.paper_trading_mode,
            "kill_switch_active": system_state.system_status == "Halted"
        }
    }
    headers = {"Content-Disposition": "attachment; filename=strategy_rules.json"}
    return JSONResponse(content=rules, headers=headers)

# REST CSV Trade History exporter endpoint
@app.get("/api/export-csv")
async def api_export_csv(token: str = None, authorization: str = Header(None)):
    verify_token(token, authorization)
    
    csv_buffer = StringIO()
    writer = csv.writer(csv_buffer)
    
    # Headers
    writer.writerow([
        "Trade ID", "Date", "Direction", "Status", "Entry Time", "Exit Time", 
        "Expected Entry Spread", "Actual Entry Spread", "Entry Slippage", 
        "Expected Exit Spread", "Actual Exit Spread", "Exit Slippage", 
        "Petal Entry Price", "Mini Entry Price", "Petal Exit Price", "Mini Exit Price", 
        "Petal Entry Type", "Mini Entry Type", "Petal Exit Type", "Mini Exit Type", 
        "Petal PnL", "Mini PnL", "Gross PnL", "Brokerage & Charges", "Net PnL", "Trigger Reason", "Details"
    ])
    
    # Records
    for trade in system_state.trade_history:
        writer.writerow([
            trade.get("id"),
            trade.get("date"),
            trade.get("direction"),
            trade.get("status", "COMPLETED"),
            trade.get("entry_time"),
            trade.get("exit_time"),
            trade.get("entry_spread"),
            trade.get("actual_entry_spread"),
            trade.get("entry_slippage"),
            trade.get("exit_spread"),
            trade.get("actual_exit_spread"),
            trade.get("exit_slippage"),
            trade.get("petal_entry"),
            trade.get("mini_entry"),
            trade.get("petal_exit"),
            trade.get("mini_exit"),
            trade.get("petal_entry_type"),
            trade.get("mini_entry_type"),
            trade.get("petal_exit_type"),
            trade.get("mini_exit_type"),
            trade.get("petal_pnl"),
            trade.get("mini_pnl"),
            trade.get("gross_pnl", trade.get("pnl", 0.0)),
            trade.get("charges", 0.0),
            trade.get("pnl"),
            trade.get("reason"),
            trade.get("details")
        ])
        
    csv_buffer.seek(0)
    headers = {"Content-Disposition": "attachment; filename=trade_history.csv"}
    return StreamingResponse(iter([csv_buffer.getvalue()]), media_type="text/csv", headers=headers)

# REST CSV Active & Pending Manual Trades exporter endpoint
@app.get("/api/export-manual-csv")
async def api_export_manual_csv(token: str = None, authorization: str = Header(None)):
    verify_token(token, authorization)
    
    csv_buffer = StringIO()
    writer = csv.writer(csv_buffer)
    
    # Headers
    writer.writerow([
        "Manual Trade ID", "Entry Date", "Direction", "Status", "Quantity", "Trigger Diff Target", 
        "Entry Time", "Actual Entry Spread", "Expected Entry Spread", "Entry Slippage", 
        "Petal Entry Price", "Mini Entry Price", "Petal Entry Type", "Mini Entry Type", 
        "Exit Time", "Exit Date", "Actual Exit Spread", "Expected Exit Spread", "Exit Slippage",
        "Petal Exit Price", "Mini Exit Price", "Petal Exit Type", "Mini Exit Type", 
        "Petal PnL", "Mini PnL", "Unrealized PnL", "Realized PnL (Closed)", "Brokerage & Charges", "Trigger Reason"
    ])
    
    # Records
    for trade in system_state.manual_trades:
        writer.writerow([
            trade.get("id"),
            trade.get("entry_date"),
            trade.get("direction"),
            trade.get("status"),
            trade.get("quantity"),
            trade.get("trigger_diff") if trade.get("trigger_diff") is not None else "Immediate",
            trade.get("entry_time"),
            trade.get("entry_spread"),
            trade.get("expected_entry_spread"),
            trade.get("entry_slippage"),
            trade.get("petal_entry_price"),
            trade.get("mini_entry_price"),
            trade.get("petal_entry_type"),
            trade.get("mini_entry_type"),
            trade.get("exit_time"),
            trade.get("exit_date"),
            trade.get("actual_exit_spread"),
            trade.get("exit_spread"),
            trade.get("exit_slippage"),
            trade.get("petal_exit_price"),
            trade.get("mini_exit_price"),
            trade.get("petal_exit_type"),
            trade.get("mini_exit_type"),
            trade.get("petal_pnl"),
            trade.get("mini_pnl"),
            trade.get("unrealized_pnl"),
            trade.get("pnl"),
            trade.get("charges"),
            trade.get("reason")
        ])
        
    csv_buffer.seek(0)
    headers = {"Content-Disposition": "attachment; filename=manual_trades.csv"}
    return StreamingResponse(iter([csv_buffer.getvalue()]), media_type="text/csv", headers=headers)

# Serving static dashboard files
@app.get("/", response_class=HTMLResponse)
async def get_dashboard():
    try:
        with open("index.html", "r", encoding="utf-8") as f:
            return HTMLResponse(content=f.read())
    except FileNotFoundError:
        return HTMLResponse(content="<h3>index.html not found</h3>", status_code=404)

@app.get("/style.css")
async def get_style():
    return FileResponse("style.css", media_type="text/css")

@app.get("/script.js")
async def get_script():
    return FileResponse("script.js", media_type="application/javascript")

if __name__ == "__main__":
    host = os.getenv("HOST", "127.0.0.1")
    port = int(os.getenv("PORT", 7890))
    logger.info(f"Starting server on http://{host}:{port}")
    uvicorn.run("main:app", host=host, port=port, log_level="info")
