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
from typing import List, Dict, Optional

import uvicorn
from dotenv import load_dotenv
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Header, HTTPException, Request, Cookie
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import JSONResponse, HTMLResponse, FileResponse, StreamingResponse
from pydantic import BaseModel
import websockets
import hmac
import hashlib
import base64
import contextvars
import pyotp
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

# Setup IST Timezone (+5:30)
IST = timezone(timedelta(hours=5, minutes=30))

def get_ist_time() -> datetime:
    return datetime.now(IST)

def get_ist_time_str(fmt: str = "%Y-%m-%d %H:%M:%S") -> str:
    return get_ist_time().strftime(fmt)

def get_market_session_status() -> str:
    """
    Returns:
      "HOLD" - if morning hold time (09:00:00 to 09:02:59 IST)
      "SUSPENDED" - if evening/night suspension time (23:25:00 to 08:59:59 IST)
      "OPEN" - otherwise (trading allowed)
    """
    now = get_ist_time()
    h = now.hour
    m = now.minute
    
    # Morning hold: 9:00 to 9:02:59 (inclusive of 9:00, 9:01, 9:02)
    if h == 9 and 0 <= m < 3:
        return "HOLD"
        
    # Evening suspension: 23:25:00 to 08:59:59 next day
    if (h == 23 and m >= 25) or (h < 9):
        return "SUSPENDED"
        
    return "OPEN"

# Setup Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("arbitrage-bot")

# Initialize FastAPI App
app = FastAPI(title="Spread Arbitrage Workstation Core")

# Middleware Setup
app.add_middleware(GZipMiddleware, minimum_size=1000)
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
        self.broker = "Groww"
        
        # Angel One Integration properties
        self.api_key = os.getenv("ANGELONE_API_KEY", "e72eCDuy")
        self.client_id = os.getenv("ANGELONE_CLIENT_ID", "")
        self.password = os.getenv("ANGELONE_PASSWORD", "")
        self.totp_secret = os.getenv("ANGELONE_TOTP_SECRET", "")
        
        # Groww Integration properties
        self.groww_api_key = os.getenv("GROWW_API_KEY", "")
        self.groww_client_id = os.getenv("GROWW_CLIENT_ID", "")
        self.groww_secret = os.getenv("GROWW_SECRET", "")
        self.groww_petal_symbol = "GOLDPETAL31JUL26FUT"
        self.groww_mini_symbol = "GOLDM05AUG26FUT"
        self.groww_client = None
        
        # Depth spread logging state variables
        self.last_logged_buy_spread = 0.0
        self.last_logged_sell_spread = 0.0

        # Dhan Integration properties
        self.dhan_client_id = os.getenv("DHAN_CLIENT_ID", "")
        self.dhan_access_token = os.getenv("DHAN_ACCESS_TOKEN", "")
        self.dhan_petal_symbol = "GOLDPETAL31AUG26FUT"
        self.dhan_petal_token = ""
        self.dhan_mini_symbol = "GOLDM04SEP26FUT"
        self.dhan_mini_token = ""
        self.dhan_client = None
        self.dhan_tokens_cache = {}
        self.dhan_official_symbols = {}

        # Upstox Integration properties
        self.upstox_client_id = os.getenv("UPSTOX_CLIENT_ID", "")
        self.upstox_secret = os.getenv("UPSTOX_SECRET", "")
        self.upstox_access_token = os.getenv("UPSTOX_ACCESS_TOKEN", "")
        self.upstox_petal_symbol = "MCX_FO|41223"
        self.upstox_mini_symbol = "MCX_FO|41224"

        self.petal_symbol = "GOLDPETAL31JUL26"
        self.petal_token = "250000"
        self.mini_symbol = "GOLDM05AUG26"
        self.mini_token = "250001"
        
        self.angelone_petal_symbol = "GOLDPETAL31JUL26"
        self.angelone_petal_token = "250000"
        self.angelone_mini_symbol = "GOLDM05AUG26"
        self.angelone_mini_token = "250001"
        
        self.smart_connect = None
        self.mcx_tokens_cache = {}
        self.mcx_official_symbols = {}
        
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
        
        self.month_master = []
        self.load_month_master()
        
        # Load trade history from persistence file
        self.load_trade_history()
        
        # Caching dictionaries for LTP and depth of all traded instruments
        self.symbol_depths = {}
        self.symbol_ltps = {}
        
        # Trade Automation Strategy State
        self.ta_configs = []
        self.ta_trades = []
        self.ta_execution_in_progress = False
        
        self.load_ta_trades()
        self.load_ta_configs()
        self.load_angel_master()
        
    def load_angel_master(self):
        try:
            if os.path.exists("angel_master.json"):
                with open("angel_master.json", "r", encoding="utf-8") as f:
                    data = json.load(f)
                    if data.get("api_key"): self.api_key = data["api_key"]
                    if data.get("client_id"): self.client_id = data["client_id"]
                    if data.get("password"): self.password = data["password"]
                    if data.get("totp_secret"): self.totp_secret = data["totp_secret"]
                self.log("[PERSISTENCE] Loaded Angel One master credentials from angel_master.json.")
        except Exception as e:
            self.log(f"[PERSISTENCE ERROR] Failed to load Angel One master config: {e}")

    def save_angel_master(self):
        try:
            data = {
                "api_key": self.api_key,
                "client_id": self.client_id,
                "password": self.password,
                "totp_secret": self.totp_secret
            }
            with open("angel_master.json", "w", encoding="utf-8") as f:
                json.dump(data, f, indent=4)
            self.log("[PERSISTENCE] Saved Angel One master credentials.")
        except Exception as e:
            self.log(f"[PERSISTENCE ERROR] Failed to save Angel One master config: {e}")
        
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

    def load_month_master(self):
        try:
            if os.path.exists("month_master.json"):
                with open("month_master.json", "r", encoding="utf-8") as f:
                    self.month_master = json.load(f)
                self.log(f"[PERSISTENCE] Loaded {len(self.month_master)} month master mappings from month_master.json.")
            else:
                self.month_master = []
                self.log("[PERSISTENCE] No month master file found. Starting fresh.")
        except Exception as e:
            self.log(f"[PERSISTENCE ERROR] Failed to load month master: {e}")
            self.month_master = []

    def save_month_master(self):
        try:
            with open("month_master.json", "w", encoding="utf-8") as f:
                json.dump(self.month_master, f, indent=4)
            self.log("[PERSISTENCE] Saved month master mappings.")
        except Exception as e:
            self.log(f"[PERSISTENCE ERROR] Failed to save month master: {e}")

    def load_ta_trades(self):
        try:
            if os.path.exists("ta_trades.json"):
                with open("ta_trades.json", "r", encoding="utf-8") as f:
                    self.ta_trades = json.load(f)
                self.log(f"[PERSISTENCE] Loaded {len(self.ta_trades)} Trade Automation trades from ta_trades.json.")
            else:
                self.ta_trades = []
        except Exception as e:
            self.log(f"[PERSISTENCE ERROR] Failed to load ta trades: {e}")
            self.ta_trades = []

    def save_ta_trades(self):
        try:
            with open("ta_trades.json", "w", encoding="utf-8") as f:
                json.dump(self.ta_trades, f, indent=4)
        except Exception as e:
            self.log(f"[PERSISTENCE ERROR] Failed to save ta trades: {e}")

    def load_ta_configs(self):
        try:
            if os.path.exists("ta_configs.json"):
                with open("ta_configs.json", "r", encoding="utf-8") as f:
                    self.ta_configs = json.load(f)
                self.log(f"[PERSISTENCE] Loaded {len(self.ta_configs)} Trade Automation configs from ta_configs.json.")
            else:
                self.ta_configs = []
        except Exception as e:
            self.log(f"[PERSISTENCE ERROR] Failed to load ta configs: {e}")
            self.ta_configs = []

    def save_ta_configs(self):
        try:
            with open("ta_configs.json", "w", encoding="utf-8") as f:
                json.dump(self.ta_configs, f, indent=4)
        except Exception as e:
            self.log(f"[PERSISTENCE ERROR] Failed to save ta configs: {e}")

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
                    
                totp_clean = self.totp_secret.strip().replace(" ", "").upper()
                totp = pyotp.TOTP(totp_clean).now()
                self.log(f"[ANGELONE API] Generated 6-digit TOTP passcode: '{totp}' for Client ID: '{self.client_id}'")
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

    def init_groww_client(self):
        try:
            if self.groww_secret:
                self.log(f"[GROWW API] Initializing Groww API client using Auth Token...")
                from growwapi import GrowwAPI
                self.groww_client = GrowwAPI(self.groww_secret)
                self.log("[GROWW API] Connection initialized successfully. Ready for order routing.")
            else:
                self.log("[GROWW API] Warning: Groww API Auth Token (Secret Key / Token) missing. Enter details in settings.")
                self.groww_client = None
        except Exception as e:
            self.log(f"[GROWW API] Initialization error: {e}")
            self.groww_client = None

    def init_dhan_client(self):
        try:
            if self.dhan_client_id and self.dhan_access_token:
                self.log(f"[DHAN API] Initializing Dhan client for Client ID: {self.dhan_client_id}...")
                from dhanhq import dhanhq
                try:
                    # Positional argument style (legacy / standard for many versions)
                    self.dhan_client = dhanhq(self.dhan_client_id, self.dhan_access_token)
                except TypeError:
                    # DhanContext style (newer versions)
                    from dhanhq import DhanContext
                    context = DhanContext(client_id=self.dhan_client_id, access_token=self.dhan_access_token)
                    self.dhan_client = dhanhq(context)
                self.log("[DHAN API] Connection initialized successfully. Ready for order routing.")
            else:
                self.log("[DHAN API] Warning: Dhan API credentials (Client ID / Access Token) missing. Enter details in settings.")
                self.dhan_client = None
        except Exception as e:
            self.log(f"[DHAN API] Initialization error: {e}")
            self.dhan_client = None

    def get_symbol_from_token(self, token: str, default_symbol: str) -> str:
        if not token:
            return default_symbol
        if hasattr(self, "mcx_official_symbols") and self.mcx_official_symbols:
            if token in self.mcx_official_symbols:
                return self.mcx_official_symbols[token]
        return default_symbol

    def resolve_dhan_token(self, symbol: str) -> str:
        if not symbol:
            return ""
        sym_u = symbol.upper()
        token = self.dhan_tokens_cache.get(sym_u)
        if not token:
            token = self.dhan_tokens_cache.get(sym_u.removesuffix("FUT"))
        if not token:
            token = self.dhan_tokens_cache.get(f"{sym_u.removesuffix('FUT')}FUT")
        return token or ""

    def get_mcx_lot_size(self, symbol: str) -> int:
        sym_u = symbol.upper()
        if sym_u.startswith("GOLDPETAL"):
            return 1
        elif sym_u.startswith("GOLDM"):
            return 100
        elif sym_u.startswith("GOLD"):
            return 100
        return 1

    def resolve_scrip_token_via_api(self, symbol: str) -> str:
        if not symbol:
            return ""
        sym_clean = symbol.strip().upper()
        
        # 1. Check local cached MCX tokens (Fast & Zero Rate Limit)
        if hasattr(self, "mcx_tokens_cache") and self.mcx_tokens_cache:
            candidates = [
                sym_clean,
                sym_clean.removesuffix("FUT"),
                sym_clean.replace("2026", "26").replace("2025", "25").replace("2024", "24"),
                sym_clean.replace("2026", "26").removesuffix("FUT"),
                sym_clean.replace("GOLDMINI", "GOLDM").replace("2026", "26").removesuffix("FUT"),
                sym_clean.replace("GOLDM", "GOLDMINI").replace("2026", "26").removesuffix("FUT")
            ]
            
            for cand in candidates:
                if cand in self.mcx_tokens_cache:
                    token = self.mcx_tokens_cache[cand]
                    self.log(f"[SCRIP FINDER] Auto-resolved '{symbol}' -> '{cand}' (Token: {token}) from Scrip Master cache.")
                    return token

            # Check for partial prefix/contains matches in cache
            base_search = sym_clean.replace("2026", "26").removesuffix("FUT")
            for cached_sym, cached_tok in self.mcx_tokens_cache.items():
                c_upper = cached_sym.upper()
                if c_upper == base_search or c_upper.startswith(base_search) or base_search in c_upper:
                    self.log(f"[SCRIP FINDER] Auto-resolved '{symbol}' -> '{cached_sym}' (Token: {cached_tok}) from Scrip Master cache.")
                    return cached_tok

        # 2. Fallback to SmartAPI searchScrip if client is initialized
        if not self.smart_connect:
            return ""
        try:
            search_query = sym_clean.removesuffix("FUT").replace("2026", "26")
            self.log(f"[API LOOKUP] Searching token for '{search_query}' on MCX...")
            res = self.smart_connect.searchScrip(exchange="MCX", searchscrip=search_query)
            if res and isinstance(res, dict) and res.get("status") == True:
                data = res.get("data", [])
                
                # Flexible matching helper
                def find_match(items):
                    for item in items:
                        ts = item.get("tradingsymbol", "")
                        if ts == symbol or ts == search_query or ts.startswith(search_query):
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
                    return token
                    
            self.log(f"[API LOOKUP] Search returned no direct matches for '{symbol}' on MCX.")
        except Exception as e:
            self.log(f"[API LOOKUP] Search failed for '{symbol}': Rate limit error ({e}).")
        return ""



# Global State Instance
system_state = TradingSystem()

# Token verification helper
# ContextVar for thread-safe authentication status
auth_status_var = contextvars.ContextVar("auth_status", default=False)

# Simple memory-based rate limiting for login attempts
FAILED_LOGIN_ATTEMPTS = {}  # IP -> (attempts, lock_until)

def is_login_rate_limited(ip_address: str) -> bool:
    if ip_address in FAILED_LOGIN_ATTEMPTS:
        attempts, lock_until = FAILED_LOGIN_ATTEMPTS[ip_address]
        if lock_until > time.time():
            return True
        elif time.time() >= lock_until and attempts >= 5:
            # Lock has expired, reset attempts
            FAILED_LOGIN_ATTEMPTS[ip_address] = (0, 0.0)
    return False

def record_failed_login(ip_address: str):
    if ip_address not in FAILED_LOGIN_ATTEMPTS:
        FAILED_LOGIN_ATTEMPTS[ip_address] = (1, 0.0)
    else:
        attempts, _ = FAILED_LOGIN_ATTEMPTS[ip_address]
        attempts += 1
        lock_until = 0.0
        if attempts >= 5:
            lock_until = time.time() + 300  # Lock for 5 minutes (300 seconds)
        FAILED_LOGIN_ATTEMPTS[ip_address] = (attempts, lock_until)

def record_successful_login(ip_address: str):
    if ip_address in FAILED_LOGIN_ATTEMPTS:
        del FAILED_LOGIN_ATTEMPTS[ip_address]

# Session signing & verification
SESSION_SECRET = os.getenv("SESSION_SECRET", "super_secret_gold_arbitrage_key_2026")

def sign_session_token(username: str) -> str:
    timestamp = int(time.time())
    payload = f"{username}:{timestamp}"
    signature = hmac.new(
        SESSION_SECRET.encode("utf-8"),
        payload.encode("utf-8"),
        hashlib.sha256
    ).hexdigest()
    token_str = f"{payload}:{signature}"
    return base64.b64encode(token_str.encode("utf-8")).decode("utf-8")

def verify_session_token(token: str) -> bool:
    try:
        decoded = base64.b64decode(token.encode("utf-8")).decode("utf-8")
        parts = decoded.split(":")
        if len(parts) != 3:
            return False
        username, timestamp_str, signature = parts
        timestamp = int(timestamp_str)
        
        # Check session expiration: 7 days (604800 seconds)
        if time.time() - timestamp > 604800:
            return False
            
        expected_username = os.getenv("ADMIN_USERNAME", "admin")
        if username != expected_username:
            return False
            
        payload = f"{username}:{timestamp_str}"
        expected_signature = hmac.new(
            SESSION_SECRET.encode("utf-8"),
            payload.encode("utf-8"),
            hashlib.sha256
        ).hexdigest()
        return hmac.compare_digest(signature, expected_signature)
    except Exception:
        return False

# Token verification helper (used by route functions)
def verify_token(token: str = None, authorization: str = Header(None)):
    # Check context first
    if auth_status_var.get():
        return
        
    auth_token = os.getenv("AUTH_TOKEN", "secret_arbitrage_token_2026")
    provided_token = None
    if token:
        provided_token = token
    elif authorization and authorization.startswith("Bearer "):
        provided_token = authorization.split(" ")[1]
        
    if provided_token != auth_token:
        raise HTTPException(status_code=401, detail="Invalid Authentication Token")

# FastAPI HTTP Middleware for Authentication
@app.middleware("http")
async def auth_middleware(request: Request, call_next):
    path = request.url.path
    
    # 1. Allow public files/endpoints
    if path in ["/api/login", "/style.css", "/script.js", "/favicon.ico"]:
        return await call_next(request)
        
    # 2. Allow WS routes to authenticate themselves
    if path.startswith("/ws/"):
        return await call_next(request)
        
    is_authenticated = False
    
    # 3. Check session cookie
    cookie_token = request.cookies.get("session_token")
    if cookie_token and verify_session_token(cookie_token):
        is_authenticated = True
        
    # 4. Check query token or Bearer authorization header
    if not is_authenticated:
        token_param = request.query_params.get("token")
        auth_header = request.headers.get("authorization")
        auth_token = os.getenv("AUTH_TOKEN", "secret_arbitrage_token_2026")
        
        provided_token = None
        if token_param:
            provided_token = token_param
        elif auth_header and auth_header.startswith("Bearer "):
            provided_token = auth_header.split(" ")[1]
            
        if provided_token == auth_token:
            is_authenticated = True
            
    # Set context auth status
    auth_status_token = auth_status_var.set(is_authenticated)
    
    try:
        if not is_authenticated:
            # For API endpoints, return JSON error
            if path.startswith("/api/"):
                return JSONResponse(status_code=401, content={"detail": "Unauthorized"})
            
            # For pages, render the login.html interface
            try:
                base_dir = os.path.dirname(os.path.abspath(__file__))
                file_path = os.path.join(base_dir, "login.html")
                with open(file_path, "r", encoding="utf-8") as f:
                    return HTMLResponse(content=f.read(), status_code=200)
            except FileNotFoundError:
                return HTMLResponse(content="<h3>login.html not found</h3>", status_code=404)
                
        # Proceed with request
        response = await call_next(request)
        return response
    finally:
        # Reset context variable to avoid leakage
        auth_status_var.reset(auth_status_token)


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

last_broadcast_time = 0.0

async def broadcast_system_state(force: bool = False):
    global last_broadcast_time
    now = time.time()
    if not force and (now - last_broadcast_time < 0.1):
        return
    last_broadcast_time = now

    session_status = get_market_session_status()
    display_status = system_state.system_status
    if session_status == "HOLD":
        display_status = "Hold"
    elif session_status == "SUSPENDED":
        display_status = "Suspended"

    await manager.broadcast({
        "gold_petal_ltp": round(system_state.gold_petal_ltp, 2),
        "gold_mini_ltp": round(system_state.gold_mini_ltp, 2),
        "spread": round(system_state.spread, 2),
        "depth_buy_spread": round(system_state.depth_buy_spread, 2),
        "depth_sell_spread": round(system_state.depth_sell_spread, 2),
        
        "is_in_position": system_state.is_in_position,
        "position_direction": system_state.position_direction,
        "system_status": display_status,
        
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
        "groww_api_key": system_state.groww_api_key,
        "groww_client_id": system_state.groww_client_id,
        "groww_secret": system_state.groww_secret,
        "groww_petal_symbol": system_state.groww_petal_symbol,
        "groww_mini_symbol": system_state.groww_mini_symbol,
        "dhan_client_id": system_state.dhan_client_id,
        "dhan_access_token": system_state.dhan_access_token,
        "dhan_petal_symbol": system_state.dhan_petal_symbol,
        "dhan_petal_token": system_state.dhan_petal_token,
        "dhan_mini_symbol": system_state.dhan_mini_symbol,
        "dhan_mini_token": system_state.dhan_mini_token,
        "upstox_client_id": system_state.upstox_client_id,
        "upstox_secret": system_state.upstox_secret,
        "upstox_access_token": system_state.upstox_access_token,
        "upstox_petal_symbol": system_state.upstox_petal_symbol,
        "upstox_mini_symbol": system_state.upstox_mini_symbol,
        "petal_symbol": system_state.petal_symbol,
        "petal_token": system_state.petal_token,
        "mini_symbol": system_state.mini_symbol,
        "mini_token": system_state.mini_token,
        "angelone_petal_symbol": system_state.angelone_petal_symbol,
        "angelone_petal_token": system_state.angelone_petal_token,
        "angelone_mini_symbol": system_state.angelone_mini_symbol,
        "angelone_mini_token": system_state.angelone_mini_token,
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
        "month_master": system_state.month_master,
        "month_master_live": getattr(system_state, "month_master_live", []),
        
        # Trade Automation Broadcast fields
        "ta_configs": system_state.ta_configs,
        "ta_trades": system_state.ta_trades,
        
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

async def check_dhan_orders_status(order_ids: List[str]) -> Dict[str, str]:
    if not system_state.dhan_client:
        return {}
    try:
        loop = asyncio.get_running_loop()
        response = await loop.run_in_executor(
            None,
            lambda: system_state.dhan_client.get_order_list()
        )
        if isinstance(response, dict) and response.get("status") == "success":
            order_list = response.get("data", [])
            status_map = {}
            for o in order_list:
                oid = str(o.get("orderId", ""))
                if oid in order_ids:
                    raw_status = o.get("orderStatus", "").upper()
                    if raw_status == "TRADED":
                        status_map[oid] = "COMPLETE"
                    else:
                        status_map[oid] = raw_status
            return status_map
    except Exception as e:
        system_state.log(f"[DHAN LIVE ORDER STATUS] Error checking order book: {e}")
    return {}

async def cancel_dhan_order(order_id: str):
    if not system_state.dhan_client:
        return
    try:
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(
            None,
            lambda: system_state.dhan_client.cancel_order(order_id)
        )
        system_state.log(f"[DHAN LIVE ORDER] Cancelled order {order_id}")
    except Exception as e:
        system_state.log(f"[DHAN LIVE ORDER ERROR] Failed to cancel order {order_id}: {e}")

async def check_groww_orders_status(order_ids: List[str]) -> Dict[str, str]:
    if not system_state.groww_client:
        return {}
    try:
        loop = asyncio.get_running_loop()
        status_map = {}
        for oid in order_ids:
            response = await loop.run_in_executor(
                None,
                lambda: system_state.groww_client.get_order_status(order_id=oid)
            )
            if isinstance(response, dict):
                raw_status = response.get("order_status", "").upper()
                if raw_status in ["SUCCESS", "COMPLETE", "TRADED"]:
                    status_map[oid] = "COMPLETE"
                elif raw_status == "OPEN":
                    status_map[oid] = "OPEN"
                elif raw_status in ["FAILED", "REJECTED"]:
                    status_map[oid] = "REJECTED"
                elif raw_status == "CANCELLED":
                    status_map[oid] = "CANCELLED"
                else:
                    status_map[oid] = raw_status
        return status_map
    except Exception as e:
        system_state.log(f"[GROWW LIVE ORDER STATUS] Error checking order book: {e}")
    return {}

async def cancel_groww_order(order_id: str):
    if not system_state.groww_client:
        return
    try:
        from growwapi import GrowwAPI
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(
            None,
            lambda: system_state.groww_client.cancel_order(
                order_id=order_id,
                segment=GrowwAPI.SEGMENT_COMMODITY
            )
        )
        system_state.log(f"[GROWW LIVE ORDER] Cancelled order {order_id}")
    except Exception as e:
        system_state.log(f"[GROWW LIVE ORDER ERROR] Failed to cancel order {order_id}: {e}")

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

async def execute_trade(petal_action: str, mini_action: str, check_liquidity: bool = True, is_entry: bool = True, qty: int = None,
                        alt_petal_symbol: str = None, alt_petal_token: str = None,
                        alt_mini_symbol: str = None, alt_mini_token: str = None,
                        paper_mode_override: bool = None) -> dict:
    if qty is None:
        qty = system_state.trade_quantity
    required_petal = qty * 100
    required_mini = qty
    
    direction = system_state.position_direction if not is_entry else ("Expansion" if petal_action == "BUY" else "Contraction")
    
    # Resolve target symbols and tokens
    target_petal_symbol = alt_petal_symbol if alt_petal_symbol is not None else system_state.petal_symbol
    target_petal_token = alt_petal_token if alt_petal_token is not None else system_state.petal_token
    target_mini_symbol = alt_mini_symbol if alt_mini_symbol is not None else system_state.mini_symbol
    target_mini_token = alt_mini_token if alt_mini_token is not None else system_state.mini_token
    
    # Resolve depths and LTPs from caching dictionaries
    petal_depth = system_state.symbol_depths.get(target_petal_symbol) or system_state.symbol_depths.get(target_petal_token) or system_state.petal_depth
    mini_depth = system_state.symbol_depths.get(target_mini_symbol) or system_state.symbol_depths.get(target_mini_token) or system_state.mini_depth
    
    petal_ltp = system_state.symbol_ltps.get(target_petal_symbol) or system_state.symbol_ltps.get(target_petal_token) or system_state.gold_petal_ltp
    mini_ltp = system_state.symbol_ltps.get(target_mini_symbol) or system_state.symbol_ltps.get(target_mini_token) or system_state.gold_mini_ltp
    
    # 1. Option A: Enforce Depth Guard Check (Must have valid depth for execution sides)
    petal_side = "sell" if petal_action == "BUY" else "buy"
    mini_side = "sell" if mini_action == "BUY" else "buy"
    
    if (not isinstance(petal_depth, dict) or 
            petal_side not in petal_depth or 
            not petal_depth[petal_side] or 
            len(petal_depth[petal_side]) == 0):
        msg = f"Missing {target_petal_symbol} depth for {petal_side} side."
        system_state.log(f"[DEPTH GUARD] Trade skipped: {msg}")
        record_failed_attempt(direction, "FAILED", f"Depth Guard: {msg}", is_entry)
        return {"success": False, "status": "FAILED", "reason": f"Depth Guard: {msg}"}

    if (not isinstance(mini_depth, dict) or 
            mini_side not in mini_depth or 
            not mini_depth[mini_side] or 
            len(mini_depth[mini_side]) == 0):
        msg = f"Missing {target_mini_symbol} depth for {mini_side} side."
        system_state.log(f"[DEPTH GUARD] Trade skipped: {msg}")
        record_failed_attempt(direction, "FAILED", f"Depth Guard: {msg}", is_entry)
        return {"success": False, "status": "FAILED", "reason": f"Depth Guard: {msg}"}
        
    # 2. Liquidity Shield (Bid-Ask Gap Check)
    if check_liquidity:
        liquidity_ok = True
        try:
            if (isinstance(petal_depth, dict) and 
                    "buy" in petal_depth and len(petal_depth["buy"]) > 0 and
                    "sell" in petal_depth and len(petal_depth["sell"]) > 0):
                petal_bid = float(petal_depth["buy"][0]["price"])
                petal_ask = float(petal_depth["sell"][0]["price"])
                if (petal_ask - petal_bid) > 15.0:
                    system_state.log(f"[LIQUIDITY SHIELD] Trade skipped: {target_petal_symbol} Bid-Ask gap too wide ({petal_ask - petal_bid:.2f} > 15.0).")
                    liquidity_ok = False
    
            if (isinstance(mini_depth, dict) and 
                    "buy" in mini_depth and len(mini_depth["buy"]) > 0 and
                    "sell" in mini_depth and len(mini_depth["sell"]) > 0):
                mini_bid = float(mini_depth["buy"][0]["price"])
                mini_ask = float(mini_depth["sell"][0]["price"])
                if (mini_ask - mini_bid) > 150.0:
                    system_state.log(f"[LIQUIDITY SHIELD] Trade skipped: {target_mini_symbol} Bid-Ask gap too wide ({mini_ask - mini_bid:.2f} > 150.0).")
                    liquidity_ok = False
        except Exception as e:
            logger.warning(f"Error parsing depth for bid-ask gap check: {e}")
            
        if not liquidity_ok:
            record_failed_attempt(direction, "FAILED", "Liquidity Pre-check: Bid-Ask gap too wide", is_entry)
            return {"success": False, "status": "FAILED", "reason": "Liquidity Pre-check: Bid-Ask gap too wide"}

    # 3. Calculate VWAP (Volume-Weighted Average Price) from Depth
    petal_price = get_depth_average_price(petal_depth, petal_side, required_petal, petal_ltp)
    mini_price = get_depth_average_price(mini_depth, mini_side, required_mini, mini_ltp)

    system_state.log(f"[MARKET EXECUTION] Dispatching market orders: {target_petal_symbol} {petal_action} @ {petal_price:.2f}, {target_mini_symbol} {mini_action} @ {mini_price:.2f}")

    is_paper_trading = paper_mode_override if paper_mode_override is not None else system_state.paper_trading_mode

    if is_paper_trading:
        # Paper Trading execution: fill instantly at average price
        system_state.log(f"[PAPER MARKET FILL] {target_petal_symbol} {petal_action} filled @ MARKET {petal_price:.2f}")
        system_state.log(f"[PAPER MARKET FILL] {target_mini_symbol} {mini_action} filled @ MARKET {mini_price:.2f}")
        
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
        # Determine the broker methods dynamically
        if system_state.broker == "Dhan":
            if not system_state.dhan_client:
                system_state.log("[DHAN API] Error: Client not initialized. Cannot place live orders.")
                record_failed_attempt(direction, "FAILED", "Dhan client not initialized", is_entry)
                return {"success": False, "status": "FAILED", "reason": "Dhan client not initialized"}
                
            async def place_order_func(symbol: str, token: str, action: str, order_qty: int):
                correct_symbol = symbol
                if token in system_state.dhan_official_symbols:
                    correct_symbol = system_state.dhan_official_symbols[token]
                lot_multiplier = system_state.get_mcx_lot_size(correct_symbol)
                final_qty = order_qty * lot_multiplier
                system_state.log(f"[DHAN LIVE ORDER] Symbol: {correct_symbol}, Token: {token}, Action: {action}, Multiplier: {lot_multiplier}, Qty: {final_qty}")
                try:
                    loop = asyncio.get_running_loop()
                    response = await loop.run_in_executor(
                        None,
                        lambda: system_state.dhan_client.place_order(
                            security_id=token,
                            exchange_segment="MCX_COMM",
                            transaction_type=action,
                            quantity=final_qty,
                            order_type="MARKET",
                            product_type="MARGIN",
                            price=0.0,
                            validity="DAY"
                        )
                    )
                    system_state.log(f"[DHAN RESPONSE] {response}")
                    if isinstance(response, dict):
                        data_block = response.get("data", {})
                        if isinstance(data_block, dict):
                            return str(data_block.get("orderId") or data_block.get("orderid") or "")
                    return ""
                except Exception as e:
                    system_state.log(f"[DHAN LIVE ORDER ERROR] Failed to place order for {symbol}: {e}")
                    return ""

            cancel_order_func = cancel_dhan_order
            check_status_func = check_dhan_orders_status

        elif system_state.broker == "Groww":
            if not system_state.groww_client:
                system_state.log("[GROWW API] Error: Client not initialized. Cannot place live orders.")
                record_failed_attempt(direction, "FAILED", "Groww client not initialized", is_entry)
                return {"success": False, "status": "FAILED", "reason": "Groww client not initialized"}
                
            async def place_order_func(symbol: str, token: str, action: str, order_qty: int):
                from growwapi import GrowwAPI
                lot_multiplier = system_state.get_mcx_lot_size(symbol)
                final_qty = order_qty * lot_multiplier
                system_state.log(f"[GROWW LIVE ORDER] Symbol: {symbol}, Action: {action}, Multiplier: {lot_multiplier}, Qty: {final_qty}")
                try:
                    trans_type = GrowwAPI.TRANSACTION_TYPE_BUY if action.upper() == "BUY" else GrowwAPI.TRANSACTION_TYPE_SELL
                    target_price = petal_price if symbol == target_petal_symbol else mini_price
                    loop = asyncio.get_running_loop()
                    response = await loop.run_in_executor(
                        None,
                        lambda: system_state.groww_client.place_order(
                            trading_symbol=symbol,
                            quantity=final_qty,
                            validity=GrowwAPI.VALIDITY_DAY,
                            exchange=GrowwAPI.EXCHANGE_MCX,
                            segment=GrowwAPI.SEGMENT_COMMODITY,
                            product=GrowwAPI.PRODUCT_NRML,
                            order_type=GrowwAPI.ORDER_TYPE_LIMIT,
                            transaction_type=trans_type,
                            price=float(target_price)
                        )
                    )
                    system_state.log(f"[GROWW RESPONSE] {response}")
                    if isinstance(response, dict):
                        return str(response.get("groww_order_id") or "")
                    return ""
                except Exception as e:
                    system_state.log(f"[GROWW LIVE ORDER ERROR] Failed to place order for {symbol}: {e}")
                    return ""

            cancel_order_func = cancel_groww_order
            check_status_func = check_groww_orders_status

        else:
            # Default to AngelOne API
            if not ANGELONE_SDK_AVAILABLE or not system_state.smart_connect:
                system_state.log("[ANGELONE API] Error: Client not initialized. Cannot place live orders.")
                record_failed_attempt(direction, "FAILED", "AngelOne client not initialized", is_entry)
                return {"success": False, "status": "FAILED", "reason": "AngelOne client not initialized"}
                
            async def place_order_func(symbol: str, token: str, action: str, order_qty: int):
                correct_symbol = system_state.get_symbol_from_token(token, symbol)
                lot_multiplier = system_state.get_mcx_lot_size(correct_symbol)
                final_qty = order_qty * lot_multiplier
                system_state.log(f"[LIVE ORDER PARAMETERS] Symbol: {correct_symbol}, Token: {token}, Action: {action}, Multiplier: {lot_multiplier}, Target Qty: {order_qty} -> Final API Qty: {final_qty}")
                order_params = {
                    "variety": "NORMAL",
                    "tradingsymbol": correct_symbol,
                    "symboltoken": token,
                    "transactiontype": action,
                    "exchange": "MCX",
                    "ordertype": "MARKET",
                    "producttype": "CARRYFORWARD",
                    "duration": "DAY",
                    "quantity": str(final_qty)
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

            cancel_order_func = cancel_real_order
            check_status_func = check_real_orders_status

        # Place the market orders concurrently
        petal_order_id = await place_order_func(target_petal_symbol, target_petal_token, petal_action, required_petal)
        mini_order_id = await place_order_func(target_mini_symbol, target_mini_token, mini_action, required_mini)
        
        if not petal_order_id and not mini_order_id:
            system_state.log("[LIVE ORDER ERROR] Both market order placements failed to return IDs.")
            record_failed_attempt(direction, "FAILED", "Market order placements failed", is_entry)
            return {"success": False, "status": "FAILED", "reason": "Market order placements failed"}
            
        # Instant rollback if one order fails to place on the broker API
        if petal_order_id and not mini_order_id:
            system_state.log(f"[EMERGENCY ROLLBACK] Leg 2 ({target_mini_symbol}) failed to place. Reversing Leg 1 ({target_petal_symbol}) instantly...")
            rollback_action = "SELL" if petal_action == "BUY" else "BUY"
            await place_order_func(target_petal_symbol, target_petal_token, rollback_action, required_petal)
            record_failed_attempt(direction, "FAILED", "Leg 2 failed to place. Leg 1 rolled back.", is_entry)
            return {"success": False, "status": "FAILED", "reason": "Leg 2 failed to place"}
            
        if mini_order_id and not petal_order_id:
            system_state.log(f"[EMERGENCY ROLLBACK] Leg 1 ({target_petal_symbol}) failed to place. Reversing Leg 2 ({target_mini_symbol}) instantly...")
            rollback_action = "SELL" if mini_action == "BUY" else "BUY"
            await place_order_func(target_mini_symbol, target_mini_token, rollback_action, required_mini)
            record_failed_attempt(direction, "FAILED", "Leg 1 failed to place. Leg 2 rolled back.", is_entry)
            return {"success": False, "status": "FAILED", "reason": "Leg 1 failed to place"}

        # Both placed successfully, check status loop
        petal_filled = False
        mini_filled = False
        
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
            status_map = await check_status_func([petal_order_id, mini_order_id])
            
            if not petal_filled:
                status = status_map.get(petal_order_id)
                if status in ["COMPLETE", "COMPLETED", "TRADED", "EXECUTED", "SUCCESS"]:
                    petal_filled = True
                    petal_fill_price = petal_ltp
                    system_state.log(f"[LIVE MARKET FILL] Leg 1: {target_petal_symbol} filled @ MARKET {petal_fill_price:.2f}")
                elif status in ["REJECTED", "CANCELLED"]:
                    system_state.log(f"[LIVE ORDER CANCEL/REJECT] Leg 1: {target_petal_symbol} order {status.lower()}")
                    break
                    
            if not mini_filled:
                status = status_map.get(mini_order_id)
                if status in ["COMPLETE", "COMPLETED", "TRADED", "EXECUTED", "SUCCESS"]:
                    mini_filled = True
                    mini_fill_price = mini_ltp
                    system_state.log(f"[LIVE MARKET FILL] Leg 2: {target_mini_symbol} filled @ MARKET {mini_fill_price:.2f}")
                elif status in ["REJECTED", "CANCELLED"]:
                    system_state.log(f"[LIVE ORDER CANCEL/REJECT] Leg 2: {target_mini_symbol} order {status.lower()}")
                    break
                    
            if petal_filled and mini_filled:
                break

        # Cancel/reverse if not completed
        if not petal_filled and not mini_filled:
            # Market orders usually execute instantly, but if stuck in pending (rare), cancel them.
            if petal_order_id:
                await cancel_order_func(petal_order_id)
            if mini_order_id:
                await cancel_order_func(mini_order_id)
            system_state.log("[LIVE TIMEOUT] Both orders timed out without fill. Orders cancelled.")
            record_failed_attempt(direction, "CANCELLED", "Timeout - no legs filled", is_entry)
            return {"success": False, "status": "CANCELLED", "reason": "Timeout - no legs filled"}
            
        # Emergency rollback if partial fill occurred (only one leg filled)
        if petal_filled and not mini_filled:
            system_state.log(f"[EMERGENCY ROLLBACK] Leg 1 ({target_petal_symbol}) filled, Leg 2 ({target_mini_symbol}) failed. Reversing Leg 1...")
            if petal_order_id:
                await cancel_order_func(petal_order_id)
            rollback_action = "SELL" if petal_action == "BUY" else "BUY"
            await place_order_func(target_petal_symbol, target_petal_token, rollback_action, required_petal)
            record_failed_attempt(direction, "FAILED", "Leg 1 filled, Leg 2 failed. Rolled back.", is_entry)
            return {"success": False, "status": "FAILED", "reason": "Partial fill Leg 2 failure"}
            
        if mini_filled and not petal_filled:
            system_state.log(f"[EMERGENCY ROLLBACK] Leg 2 ({target_mini_symbol}) filled, Leg 1 ({target_petal_symbol}) failed. Reversing Leg 2...")
            if mini_order_id:
                await cancel_order_func(mini_order_id)
            rollback_action = "SELL" if mini_action == "BUY" else "BUY"
            await place_order_func(target_mini_symbol, target_mini_token, rollback_action, required_mini)
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

async def run_ta_entry(mapping: dict, direction: str, qty: int, expected_spread: float, paper_mode: bool):
    if system_state.ta_execution_in_progress:
        return
    system_state.ta_execution_in_progress = True
    try:
        petal_action = "BUY" if direction == "Expansion" else "SELL"
        mini_action = "SELL" if direction == "Expansion" else "BUY"
        
        result = await execute_trade(
            petal_action, mini_action, 
            check_liquidity=True, 
            is_entry=True, 
            qty=qty,
            alt_petal_symbol=mapping["petal_symbol"],
            alt_petal_token=mapping.get("petal_token"),
            alt_mini_symbol=mapping["mini_symbol"],
            alt_mini_token=mapping.get("mini_token"),
            paper_mode_override=paper_mode
        )
        if result["success"]:
            trade_id = len(system_state.ta_trades) + 1
            new_trade = {
                "id": trade_id,
                "direction": direction,
                "quantity": qty,
                "status": "Open",
                "entry_time": get_ist_time_str("%H:%M:%S"),
                "entry_date": get_ist_time_str("%Y-%m-%d"),
                "petal_symbol": mapping["petal_symbol"],
                "mini_symbol": mapping["mini_symbol"],
                "petal_entry_price": result["petal_fill_price"],
                "mini_entry_price": result["mini_fill_price"],
                "entry_spread": (result["petal_fill_price"] * 10.0) - result["mini_fill_price"],
                "expected_entry_spread": expected_spread,
                "petal_entry_type": result["petal_order_type"],
                "mini_entry_type": result["mini_order_type"],
                "petal_exit_price": 0.0,
                "mini_exit_price": 0.0,
                "exit_spread": 0.0,
                "actual_exit_spread": 0.0,
                "petal_exit_type": "--",
                "mini_exit_type": "--",
                "exit_time": "--",
                "exit_date": "--",
                "pnl": 0.0,
                "charges": 0.0
            }
            system_state.ta_trades.append(new_trade)
            system_state.save_ta_trades()
            system_state.log(f"[TA ENTRY] Filled {direction} entry. Expected Spread {expected_spread:.2f}, Filled Spread {new_trade['entry_spread']:.2f}. Fill Price Petal {new_trade['petal_entry_price']:.2f}, Mini {new_trade['mini_entry_price']:.2f}")
    except Exception as e:
        system_state.log(f"[TA ENTRY ERROR] {e}")
    finally:
        system_state.ta_execution_in_progress = False
        await broadcast_system_state()

async def run_ta_exit(trade: dict, mapping: dict, paper_mode: bool = True):
    if system_state.ta_execution_in_progress:
        return
    system_state.ta_execution_in_progress = True
    try:
        direction = trade["direction"]
        petal_action = "SELL" if direction == "Expansion" else "BUY"
        mini_action = "BUY" if direction == "Expansion" else "SELL"
        qty = trade["quantity"]
        
        result = await execute_trade(
            petal_action, mini_action, 
            check_liquidity=False, 
            is_entry=False, 
            qty=qty,
            alt_petal_symbol=mapping["petal_symbol"],
            alt_petal_token=mapping.get("petal_token"),
            alt_mini_symbol=mapping["mini_symbol"],
            alt_mini_token=mapping.get("mini_token"),
            paper_mode_override=paper_mode
        )
        if result["success"]:
            petal_exit = result["petal_fill_price"]
            mini_exit = result["mini_fill_price"]
            petal_exit_type = result["petal_order_type"]
            mini_exit_type = result["mini_order_type"]
            
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
            trade["petal_exit_type"] = petal_exit_type
            trade["mini_exit_type"] = mini_exit_type
            trade["exit_spread"] = expected_exit_spread
            trade["actual_exit_spread"] = actual_exit_spread
            trade["exit_slippage"] = exit_slippage
            trade["pnl"] = net_pnl
            trade["charges"] = charges
            trade["exit_time"] = get_ist_time_str("%H:%M:%S")
            trade["exit_date"] = get_ist_time_str("%Y-%m-%d")
            
            # Update overall realized PnL
            system_state.realized_pnl += net_pnl
            system_state.total_trades += 1
            if net_pnl > 0:
                system_state.winning_trades += 1
            system_state.win_ratio = (system_state.winning_trades / system_state.total_trades) * 100.0 if system_state.total_trades > 0 else 0.0
            
            # Put into trade history so it shows up in history table too
            history_record = {
                "id": len(system_state.trade_history) + 1,
                "date": trade["entry_date"],
                "direction": direction,
                "status": "COMPLETED",
                "entry_time": trade["entry_time"],
                "exit_time": trade["exit_time"],
                "petal_action": petal_action,
                "mini_action": mini_action,
                "petal_entry": round(trade["petal_entry_price"], 2),
                "mini_entry": round(trade["mini_entry_price"], 2),
                "petal_exit": round(petal_exit, 2),
                "mini_exit": round(mini_exit, 2),
                "entry_spread": round(trade["expected_entry_spread"], 2),
                "actual_entry_spread": round(trade["entry_spread"], 2),
                "entry_slippage": round(trade.get("entry_slippage", 0.0), 2),
                "exit_spread": round(expected_exit_spread, 2),
                "actual_exit_spread": round(actual_exit_spread, 2),
                "exit_slippage": round(exit_slippage, 2),
                "petal_entry_type": trade["petal_entry_type"],
                "mini_entry_type": trade["mini_entry_type"],
                "petal_exit_type": petal_exit_type,
                "mini_exit_type": mini_exit_type,
                "petal_pnl": round(p_pnl, 2),
                "mini_pnl": round(m_pnl, 2),
                "gross_pnl": round(trade_pnl, 2),
                "charges": round(charges, 2),
                "pnl": round(net_pnl, 2),
                "reason": "TA-Exit",
                "details": f"Trade Automation ID {trade['id']} closed. Net P&L: {net_pnl:.2f}."
            }
            system_state.trade_history.append(history_record)
            
            system_state.save_ta_trades()
            system_state.save_trade_history()
            system_state.log(f"[TA EXIT] Squared off trade ID {trade['id']}. Net PnL: INR {net_pnl:+.2f}")
    except Exception as e:
        system_state.log(f"[TA EXIT ERROR] {e}")
    finally:
        system_state.ta_execution_in_progress = False
        await broadcast_system_state()

async def run_trade_automation_checks():
    if system_state.ta_execution_in_progress:
        return
        
    for config in system_state.ta_configs:
        if not config.get("enabled", False):
            continue
            
        idx = config.get("month_idx", -1)
        if idx < 0 or idx >= len(system_state.month_master):
            continue
            
        mapping = system_state.month_master[idx]
        p_sym = mapping["petal_symbol"]
        m_sym = mapping["mini_symbol"]
        
        # Find matching live stat in system_state.month_master_live
        live_stat = None
        for stat in system_state.month_master_live:
            if stat["petal_symbol"] == p_sym and stat["mini_symbol"] == m_sym:
                live_stat = stat
                break
                
        if not live_stat:
            continue
            
        buy_spread = live_stat["depth_buy_spread"]
        sell_spread = live_stat["depth_sell_spread"]
        
        # Get active (Open) Trade Automation trades for this specific month pair
        open_trades = [t for t in system_state.ta_trades if t["status"] == "Open" and t["petal_symbol"] == p_sym and t["mini_symbol"] == m_sym]
        
        direction = config.get("direction", "Expansion")
        qty = config.get("quantity", 1)
        entry_diff = config.get("entry_diff", 500.0)
        averaging_step = config.get("averaging_step", 50.0)
        exit_gap = config.get("exit_gap", 100.0)
        paper_mode = config.get("paper_mode", True)
        
        # Check exits first
        for trade in open_trades:
            entry_spread = trade["entry_spread"]
            exit_triggered = False
            if direction == "Expansion":
                if sell_spread >= entry_spread + exit_gap:
                    exit_triggered = True
            elif direction == "Contraction":
                if buy_spread <= entry_spread - exit_gap:
                    exit_triggered = True
                    
            if exit_triggered:
                system_state.log(f"[TA TRIGGER] Exit met for trade ID {trade['id']} ({p_sym}/{m_sym}). Entry: {entry_spread:.2f}, Exit: {sell_spread if direction == 'Expansion' else buy_spread:.2f}")
                await run_ta_exit(trade, mapping, paper_mode)
                return # Process one action at a time to prevent concurrency conflicts
                
        # Check entries
        if len(open_trades) == 0:
            # Check first entry
            entry_triggered = False
            if direction == "Expansion":
                if buy_spread <= entry_diff:
                    entry_triggered = True
            elif direction == "Contraction":
                if sell_spread >= entry_diff:
                    entry_triggered = True
                    
            if entry_triggered:
                system_state.log(f"[TA TRIGGER] First entry met for {p_sym}/{m_sym}. Spread: {buy_spread if direction == 'Expansion' else sell_spread:.2f} (Target: {entry_diff:.2f})")
                await run_ta_entry(mapping, direction, qty, buy_spread if direction == "Expansion" else sell_spread, paper_mode)
                return
        else:
            # Check averaging entry
            last_trade = open_trades[-1]
            last_entry_spread = last_trade["entry_spread"]
            
            averaging_triggered = False
            if direction == "Expansion":
                if buy_spread <= last_entry_spread - averaging_step:
                    averaging_triggered = True
            elif direction == "Contraction":
                if sell_spread >= last_entry_spread + averaging_step:
                    averaging_triggered = True
                    
            if averaging_triggered:
                system_state.log(f"[TA TRIGGER] Averaging entry met for {p_sym}/{m_sym}. Spread: {buy_spread if direction == 'Expansion' else sell_spread:.2f} (Last: {last_entry_spread:.2f}, Step: {averaging_step:.2f})")
                await run_ta_entry(mapping, direction, qty, buy_spread if direction == "Expansion" else sell_spread, paper_mode)
                return

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
                    "petal_symbol": system_state.petal_symbol,
                    "mini_symbol": system_state.mini_symbol,
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

def extract_month_from_symbol(symbol: str) -> str:
    if not symbol:
        return "UNKNOWN"
    months = ["JAN", "FEB", "MAR", "APR", "MAY", "JUN", "JUL", "AUG", "SEP", "OCT", "NOV", "DEC"]
    symbol_upper = symbol.upper()
    for m in months:
        if m in symbol_upper:
            return m
    return "UNKNOWN"

def record_depth_spread(petal_symbol: str, mini_symbol: str, 
                        avg_petal_buy: float, avg_mini_sell: float, depth_buy_spread: float,
                        avg_petal_sell: float, avg_mini_buy: float, depth_sell_spread: float):
    # Disabled logging to depth_spread_history.csv as requested
    pass

# ----------------- Trading Engine and Live Tickers -----------------
async def process_market_data(data: dict):
    global system_state
    
    petal_ltp = data["petal_ltp"]
    mini_ltp = data["mini_ltp"]
    spread = (petal_ltp * 10.0) - mini_ltp
    
    system_state.gold_petal_ltp = petal_ltp
    system_state.gold_mini_ltp = mini_ltp
    system_state.spread = spread

    # Cache main active instruments
    system_state.symbol_ltps[system_state.petal_symbol] = petal_ltp
    if system_state.petal_token:
        system_state.symbol_ltps[system_state.petal_token] = petal_ltp
    system_state.symbol_ltps[system_state.mini_symbol] = mini_ltp
    if system_state.mini_token:
        system_state.symbol_ltps[system_state.mini_token] = mini_ltp
        
    system_state.symbol_depths[system_state.petal_symbol] = system_state.petal_depth
    if system_state.petal_token:
        system_state.symbol_depths[system_state.petal_token] = system_state.petal_depth
    system_state.symbol_depths[system_state.mini_symbol] = system_state.mini_depth
    if system_state.mini_token:
        system_state.symbol_depths[system_state.mini_token] = system_state.mini_depth
    

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
    
    # Record depth spreads if they change (de-duplicated)
    if (abs(system_state.depth_buy_spread - system_state.last_logged_buy_spread) > 0.01 or 
            abs(system_state.depth_sell_spread - system_state.last_logged_sell_spread) > 0.01):
        
        system_state.last_logged_buy_spread = system_state.depth_buy_spread
        system_state.last_logged_sell_spread = system_state.depth_sell_spread
        
        record_depth_spread(
            system_state.petal_symbol, 
            system_state.mini_symbol, 
            avg_petal_buy, 
            avg_mini_sell, 
            system_state.depth_buy_spread, 
            avg_petal_sell, 
            avg_mini_buy, 
            system_state.depth_sell_spread
        )
            
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
            
    # Calculate live Trade Automation trades P&Ls and margins
    ta_unrealized_pnl = 0.0
    ta_used_margin = 0.0
    for trade in system_state.ta_trades:
        if trade.get("status") == "Open":
            t_qty = trade.get("quantity", 1)
            t_dir = trade.get("direction")
            t_petal_symbol = trade.get("petal_symbol")
            t_mini_symbol = trade.get("mini_symbol")
            t_petal_ltp = system_state.symbol_ltps.get(t_petal_symbol) or petal_ltp
            t_mini_ltp = system_state.symbol_ltps.get(t_mini_symbol) or mini_ltp
            
            if t_dir == "Expansion":
                t_petal_pnl = (t_petal_ltp - trade.get("petal_entry_price", 0.0)) * 100.0 * t_qty
                t_mini_pnl = (trade.get("mini_entry_price", 0.0) - t_mini_ltp) * 10.0 * t_qty
            else:
                t_petal_pnl = (trade.get("petal_entry_price", 0.0) - t_petal_ltp) * 100.0 * t_qty
                t_mini_pnl = (t_mini_ltp - trade.get("mini_entry_price", 0.0)) * 10.0 * t_qty
            trade["petal_pnl"] = t_petal_pnl
            trade["mini_pnl"] = t_mini_pnl
            trade["unrealized_pnl"] = t_petal_pnl + t_mini_pnl
            ta_unrealized_pnl += trade["unrealized_pnl"]
            ta_used_margin += 50000.0 * t_qty
            
    system_state.used_margin += manual_used_margin + ta_used_margin
    system_state.total_pnl = system_state.realized_pnl + system_state.unrealized_pnl + manual_unrealized_pnl + ta_unrealized_pnl
    system_state.available_balance = system_state.total_capital - system_state.used_margin + system_state.total_pnl
    if system_state.total_capital > 0:
        system_state.returns_percentage = (system_state.total_pnl / system_state.total_capital) * 100.0
    else:
        system_state.returns_percentage = 0.0
        
    session_status = get_market_session_status()
    if session_status == "SUSPENDED":
        # Check specifically for evening shutdown window (23:25 to 23:59)
        now = get_ist_time()
        if now.hour == 23 and now.minute >= 25:
            system_state.log("[EMERGENCY SHUTDOWN] Time is 23:25 or later. Shutting down server immediately.")
            print("[EMERGENCY SHUTDOWN] Time is 23:25 or later. Shutting down server immediately.")
            os._exit(0)

    # If the session is HOLD or SUSPENDED, skip all trade execution and automated checks.
    if session_status in ["HOLD", "SUSPENDED"]:
        await broadcast_system_state()
        return

    # Process pending manual trade triggers
    for trade in system_state.manual_trades:
        if trade.get("status") == "Pending":
            # Bug Fix: Ensure the pending trade's contract symbols match the active streamed ones
            if trade.get("petal_symbol") != system_state.petal_symbol or trade.get("mini_symbol") != system_state.mini_symbol:
                continue
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
 
    # Process Trade Automation Strategy Checks
    if system_state.ta_configs:
        asyncio.create_task(run_trade_automation_checks())

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

def calculate_month_master_live_stats(quotes_map: dict) -> list:
    res = []
    for mapping in system_state.month_master:
        p_tok = mapping.get("petal_token")
        m_tok = mapping.get("mini_token")
        p_sym = mapping.get("petal_symbol")
        m_sym = mapping.get("mini_symbol")
        
        p_q = None
        m_q = None
        if p_tok:
            p_q = quotes_map.get(p_tok)
        if not p_q and p_sym:
            p_q = quotes_map.get(p_sym)
        if not p_q:
            p_q = {}
            
        if m_tok:
            m_q = quotes_map.get(m_tok)
        if not m_q and m_sym:
            m_q = quotes_map.get(m_sym)
        if not m_q:
            m_q = {}
            
        p_ltp = float(p_q.get("ltp") or p_q.get("last_price") or 0.0)
        m_ltp = float(m_q.get("ltp") or m_q.get("last_price") or 0.0)
        
        if p_ltp <= 0:
            if p_sym and p_sym in system_state.symbol_ltps:
                p_ltp = float(system_state.symbol_ltps[p_sym])
            else:
                p_ltp = 7200.0
                
        if m_ltp <= 0:
            if m_sym and m_sym in system_state.symbol_ltps:
                m_ltp = float(system_state.symbol_ltps[m_sym])
            else:
                m_ltp = 71150.0
        
        if p_ltp > 0 and m_ltp > 0:
            mm_spread = (p_ltp * 10.0) - m_ltp
            
            p_depth = p_q.get("depth")
            if not p_depth or not p_depth.get("buy") or not p_depth.get("sell"):
                p_depth = generate_simulated_depth(p_ltp)
                
            m_depth = m_q.get("depth")
            if not m_depth or not m_depth.get("buy") or not m_depth.get("sell"):
                m_depth = generate_simulated_depth(m_ltp)
                
            mm_qty = system_state.trade_quantity
            avg_p_buy = get_depth_average_price(p_depth, "sell", 100 * mm_qty, p_ltp)
            avg_m_sell = get_depth_average_price(m_depth, "buy", mm_qty, m_ltp)
            mm_buy_spread = (avg_p_buy * 10.0) - avg_m_sell
            
            avg_p_sell = get_depth_average_price(p_depth, "buy", 100 * mm_qty, p_ltp)
            avg_m_buy = get_depth_average_price(m_depth, "sell", mm_qty, m_ltp)
            mm_sell_spread = (avg_p_sell * 10.0) - avg_m_buy
            
            # Cache the values dynamically
            if p_sym:
                system_state.symbol_ltps[p_sym] = p_ltp
                system_state.symbol_depths[p_sym] = p_depth
            if p_tok:
                system_state.symbol_ltps[p_tok] = p_ltp
                system_state.symbol_depths[p_tok] = p_depth
            if m_sym:
                system_state.symbol_ltps[m_sym] = m_ltp
                system_state.symbol_depths[m_sym] = m_depth
            if m_tok:
                system_state.symbol_ltps[m_tok] = m_ltp
                system_state.symbol_depths[m_tok] = m_depth

            # Log depth spread for this month master mapping
            record_depth_spread(
                p_sym,
                m_sym,
                avg_p_buy,
                avg_m_sell,
                mm_buy_spread,
                avg_p_sell,
                avg_m_buy,
                mm_sell_spread
            )
            
            res.append({
                "petal_symbol": p_sym,
                "mini_symbol": m_sym,
                "petal_ltp": p_ltp,
                "mini_ltp": m_ltp,
                "spread": round(mm_spread, 2),
                "depth_buy_spread": round(mm_buy_spread, 2),
                "depth_sell_spread": round(mm_sell_spread, 2)
            })
    return res

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
                        # Build token list for active & month master
                        tokens_to_query = {system_state.petal_token, system_state.mini_token}
                        for m in system_state.month_master:
                            if m.get("petal_token"):
                                tokens_to_query.add(m["petal_token"])
                            if m.get("mini_token"):
                                tokens_to_query.add(m["mini_token"])
                        tokens_to_query = [t for t in tokens_to_query if t]

                        loop = asyncio.get_running_loop()
                        market_quotes = await loop.run_in_executor(
                            None,
                            lambda: system_state.smart_connect.getMarketData(
                                mode="FULL",
                                exchangeTokens={"MCX": tokens_to_query}
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
                        quotes_map = {}
                        if market_quotes and market_quotes.get("status") == True:
                            fetched_list = market_quotes.get("data", {}).get("fetched", [])
                            if isinstance(fetched_list, list):
                                for item in fetched_list:
                                    if isinstance(item, dict) and "symbolToken" in item:
                                        tok = item["symbolToken"]
                                        quotes_map[tok] = item
                                        if tok == system_state.petal_token:
                                            petal_quote = item
                                        elif tok == system_state.mini_token:
                                            mini_quote = item
                        
                        system_state.month_master_live = calculate_month_master_live_stats(quotes_map)
 
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
                    # Halt simulation and wait for AngelOne connection
                    await broadcast_system_state()
                    await asyncio.sleep(2.0)
                    continue

            elif system_state.broker == "Dhan":
                # Directly fetch live market data from Angel One to avoid Dhan API rate limits / whitelisting issues
                if not system_state.smart_connect:
                    system_state.api_connected = False
                else:
                    try:
                        # Resolve active contract symbols in Angel One scrip master mapping
                        a1_petal_token = getattr(system_state, "angelone_petal_token", None)
                        a1_mini_token = getattr(system_state, "angelone_mini_token", None)
                        
                        a1_petal_symbol = getattr(system_state, "angelone_petal_symbol", "")
                        a1_mini_symbol = getattr(system_state, "angelone_mini_symbol", "")
                        
                        if not a1_petal_token and a1_petal_symbol:
                            a1_petal_token = system_state.mcx_tokens_cache.get(a1_petal_symbol.upper())
                        if not a1_mini_token and a1_mini_symbol:
                            a1_mini_token = system_state.mcx_tokens_cache.get(a1_mini_symbol.upper())
                            
                        if not a1_petal_token:
                            a1_petal_token = "250000"
                        if not a1_mini_token:
                            a1_mini_token = "250001"
                            
                        # Build token list for active & month master
                        tokens_to_query = {a1_petal_token, a1_mini_token}
                        for m in system_state.month_master:
                            if m.get("petal_token"):
                                tokens_to_query.add(m["petal_token"])
                            if m.get("mini_token"):
                                tokens_to_query.add(m["mini_token"])
                        tokens_to_query = [t for t in tokens_to_query if t]

                        loop = asyncio.get_running_loop()
                        market_quotes = await loop.run_in_executor(
                            None,
                            lambda: system_state.smart_connect.getMarketData(
                                mode="FULL",
                                exchangeTokens={"MCX": tokens_to_query}
                            )
                        )
                        
                        petal_quote = {}
                        mini_quote = {}
                        quotes_map = {}
                        if market_quotes and market_quotes.get("status") == True:
                            fetched_list = market_quotes.get("data", {}).get("fetched", [])
                            if isinstance(fetched_list, list):
                                for item in fetched_list:
                                    if isinstance(item, dict) and "symbolToken" in item:
                                        tok = item["symbolToken"]
                                        quotes_map[tok] = item
                                        if tok == a1_petal_token:
                                            petal_quote = item
                                        elif tok == a1_mini_token:
                                            mini_quote = item
                        
                        system_state.month_master_live = calculate_month_master_live_stats(quotes_map)
                                            
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
                        system_state.log(f"[DHAN FEED ERROR] Failed to fetch quotes via Angel One: {e}")
                        
                if not system_state.api_connected:
                    await broadcast_system_state()
                    await asyncio.sleep(2.0)
                    continue
            elif system_state.broker == "Groww":
                if not system_state.groww_client:
                    system_state.api_connected = False
                else:
                    try:
                        from growwapi import GrowwAPI
                        
                        loop = asyncio.get_running_loop()
                        
                        # Build symbols list for active & month master
                        symbols_to_query = {system_state.petal_symbol, system_state.mini_symbol}
                        for m in system_state.month_master:
                            if m.get("petal_symbol"):
                                symbols_to_query.add(m["petal_symbol"])
                            if m.get("mini_symbol"):
                                symbols_to_query.add(m["mini_symbol"])
                                
                        quotes_map = {}
                        for sym in symbols_to_query:
                            q = await loop.run_in_executor(
                                None,
                                lambda s=sym: system_state.groww_client.get_quote(
                                    exchange=GrowwAPI.EXCHANGE_MCX,
                                    segment=GrowwAPI.SEGMENT_COMMODITY,
                                    trading_symbol=s
                                )
                            )
                            if q:
                                quotes_map[sym] = q
                                
                        petal_quote = quotes_map.get(system_state.petal_symbol, {})
                        mini_quote = quotes_map.get(system_state.mini_symbol, {})
                        
                        system_state.month_master_live = calculate_month_master_live_stats(quotes_map)
                        
                        petal_ltp = float(petal_quote.get("last_price", 0.0) if petal_quote else 0.0)
                        mini_ltp = float(mini_quote.get("last_price", 0.0) if mini_quote else 0.0)
                        
                        if petal_ltp > 0 and mini_ltp > 0:
                            system_state.api_connected = True
                            
                            system_state.gold_petal_volume = int(petal_quote.get("volume", 0))
                            system_state.gold_petal_buy_qty = int(petal_quote.get("total_buy_quantity", 0))
                            system_state.gold_petal_sell_qty = int(petal_quote.get("total_sell_quantity", 0))
                            
                            system_state.gold_mini_volume = int(mini_quote.get("volume", 0))
                            system_state.gold_mini_buy_qty = int(mini_quote.get("total_buy_quantity", 0))
                            system_state.gold_mini_sell_qty = int(mini_quote.get("total_sell_quantity", 0))
                            
                            # Parse depth
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
                        system_state.log(f"[GROWW FEED ERROR] Failed to fetch quotes via Groww: {e}")
                        
                if not system_state.api_connected:
                    await broadcast_system_state()
                    await asyncio.sleep(2.0)
                    continue
            else:
                # Simulation Mode Active
                system_state.api_connected = True
                
                # Build mock quotes_map for month master
                quotes_map = {}
                for mapping in system_state.month_master:
                    p_sym = mapping.get("petal_symbol")
                    m_sym = mapping.get("mini_symbol")
                    p_tok = mapping.get("petal_token")
                    m_tok = mapping.get("mini_token")
                    
                    # Generate distinct prices per month pair using stable hashing
                    hash_val = sum(ord(c) for c in p_sym) % 100
                    offset_petal = hash_val * 2.5
                    offset_mini = hash_val * 25.0
                    
                    p_ltp = round(petal_base + offset_petal + random.uniform(-3, 3), 2)
                    m_ltp = round(mini_base + offset_mini + random.uniform(-30, 30), 2)
                    
                    p_depth = generate_simulated_depth(p_ltp)
                    m_depth = generate_simulated_depth(m_ltp)
                    
                    mock_quote_petal = {
                        "ltp": p_ltp,
                        "depth": p_depth
                    }
                    mock_quote_mini = {
                        "ltp": m_ltp,
                        "depth": m_depth
                    }
                    
                    if p_tok:
                        quotes_map[p_tok] = mock_quote_petal
                    quotes_map[p_sym] = mock_quote_petal
                    
                    if m_tok:
                        quotes_map[m_tok] = mock_quote_mini
                    quotes_map[m_sym] = mock_quote_mini
                    
                system_state.month_master_live = calculate_month_master_live_stats(quotes_map)

            # 2. Live Market Feed Generator / Simulator Ticks
            petal_step = random.uniform(-6.0, 6.0)
            mini_step = random.uniform(-6.0, 6.0)
            petal_base = max(7000.0, min(7500.0, petal_base + petal_step))
            mini_base = max(69000.0, min(74000.0, mini_base + mini_step))
            
            system_state.gold_petal_volume = random.randint(15000, 50000)
            system_state.gold_petal_buy_qty = random.randint(5000, 20000)
            system_state.gold_petal_sell_qty = random.randint(5000, 20000)
            
            system_state.gold_mini_volume = random.randint(25000, 80000)
            system_state.gold_mini_buy_qty = random.randint(10000, 35000)
            system_state.gold_mini_sell_qty = random.randint(10000, 35000)
            
            system_state.petal_depth = generate_simulated_depth(petal_base)
            system_state.mini_depth = generate_simulated_depth(mini_base)
            
            await process_market_data({
                "petal_ltp": round(petal_base, 2),
                "mini_ltp": round(mini_base, 2)
            })
            await asyncio.sleep(1.0)
        except Exception as e:
            logger.error(f"Error in Live Ticker thread: {e}")
            
        await asyncio.sleep(1.0)  # Live MCX ticks every 1 second

# Scrip Finder helper
async def search_active_mcx_tokens():
    import urllib.request
    import json
    import csv
    
    # 1. Angel One Scrip Master Download
    url = "https://margincalculator.angelone.in/OpenAPI_File/files/OpenAPIScripMaster.json"
    system_state.log("[SCRIP FINDER] Fetching active MCX contracts from Angel One Scrip Master...")
    
    try:
        loop = asyncio.get_running_loop()
        def download_and_parse_angelone():
            req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
            with urllib.request.urlopen(req, timeout=10) as response:
                data = json.loads(response.read().decode('utf-8'))
            results = []
            for item in data:
                exch = item.get("exch_seg", "")
                symbol = item.get("symbol", "")
                token = item.get("token", "")
                if exch == "MCX" and symbol and token:
                    sym_u = symbol.upper()
                    system_state.mcx_tokens_cache[sym_u] = token
                    system_state.mcx_tokens_cache[sym_u.removesuffix("FUT")] = token
                    system_state.mcx_tokens_cache[f"{sym_u.removesuffix('FUT')}FUT"] = token
                    system_state.mcx_official_symbols[token] = sym_u
                    if symbol.startswith("GOLDPETAL") or symbol.startswith("GOLDM"):
                        results.append({
                            "symbol": symbol,
                            "token": token,
                            "expiry": item.get("expiry"),
                            "name": item.get("name")
                        })
            return results

        mcx_symbols = await loop.run_in_executor(None, download_and_parse_angelone)
        system_state.log(f"[SCRIP FINDER] Cached MCX tokens. Found {len(mcx_symbols)} active Gold contracts:")
        mcx_symbols.sort(key=lambda x: x["symbol"])
        for res in mcx_symbols[:30]:
            system_state.log(f"-> Symbol: {res['symbol']} | Token: {res['token']} | Expiry: {res['expiry']}")
            
    except Exception as e:
        system_state.log(f"[SCRIP FINDER] Error searching Angel One scrip master: {e}")

    # 2. Dhan Scrip Master Download
    try:
        loop = asyncio.get_running_loop()
        def download_and_parse_dhan():
            dhan_url = "https://images.dhan.co/api-data/api-scrip-master.csv"
            system_state.log("[SCRIP FINDER] Fetching active MCX contracts from Dhan Scrip Master...")
            req = urllib.request.Request(dhan_url, headers={'User-Agent': 'Mozilla/5.0'})
            with urllib.request.urlopen(req, timeout=20) as response:
                lines = (line.decode('utf-8') for line in response)
                reader = csv.DictReader(lines)
                count = 0
                results_dhan = []
                for row in reader:
                    exch = row.get("SEM_EXM_EXCH_ID", "").strip()
                    segment = row.get("SEM_SEGMENT", "").strip()
                    symbol = row.get("SEM_TRADING_SYMBOL", "").strip()
                    token = row.get("SEM_SMST_SECURITY_ID", "").strip()
                    if (exch == "MCX" or segment == "M") and symbol and token:
                        sym_u = symbol.upper()
                        system_state.dhan_tokens_cache[sym_u] = token
                        system_state.dhan_tokens_cache[sym_u.removesuffix("FUT")] = token
                        system_state.dhan_tokens_cache[f"{sym_u.removesuffix('FUT')}FUT"] = token
                        system_state.dhan_official_symbols[token] = sym_u
                        if sym_u.startswith("GOLDPETAL") or sym_u.startswith("GOLDM"):
                            results_dhan.append({
                                "symbol": sym_u,
                                "token": token
                            })
                        count += 1
                return count, results_dhan

        dhan_count, dhan_results = await loop.run_in_executor(None, download_and_parse_dhan)
        system_state.log(f"[SCRIP FINDER] Cached {dhan_count} Dhan MCX symbols successfully. Gold contracts found:")
        dhan_results.sort(key=lambda x: x["symbol"])
        for res in dhan_results[:40]:
            system_state.log(f"-> Dhan Symbol: {res['symbol']} | Token: {res['token']}")
    except Exception as e:
        system_state.log(f"[SCRIP FINDER] Error searching Dhan scrip master: {e}")

    # Always try to initialize AngelOne client if credentials are configured (required for live feed fallback)
    if system_state.client_id and system_state.password:
        system_state.init_angelone_client()

    # Initialize active broker client
    if system_state.broker == "Dhan":
        system_state.init_dhan_client()
    elif system_state.broker == "Groww":
        system_state.init_groww_client()

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
    cookie_token = websocket.cookies.get("session_token")
    
    is_valid = False
    if token and token == os.getenv("AUTH_TOKEN", "secret_arbitrage_token_2026"):
        is_valid = True
    elif cookie_token and verify_session_token(cookie_token):
        is_valid = True
        
    if not is_valid:
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
            
            # Trade Automation Broadcast fields
            "ta_configs": system_state.ta_configs,
            "ta_trades": system_state.ta_trades,
            
            "logs": system_state.logs
        })
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        manager.disconnect(websocket)

# REST endpoints for Authentication
class LoginPayload(BaseModel):
    username: str
    password: str
    otp: str

@app.post("/api/login")
async def api_login(payload: LoginPayload, request: Request):
    ip_address = request.client.host if request.client else "unknown"
    
    # Check rate limiting
    if is_login_rate_limited(ip_address):
        raise HTTPException(status_code=429, detail="Too many failed login attempts. Locked out for 5 minutes.")
        
    expected_username = os.getenv("ADMIN_USERNAME", "admin")
    expected_password = os.getenv("ADMIN_PASSWORD", "goldarbitrage2026")
    
    if payload.username != expected_username or payload.password != expected_password:
        record_failed_login(ip_address)
        raise HTTPException(status_code=401, detail="Invalid username or password")
        
    # Verify TOTP code (if secret is configured)
    totp_secret = os.getenv("TOTP_SECRET", "").strip()
    if totp_secret:
        # Some users might have spaces in their key, clean it
        totp_secret_clean = "".join(totp_secret.split())
        try:
            totp = pyotp.TOTP(totp_secret_clean)
            if not totp.verify(payload.otp):
                record_failed_login(ip_address)
                raise HTTPException(status_code=401, detail="Invalid 2FA OTP code")
        except Exception as e:
            logger.error(f"Error checking TOTP OTP code: {e}")
            raise HTTPException(status_code=500, detail="Internal server error verifying 2FA OTP")
            
    # Success
    record_successful_login(ip_address)
    token = sign_session_token(payload.username)
    response = JSONResponse(content={"status": "success", "message": "Logged in successfully"})
    
    cookie_secure = os.getenv("COOKIE_SECURE", "false").lower() == "true"
    response.set_cookie(
        key="session_token",
        value=token,
        httponly=True,
        samesite="strict",
        max_age=604800,  # 7 days
        secure=cookie_secure
    )
    return response

@app.post("/api/logout")
async def api_logout():
    response = JSONResponse(content={"status": "success", "message": "Logged out successfully"})
    response.delete_cookie(key="session_token")
    return response

# REST manual position entry endpoint
class EntryPayload(BaseModel):
    direction: str
    trigger_diff: float = None
    quantity: int = None

@app.post("/api/entry")
async def api_entry(payload: EntryPayload, token: str = None, authorization: str = Header(None)):
    verify_token(token, authorization)
    
    session_status = get_market_session_status()
    if session_status == "HOLD":
        raise HTTPException(status_code=400, detail="Trading is suspended during morning hold (09:00 - 09:03).")
    elif session_status == "SUSPENDED":
        raise HTTPException(status_code=400, detail="Trading is suspended after market close (23:25 - 09:00).")

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
            "petal_symbol": system_state.petal_symbol,
            "mini_symbol": system_state.mini_symbol,
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
                
    # 3. Square off all active Trade Automation trades
    for trade in list(system_state.ta_trades):
        if trade.get("status") == "Open":
            mapping = None
            for m in system_state.month_master:
                if m["petal_symbol"] == trade["petal_symbol"] and m["mini_symbol"] == trade["mini_symbol"]:
                    mapping = m
                    break
            if not mapping:
                mapping = {
                    "petal_symbol": trade["petal_symbol"],
                    "petal_token": "",
                    "mini_symbol": trade["mini_symbol"],
                    "mini_token": ""
                }
            # Find config to get paper mode
            paper_mode = True
            for config in system_state.ta_configs:
                idx = config.get("month_idx", -1)
                if 0 <= idx < len(system_state.month_master):
                    m = system_state.month_master[idx]
                    if m["petal_symbol"] == trade["petal_symbol"] and m["mini_symbol"] == trade["mini_symbol"]:
                        paper_mode = config.get("paper_mode", True)
                        break
            system_state.log(f"[KILL SWITCH] Squaring off Trade Automation trade ID {trade['id']}...")
            await run_ta_exit(trade, mapping, paper_mode)
            
    # Also disable all Trade Automation configs
    for config in system_state.ta_configs:
        config["enabled"] = False
                
    system_state.save_manual_trades()
    system_state.save_ta_trades()
    system_state.save_ta_configs()
    system_state.save_trade_history()
    system_state.system_status = "Halted"
    await broadcast_system_state()
    return {"status": "SUCCESS", "message": "Positions cleared and system halted."}

# REST Strategy parameters update form submission endpoint
class UpdateParamsPayload(BaseModel):
    entry_threshold: float = 1000.0
    target_threshold: float = 1150.0
    stop_loss_threshold: float = 600.0
    total_capital: float = 500000.0
    paper_trading_mode: bool = True
    trade_quantity: int = 1
    auto_target_enabled: bool = False
    auto_target_val: float = 5000.0
    auto_sl_enabled: bool = False
    auto_sl_val: float = -3000.0
    auto_square_off_enabled: bool = False
    auto_square_off_time: str = "23:30"
    auto_trading_enabled: bool = False
    spread_buffer: float = 0.0
    auto_contraction_enabled: bool = False
    auto_spread_exit_enabled: bool = False
    broker: str = "AngelOne"
    api_key: str = ""
    client_id: str = ""
    password: str = ""
    totp_secret: str = ""
    petal_symbol: str = ""
    petal_token: str = ""
    mini_symbol: str = ""
    mini_token: str = ""
    groww_api_key: str = ""
    groww_client_id: str = ""
    groww_secret: str = ""
    groww_petal_symbol: str = ""
    groww_mini_symbol: str = ""
    dhan_client_id: str = ""
    dhan_access_token: str = ""
    dhan_petal_symbol: str = ""
    dhan_petal_token: str = ""
    dhan_mini_symbol: str = ""
    dhan_mini_token: str = ""
    upstox_client_id: str = ""
    upstox_secret: str = ""
    upstox_access_token: str = ""
    upstox_petal_symbol: str = ""
    upstox_mini_symbol: str = ""

class MonthMasterMapping(BaseModel):
    petal_symbol: str = ""
    petal_token: str = ""
    mini_symbol: str = ""
    mini_token: str = ""

class MonthMasterPayload(BaseModel):
    mappings: List[MonthMasterMapping]

@app.get("/api/month-master")
async def api_get_month_master(token: str = None, authorization: str = Header(None)):
    verify_token(token, authorization)
    return {"status": "SUCCESS", "mappings": system_state.month_master}

@app.post("/api/month-master")
async def api_post_month_master(payload: MonthMasterPayload, token: str = None, authorization: str = Header(None)):
    verify_token(token, authorization)
    resolved_mappings = []
    for m in payload.mappings:
        item = m.dict()
        p_sym = item.get("petal_symbol", "").strip()
        m_sym = item.get("mini_symbol", "").strip()
        p_tok = item.get("petal_token", "").strip()
        m_tok = item.get("mini_token", "").strip()
        
        if not p_sym and not m_sym:
            continue
            
        if not p_tok and p_sym:
            if system_state.broker == "Dhan":
                p_tok = system_state.resolve_dhan_token(p_sym)
            else:
                p_tok = system_state.resolve_scrip_token_via_api(p_sym)
        if not m_tok and m_sym:
            if system_state.broker == "Dhan":
                m_tok = system_state.resolve_dhan_token(m_sym)
            else:
                m_tok = system_state.resolve_scrip_token_via_api(m_sym)
                
        item["petal_symbol"] = p_sym
        item["petal_token"] = p_tok
        item["mini_symbol"] = m_sym
        item["mini_token"] = m_tok
        resolved_mappings.append(item)
        
    system_state.month_master = resolved_mappings
    system_state.save_month_master()
    system_state.log(f"[MONTH MASTER] Updated month master mappings ({len(system_state.month_master)} pair(s) active).")
    await broadcast_system_state()
    return {"status": "SUCCESS", "message": "Month Master mappings updated successfully.", "mappings": system_state.month_master}

class TAConfigItem(BaseModel):
    month_idx: int
    entry_diff: float
    averaging_step: float
    exit_gap: float
    quantity: int
    direction: str
    paper_mode: bool
    enabled: bool

class TAConfigPayload(BaseModel):
    configs: List[TAConfigItem]

@app.post("/api/ta-config")
async def api_post_ta_config(payload: TAConfigPayload, token: str = None, authorization: str = Header(None)):
    verify_token(token, authorization)
    
    session_status = get_market_session_status()
    if session_status == "SUSPENDED":
        has_enabled = any(c.dict().get("enabled") for c in payload.configs)
        if has_enabled:
            raise HTTPException(status_code=400, detail="Cannot enable Trade Automation configs after market close (23:25 - 09:00).")

    system_state.ta_configs = [c.dict() for c in payload.configs]
    system_state.save_ta_configs()
    system_state.log(f"Trade Automation configs updated: {len(system_state.ta_configs)} active instance(s).")
    await broadcast_system_state()
    return {"status": "SUCCESS", "message": "Trade Automation configurations updated successfully."}

class TAExitTradePayload(BaseModel):
    trade_id: int

@app.post("/api/ta-exit-trade")
async def api_ta_exit_trade(payload: TAExitTradePayload, token: str = None, authorization: str = Header(None)):
    verify_token(token, authorization)
    
    trade = None
    for t in system_state.ta_trades:
        if t["id"] == payload.trade_id:
            trade = t
            break
            
    if not trade:
        raise HTTPException(status_code=404, detail="Trade Automation trade not found.")
        
    if trade["status"] != "Open":
        raise HTTPException(status_code=400, detail="Trade is not active.")
        
    # Find month mapping
    mapping = None
    for m in system_state.month_master:
        if m["petal_symbol"] == trade["petal_symbol"] and m["mini_symbol"] == trade["mini_symbol"]:
            mapping = m
            break
            
    if not mapping:
        mapping = {
            "petal_symbol": trade["petal_symbol"],
            "petal_token": "",
            "mini_symbol": trade["mini_symbol"],
            "mini_token": ""
        }
        
    # Find config for the trade's month mapping to get the paper mode
    paper_mode = True
    for config in system_state.ta_configs:
        idx = config.get("month_idx", -1)
        if 0 <= idx < len(system_state.month_master):
            m = system_state.month_master[idx]
            if m["petal_symbol"] == trade["petal_symbol"] and m["mini_symbol"] == trade["mini_symbol"]:
                paper_mode = config.get("paper_mode", True)
                break
                
    system_state.log(f"[TA MANUAL EXIT] Squaring off Trade Automation trade ID {trade['id']}...")
    await run_ta_exit(trade, mapping, paper_mode)
    return {"status": "SUCCESS", "message": "Trade Automation trade closed successfully."}

class TADismissTradePayload(BaseModel):
    trade_id: int

@app.post("/api/ta-dismiss-trade")
async def api_ta_dismiss_trade(payload: TADismissTradePayload, token: str = None, authorization: str = Header(None)):
    verify_token(token, authorization)
    system_state.ta_trades = [t for t in system_state.ta_trades if t["id"] != payload.trade_id]
    system_state.save_ta_trades()
    await broadcast_system_state()
    return {"status": "SUCCESS", "message": "Trade dismissed successfully."}

@app.post("/api/update-rules")
async def api_update_rules(payload: UpdateParamsPayload, token: str = None, authorization: str = Header(None)):
    verify_token(token, authorization)
    
    session_status = get_market_session_status()
    if session_status == "SUSPENDED":
        if payload.auto_trading_enabled:
            raise HTTPException(status_code=400, detail="Cannot enable Auto Trading after market close (23:25 - 09:00).")

    system_state.entry_threshold = payload.entry_threshold
    system_state.target_threshold = payload.target_threshold
    system_state.sl_threshold = payload.stop_loss_threshold
    system_state.total_capital = payload.total_capital
    system_state.trade_quantity = max(1, payload.trade_quantity)
    
    system_state.spread_buffer = payload.spread_buffer
    system_state.auto_contraction_enabled = payload.auto_contraction_enabled
    system_state.auto_spread_exit_enabled = payload.auto_spread_exit_enabled
    
    system_state.paper_trading_mode = payload.paper_trading_mode
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
    system_state.save_angel_master()
    
    system_state.groww_api_key = payload.groww_api_key
    system_state.groww_client_id = payload.groww_client_id
    system_state.groww_secret = payload.groww_secret
    if payload.groww_petal_symbol:
        system_state.groww_petal_symbol = payload.groww_petal_symbol
    if payload.groww_mini_symbol:
        system_state.groww_mini_symbol = payload.groww_mini_symbol

    system_state.dhan_client_id = payload.dhan_client_id
    system_state.dhan_access_token = payload.dhan_access_token
    if payload.dhan_petal_symbol:
        system_state.dhan_petal_symbol = payload.dhan_petal_symbol
    if payload.dhan_petal_token:
        system_state.dhan_petal_token = payload.dhan_petal_token
    if payload.dhan_mini_symbol:
        system_state.dhan_mini_symbol = payload.dhan_mini_symbol
    if payload.dhan_mini_token:
        system_state.dhan_mini_token = payload.dhan_mini_token

    system_state.upstox_client_id = payload.upstox_client_id
    system_state.upstox_secret = payload.upstox_secret
    system_state.upstox_access_token = payload.upstox_access_token
    system_state.upstox_petal_symbol = payload.upstox_petal_symbol
    system_state.upstox_mini_symbol = payload.upstox_mini_symbol

    # Always save Angel One specific symbols and resolve/store tokens
    system_state.angelone_petal_symbol = payload.petal_symbol
    system_state.angelone_mini_symbol = payload.mini_symbol
    if payload.petal_token:
        system_state.angelone_petal_token = payload.petal_token
    else:
        resolved_tok = system_state.resolve_scrip_token_via_api(payload.petal_symbol)
        if resolved_tok:
            system_state.angelone_petal_token = resolved_tok
        else:
            system_state.angelone_petal_token = payload.petal_token

    if payload.mini_token:
        system_state.angelone_mini_token = payload.mini_token
    else:
        resolved_tok = system_state.resolve_scrip_token_via_api(payload.mini_symbol)
        if resolved_tok:
            system_state.angelone_mini_token = resolved_tok
        else:
            system_state.angelone_mini_token = payload.mini_token

    if system_state.broker == "Dhan":
        system_state.petal_symbol = system_state.dhan_petal_symbol
        system_state.mini_symbol = system_state.dhan_mini_symbol
        if system_state.dhan_petal_token:
            system_state.petal_token = system_state.dhan_petal_token
        else:
            system_state.petal_token = system_state.resolve_dhan_token(system_state.petal_symbol)
            
        if system_state.dhan_mini_token:
            system_state.mini_token = system_state.dhan_mini_token
        else:
            system_state.mini_token = system_state.resolve_dhan_token(system_state.mini_symbol)
            
        system_state.log(f"[DHAN RESOLVE] Leg 1: {system_state.petal_symbol} -> Token: {system_state.petal_token}, Leg 2: {system_state.mini_symbol} -> Token: {system_state.mini_token}")
    elif system_state.broker == "Groww":
        system_state.petal_symbol = system_state.groww_petal_symbol
        system_state.mini_symbol = system_state.groww_mini_symbol
        system_state.petal_token = ""
        system_state.mini_token = ""
    elif system_state.broker == "Upstox":
        system_state.petal_symbol = system_state.upstox_petal_symbol
        system_state.mini_symbol = system_state.upstox_mini_symbol
        system_state.petal_token = system_state.upstox_petal_symbol
        system_state.mini_token = system_state.upstox_mini_symbol
    else:
        # Default to Angel One (or Simulation)
        system_state.petal_symbol = system_state.angelone_petal_symbol
        system_state.petal_token = system_state.angelone_petal_token
        system_state.mini_symbol = system_state.angelone_mini_symbol
        system_state.mini_token = system_state.angelone_mini_token
    
    # Trigger dynamic SDK connection if client updates keys
    if system_state.client_id and system_state.password:
        system_state.init_angelone_client()
        
    if system_state.broker == "Dhan":
        system_state.init_dhan_client()
    elif system_state.broker == "Groww":
        system_state.init_groww_client()
    
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

# REST CSV Depth Spread History exporter endpoint
@app.get("/api/export-depth-spread")
async def api_export_depth_spread(token: str = None, authorization: str = Header(None)):
    verify_token(token, authorization)
    
    csv_file = "depth_spread_history.csv"
    if not os.path.exists(csv_file):
        # Return empty file with headers
        csv_buffer = StringIO()
        writer = csv.writer(csv_buffer)
        writer.writerow([
            "Month",
            "Timestamp", 
            "Petal_Symbol",
            "Mini_Symbol",
            "Petal_Ask_Avg", 
            "Mini_Bid_Avg", 
            "Depth_Buy_Spread", 
            "Petal_Bid_Avg", 
            "Mini_Ask_Avg", 
            "Depth_Sell_Spread"
        ])
        csv_buffer.seek(0)
        headers = {"Content-Disposition": "attachment; filename=depth_spread_history.csv"}
        return StreamingResponse(iter([csv_buffer.getvalue()]), media_type="text/csv", headers=headers)
        
    try:
        with open(csv_file, "r", encoding="utf-8") as f:
            content = f.read()
        headers = {"Content-Disposition": "attachment; filename=depth_spread_history.csv"}
        return StreamingResponse(iter([content]), media_type="text/csv", headers=headers)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# REST CSV Active & Pending Manual Trades exporter endpoint
@app.get("/api/export-manual-csv")
async def api_export_manual_csv(token: str = None, authorization: str = Header(None)):
    verify_token(token, authorization)
    
    csv_buffer = StringIO()
    writer = csv.writer(csv_buffer)
    
    # Headers
    writer.writerow([
        "Manual Trade ID", "Entry Date", "Leg 1 Symbol", "Leg 2 Symbol", "Direction", "Status", "Quantity", "Trigger Diff Target", 
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
            trade.get("petal_symbol", system_state.petal_symbol),
            trade.get("mini_symbol", system_state.mini_symbol),
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
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

@app.get("/", response_class=HTMLResponse)
async def get_dashboard():
    try:
        file_path = os.path.join(BASE_DIR, "index.html")
        with open(file_path, "r", encoding="utf-8") as f:
            return HTMLResponse(content=f.read())
    except FileNotFoundError:
        return HTMLResponse(content="<h3>index.html not found</h3>", status_code=404)

@app.get("/style.css")
async def get_style():
    return FileResponse(os.path.join(BASE_DIR, "style.css"), media_type="text/css")

@app.get("/script.js")
async def get_script():
    return FileResponse(os.path.join(BASE_DIR, "script.js"), media_type="application/javascript")

if __name__ == "__main__":
    host = os.getenv("HOST", "127.0.0.1")
    port = int(os.getenv("PORT", 7890))
    logger.info(f"Starting server on http://{host}:{port}")
    uvicorn.run("main:app", host=host, port=port, log_level="info")
