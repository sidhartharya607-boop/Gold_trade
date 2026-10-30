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
from typing import List, Dict, Optional, Union, Any

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



BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Load environment variables with explicit base directory path and override
load_dotenv(os.path.join(BASE_DIR, ".env"), override=True)
load_dotenv(override=True)

def get_data_filepath(filename: str) -> str:
    path_in_base = os.path.join(BASE_DIR, filename)
    if os.path.exists(path_in_base):
        return path_in_base
    if os.path.exists(filename):
        cwd_file = os.path.abspath(filename)
        try:
            if cwd_file != path_in_base and os.path.exists(cwd_file) and os.path.getsize(cwd_file) > 0:
                with open(cwd_file, "r", encoding="utf-8") as f_in:
                    content = f_in.read()
                with open(path_in_base, "w", encoding="utf-8") as f_out:
                    f_out.write(content)
                return path_in_base
        except Exception:
            pass
        return cwd_file
    return path_in_base

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
      "HOLD" - if within scheduled volatility blackout windows (no trading/movement allowed):
               - 09:00:00 to 09:02:59 IST (Morning market open)
               - 09:13:00 to 09:16:59 IST (Opening settlement)
               - 13:27:00 to 13:32:59 IST (1:27 PM to 1:33 PM London open)
               - 16:58:00 to 17:04:59 IST (4:58 PM to 5:05 PM Evening session)
               - 17:58:00 to 18:09:59 IST (5:58 PM to 6:10 PM US open)
      "SUSPENDED" - if evening/night suspension time (23:27:00 to 08:59:59 IST)
      "OPEN" - otherwise (trading allowed)
    """
    now = get_ist_time()
    h = now.hour
    m = now.minute
    
    # 1. Morning open hold: 09:00 to 09:02:59 (inclusive of 9:00, 9:01, 9:02)
    if h == 9 and 0 <= m < 3:
        return "HOLD"
        
    # 2. Morning settlement hold: 09:13 to 09:16:59 (inclusive of 9:13, 9:14, 9:15, 9:16)
    if h == 9 and 13 <= m < 17:
        return "HOLD"
        
    # 3. London open hold: 13:27 to 13:32:59 (1:27 PM to 1:33 PM)
    if h == 13 and 27 <= m < 33:
        return "HOLD"
        
    # 4. Evening hold: 16:58 to 17:04:59 (4:58 PM to 5:05 PM)
    if (h == 16 and m >= 58) or (h == 17 and m < 5):
        return "HOLD"
        
    # 5. US open hold: 17:58 to 18:09:59 (5:58 PM to 6:10 PM)
    if (h == 17 and m >= 58) or (h == 18 and m < 10):
        return "HOLD"
        
    # 6. Evening / Overnight suspension: 23:27:00 to 08:59:59 next day
    if (h == 23 and m >= 27) or (h < 9):
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

def get_gold_contract_specs(symbol: str) -> dict:
    sym_u = (symbol or "").upper()
    if sym_u.startswith("GOLDPETAL") or "PETAL" in sym_u:
        return {
            "type": "GOLDPETAL",
            "lot_size_grams": 1,
            "lots_per_unit_mini": 100,      # 100 lots of Petal per 1 GOLDM (100g)
            "price_scale_to_10g": 10.0,     # Quote is per 1g -> x10 for 10g scale
            "api_lot_multiplier": 1,        # 1 lot per unit in API (passes 100)
            "pnl_multiplier": 100.0,
            "bid_ask_limit": 15.0
        }
    elif sym_u.startswith("GOLDTEN") or sym_u.startswith("GOLD10") or "TEN" in sym_u:
        return {
            "type": "GOLDTEN",
            "lot_size_grams": 10,
            "lots_per_unit_mini": 10,       # 10 lots of Gold Ten per 1 GOLDM (100g)
            "price_scale_to_10g": 1.0,      # Quote is per 10g -> x1.0
            "api_lot_multiplier": 1,        # 1 lot per unit in API (passes 10)
            "pnl_multiplier": 10.0,
            "bid_ask_limit": 150.0
        }
    elif sym_u.startswith("GOLDGUINEA") or "GUINEA" in sym_u:
        return {
            "type": "GOLDGUINEA",
            "lot_size_grams": 8,
            "lots_per_unit_mini": 12,       # 12 lots (96g) per 1 GOLDM (100g)
            "price_scale_to_10g": 1.25,     # Quote is per 8g -> (P / 8) * 10 = P * 1.25
            "api_lot_multiplier": 1,        # 1 lot per unit in API (passes 12)
            "pnl_multiplier": 12.0,
            "bid_ask_limit": 120.0
        }
    elif sym_u.startswith("GOLDM") or "MINI" in sym_u:
        return {
            "type": "GOLDM",
            "lot_size_grams": 100,
            "lots_per_unit_mini": 1,        # 1 lot per unit (100g)
            "price_scale_to_10g": 1.0,      # Quote is per 10g -> x1.0
            "api_lot_multiplier": 100,      # Angel One requires 100 in quantity (lot size in grams)
            "pnl_multiplier": 10.0,
            "bid_ask_limit": 150.0
        }
    elif sym_u.startswith("GOLD"):
        return {
            "type": "GOLD",
            "lot_size_grams": 1000,
            "lots_per_unit_mini": 1,
            "price_scale_to_10g": 1.0,
            "api_lot_multiplier": 1,
            "pnl_multiplier": 100.0,
            "bid_ask_limit": 150.0
        }
    return {
        "type": "GOLDPETAL",
        "lot_size_grams": 1,
        "lots_per_unit_mini": 100,
        "price_scale_to_10g": 10.0,
        "api_lot_multiplier": 1,
        "pnl_multiplier": 100.0,
        "bid_ask_limit": 15.0
    }

# =====================================================================
# ⚖️ DYNAMIC ARBITRAGE CHARGES & TAX CALCULATOR (PER GRAM BASIS)
# =====================================================================
def calculate_trade_pair_charges(petal_sym: str, mini_sym: str, ltp: float = 72000.0) -> dict:
    """
    Dynamically calculates exact MCX exchange turnover charges, CTT, SEBI, GST, 
    Stamp Duty & Brokerage based on the exact grams traded for this specific pair.
    - 100g (Petal vs Mini): ~Rs. 339
    - 96g (Guinea 12 lots vs Mini): ~Rs. 329
    - 10g (Petal 10 lots vs Ten 1 lot): ~Rs. 119
    - 8g (Guinea 1 lot vs Petal 8 lots): ~Rs. 114
    - 40g (Guinea 5 lots vs Ten 4 lots): ~Rs. 200
    """
    spec_p = get_gold_contract_specs(petal_sym)
    spec_m = get_gold_contract_specs(mini_sym)
    p_type = spec_p.get("type", "")
    m_type = spec_m.get("type", "")

    # Determine traded weight in grams for 1 unit arbitrage combination
    if ("M" in m_type or "MINI" in m_type or spec_m.get("lot_size_grams", 100) == 100) and p_type == "GOLDGUINEA":
        # 12 Guinea lots (96g) vs 1 Mini lot (100g)
        leg1_grams = 96.0
        leg2_grams = 100.0
        grams = 96.0  # Effective matched arbitrage weight
    elif ("M" in m_type or "MINI" in m_type or spec_m.get("lot_size_grams", 100) == 100) and p_type != "GOLDGUINEA":
        # 100 Petal lots (100g) vs 1 Mini lot (100g)
        leg1_grams = 100.0
        leg2_grams = 100.0
        grams = 100.0
    elif (p_type == "GOLDGUINEA" and (m_type == "GOLDPETAL" or spec_m.get("lot_size_grams") == 1)):
        # 1 Guinea (8g) vs 8 Petals (8g)
        leg1_grams = 8.0
        leg2_grams = 8.0
        grams = 8.0
    elif ((p_type == "GOLDPETAL" or spec_p.get("lot_size_grams") == 1) and m_type == "GOLDGUINEA"):
        # 8 Petals (8g) vs 1 Guinea (8g)
        leg1_grams = 8.0
        leg2_grams = 8.0
        grams = 8.0
    elif (p_type == "GOLDPETAL" and (m_type == "GOLDTEN" or spec_m.get("lot_size_grams") == 10)):
        # 10 Petals (10g) vs 1 Ten (10g)
        leg1_grams = 10.0
        leg2_grams = 10.0
        grams = 10.0
    elif ((p_type == "GOLDTEN" or spec_p.get("lot_size_grams") == 10) and m_type == "GOLDPETAL"):
        leg1_grams = 10.0
        leg2_grams = 10.0
        grams = 10.0
    elif (p_type == "GOLDGUINEA" and (m_type == "GOLDTEN" or spec_m.get("lot_size_grams") == 10)):
        # 5 Guineas (40g) vs 4 Tens (40g)
        leg1_grams = 40.0
        leg2_grams = 40.0
        grams = 40.0
    elif ((p_type == "GOLDTEN" or spec_p.get("lot_size_grams") == 10) and m_type == "GOLDGUINEA"):
        leg1_grams = 40.0
        leg2_grams = 40.0
        grams = 40.0
    else:
        grams = float(max(spec_p.get("lot_size_grams", 10), spec_m.get("lot_size_grams", 10)))
        if grams < 1.0: grams = 10.0
        leg1_grams = grams
        leg2_grams = grams

    # Normalized MCX price per 1 gram (gold price ~72,000 per 10g => ~7,200/g)
    price_per_gram = (ltp / 10.0) if ltp > 10000.0 else (ltp if ltp > 1000.0 else 7200.0)
    
    # 2 round-trip orders per leg (entry and exit)
    leg1_turnover = leg1_grams * price_per_gram * 2.0
    leg2_turnover = leg2_grams * price_per_gram * 2.0
    total_turnover = leg1_turnover + leg2_turnover
    sell_turnover = (leg1_grams + leg2_grams) * price_per_gram
    buy_turnover = (leg1_grams + leg2_grams) * price_per_gram

    brokerage = 20.0 * 4.0               # 4 orders total = Rs 80
    exchange_charges = 0.000021 * total_turnover
    ctt = 0.0001 * sell_turnover         # 0.01% on sell side
    sebi = 0.0000001 * total_turnover    # Rs 10 per crore
    stamp = 0.00002 * buy_turnover       # 0.002% on buy side
    gst = 0.18 * (brokerage + exchange_charges)

    total_charges = round(brokerage + exchange_charges + ctt + sebi + stamp + gst, 2)
    # 8 points slippage cushion on 10g scale:
    slippage = round((grams / 10.0) * 8.0, 2)
    multiplier_10g_scale = round(grams / 10.0, 2)  # PnL multiplier per point of 10g normalized spread

    return {
        "grams": grams,
        "total_charges": total_charges,
        "slippage_cushion": slippage,
        "multiplier_10g_scale": multiplier_10g_scale
    }

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
        self.last_global_order_time = 0.0
        self.order_execution_in_progress = False

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
        self.mcx_lotsize_cache = {}
        self.mcx_expiry_cache = {}  # symbol -> expiry string from Angel One scrip master (e.g. "30JAN2027")

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
        self.last_ta_exit_time = 0.0
        
        self.load_ta_trades()
        self.load_ta_configs()

        # Trade Automation with Lots State
        self.ta_lots_configs = []
        self.ta_lots_trades = []
        self.ta_lots_execution_in_progress = False
        self.last_ta_lots_exit_time = 0.0

        self.load_ta_lots_trades()
        self.load_ta_lots_configs()

        # Daily Spread Tracking State (for Month Master Export)
        self.daily_spread_stats = {}
        self.load_daily_spread_stats()

        # Manual BOT State (Semi-Automated Multi-Order Slicer & Multi-Pair Tasks)
        self.manual_bot = {
            "active": False,
            "direction": "Expansion",
            "petal_symbol": "",
            "mini_symbol": "",
            "trigger_diff": 1400.0,
            "diff_gap": 20.0,
            "total_orders": 5,
            "filled_orders": 0,
            "quantity": 1,
            "order_delay": 10.0,
            "last_order_time": 0.0,
            "status_message": "Idle",
            "why_waiting": "Bot not started",
            "trades": []
        }
        self.manual_bots = []
        self.load_manual_bot()

        self.load_angel_master()
        self.load_rules()

        # AI Arbitrage Quantitative Engine State
        self.ai_divergence_threshold = 80.0  # Dynamic threshold (default 80.0 pts, user configurable)
        self.ai_opportunities_log = []
        self.load_ai_opportunities()
        
    def load_rules(self):
        try:
            filepath = get_data_filepath("rules_config.json")
            if os.path.exists(filepath):
                with open(filepath, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    if "entry_threshold" in data: self.entry_threshold = float(data["entry_threshold"])
                    if "target_threshold" in data: self.target_threshold = float(data["target_threshold"])
                    if "sl_threshold" in data: self.sl_threshold = float(data["sl_threshold"])
                    if "total_capital" in data: self.total_capital = float(data["total_capital"])
                    if "trade_quantity" in data: self.trade_quantity = max(1, int(data["trade_quantity"]))
                    if "spread_buffer" in data: self.spread_buffer = float(data["spread_buffer"])
                    if "auto_contraction_enabled" in data: self.auto_contraction_enabled = bool(data["auto_contraction_enabled"])
                    if "auto_spread_exit_enabled" in data: self.auto_spread_exit_enabled = bool(data["auto_spread_exit_enabled"])
                    if "paper_trading_mode" in data: self.paper_trading_mode = bool(data["paper_trading_mode"])
                    if "auto_trading_enabled" in data: self.auto_trading_enabled = bool(data["auto_trading_enabled"])
                    if "auto_target_enabled" in data: self.auto_target_enabled = bool(data["auto_target_enabled"])
                    if "auto_target_val" in data: self.auto_target_val = float(data["auto_target_val"])
                    if "auto_sl_enabled" in data: self.auto_sl_enabled = bool(data["auto_sl_enabled"])
                    if "auto_sl_val" in data: self.auto_sl_val = float(data["auto_sl_val"])
                    if "auto_square_off_enabled" in data: self.auto_square_off_enabled = bool(data["auto_square_off_enabled"])
                    if "auto_square_off_time" in data: self.auto_square_off_time = str(data["auto_square_off_time"])
                    if "broker" in data: self.broker = str(data["broker"])
                    if "groww_api_key" in data: self.groww_api_key = str(data["groww_api_key"])
                    if "groww_client_id" in data: self.groww_client_id = str(data["groww_client_id"])
                    if "groww_secret" in data: self.groww_secret = str(data["groww_secret"])
                    if "groww_petal_symbol" in data: self.groww_petal_symbol = str(data["groww_petal_symbol"])
                    if "groww_mini_symbol" in data: self.groww_mini_symbol = str(data["groww_mini_symbol"])
                    if "dhan_client_id" in data: self.dhan_client_id = str(data["dhan_client_id"])
                    if "dhan_access_token" in data: self.dhan_access_token = str(data["dhan_access_token"])
                    if "dhan_petal_symbol" in data: self.dhan_petal_symbol = str(data["dhan_petal_symbol"])
                    if "dhan_petal_token" in data: self.dhan_petal_token = str(data["dhan_petal_token"])
                    if "dhan_mini_symbol" in data: self.dhan_mini_symbol = str(data["dhan_mini_symbol"])
                    if "dhan_mini_token" in data: self.dhan_mini_token = str(data["dhan_mini_token"])
                    if "upstox_client_id" in data: self.upstox_client_id = str(data["upstox_client_id"])
                    if "upstox_secret" in data: self.upstox_secret = str(data["upstox_secret"])
                    if "upstox_access_token" in data: self.upstox_access_token = str(data["upstox_access_token"])
                    if "upstox_petal_symbol" in data: self.upstox_petal_symbol = str(data["upstox_petal_symbol"])
                    if "upstox_mini_symbol" in data: self.upstox_mini_symbol = str(data["upstox_mini_symbol"])
                self.log(f"[PERSISTENCE] Loaded strategy rules from {filepath}.")
        except Exception as e:
            self.log(f"[PERSISTENCE ERROR] Failed to load rules config: {e}")

    def save_rules(self):
        try:
            data = {
                "entry_threshold": self.entry_threshold,
                "target_threshold": self.target_threshold,
                "sl_threshold": self.sl_threshold,
                "total_capital": self.total_capital,
                "trade_quantity": self.trade_quantity,
                "spread_buffer": self.spread_buffer,
                "auto_contraction_enabled": self.auto_contraction_enabled,
                "auto_spread_exit_enabled": self.auto_spread_exit_enabled,
                "paper_trading_mode": self.paper_trading_mode,
                "auto_trading_enabled": self.auto_trading_enabled,
                "auto_target_enabled": self.auto_target_enabled,
                "auto_target_val": self.auto_target_val,
                "auto_sl_enabled": self.auto_sl_enabled,
                "auto_sl_val": self.auto_sl_val,
                "auto_square_off_enabled": self.auto_square_off_enabled,
                "auto_square_off_time": self.auto_square_off_time,
                "broker": self.broker,
                "groww_api_key": self.groww_api_key,
                "groww_client_id": self.groww_client_id,
                "groww_secret": self.groww_secret,
                "groww_petal_symbol": self.groww_petal_symbol,
                "groww_mini_symbol": self.groww_mini_symbol,
                "dhan_client_id": self.dhan_client_id,
                "dhan_access_token": self.dhan_access_token,
                "dhan_petal_symbol": self.dhan_petal_symbol,
                "dhan_petal_token": self.dhan_petal_token,
                "dhan_mini_symbol": self.dhan_mini_symbol,
                "dhan_mini_token": self.dhan_mini_token,
                "upstox_client_id": self.upstox_client_id,
                "upstox_secret": self.upstox_secret,
                "upstox_access_token": self.upstox_access_token,
                "upstox_petal_symbol": self.upstox_petal_symbol,
                "upstox_mini_symbol": self.upstox_mini_symbol
            }
            filepath = get_data_filepath("rules_config.json")
            with open(filepath, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=4)
            self.log(f"[PERSISTENCE] Saved strategy rules to {filepath}.")
        except Exception as e:
            self.log(f"[PERSISTENCE ERROR] Failed to save rules config: {e}")

    def load_angel_master(self):
        try:
            filepath = get_data_filepath("angel_master.json")
            if os.path.exists(filepath):
                with open(filepath, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    if data.get("api_key"): self.api_key = data["api_key"]
                    if data.get("client_id"): self.client_id = data["client_id"]
                    if data.get("password"): self.password = data["password"]
                    if data.get("totp_secret"): self.totp_secret = data["totp_secret"]
                self.log(f"[PERSISTENCE] Loaded Angel One master credentials from {filepath}.")
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
            filepath = get_data_filepath("angel_master.json")
            with open(filepath, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=4)
            self.log(f"[PERSISTENCE] Saved Angel One master credentials to {filepath}.")
        except Exception as e:
            self.log(f"[PERSISTENCE ERROR] Failed to save Angel One master config: {e}")
        
    def load_trade_history(self):
        try:
            filepath = get_data_filepath("trade_history.json")
            if os.path.exists(filepath):
                with open(filepath, "r", encoding="utf-8") as f:
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
                self.log(f"[PERSISTENCE] Loaded {len(self.trade_history)} trades from {filepath}. Realized PnL: INR {self.realized_pnl:.2f}")
            else:
                self.trade_history = []
                self.log("[PERSISTENCE] No trade history file found. Starting fresh.")
        except Exception as e:
            self.log(f"[PERSISTENCE ERROR] Failed to load trade history: {e}")
            self.trade_history = []

    def save_trade_history(self):
        try:
            filepath = get_data_filepath("trade_history.json")
            with open(filepath, "w", encoding="utf-8") as f:
                json.dump(self.trade_history, f, indent=4)
        except Exception as e:
            self.log(f"[PERSISTENCE ERROR] Failed to save trade history: {e}")

    def load_manual_trades(self):
        try:
            filepath = get_data_filepath("manual_trades.json")
            if os.path.exists(filepath):
                with open(filepath, "r", encoding="utf-8") as f:
                    self.manual_trades = json.load(f)
                self.manual_trades = [t for t in self.manual_trades if str(t.get("status", "")).strip().lower() != "cancelled"]
                self.log(f"[PERSISTENCE] Loaded {len(self.manual_trades)} manual trades from {filepath}.")
            else:
                self.manual_trades = []
                self.log("[PERSISTENCE] No manual trades file found. Starting fresh.")
        except Exception as e:
            self.log(f"[PERSISTENCE ERROR] Failed to load manual trades: {e}")
            self.manual_trades = []

    def save_manual_trades(self):
        try:
            filepath = get_data_filepath("manual_trades.json")
            with open(filepath, "w", encoding="utf-8") as f:
                json.dump(self.manual_trades, f, indent=4)
        except Exception as e:
            self.log(f"[PERSISTENCE ERROR] Failed to save manual trades: {e}")

    def load_month_master(self):
        try:
            filepath = get_data_filepath("month_master.json")
            base_filepath = os.path.join(BASE_DIR, "month_master.json")
            candidate_paths = [filepath, base_filepath, "month_master.json"]
            loaded = False
            for p in candidate_paths:
                if os.path.exists(p):
                    try:
                        with open(p, "r", encoding="utf-8") as f:
                            data = json.load(f)
                        if isinstance(data, list) and len(data) > 0:
                            self.month_master = data
                            for m in self.month_master:
                                if "capture_data" not in m:
                                    m["capture_data"] = True
                            self.log(f"[PERSISTENCE] Loaded {len(self.month_master)} month master mappings from {p}.")
                            loaded = True
                            if os.path.abspath(p) != os.path.abspath(base_filepath):
                                try:
                                    with open(base_filepath, "w", encoding="utf-8") as f_out:
                                        json.dump(self.month_master, f_out, indent=4)
                                except Exception:
                                    pass
                            break
                    except Exception as err:
                        self.log(f"[PERSISTENCE ERROR] Error reading {p}: {err}")
            if not loaded:
                if not getattr(self, "month_master", None):
                    self.month_master = []
                self.log(f"[PERSISTENCE] Retaining {len(self.month_master)} Month Master entries.")
        except Exception as e:
            self.log(f"[PERSISTENCE ERROR] Failed to load month master: {e}")
            if not getattr(self, "month_master", None):
                self.month_master = []

    def save_month_master(self):
        try:
            filepath = get_data_filepath("month_master.json")
            # CRITICAL SAFETY GUARD: Never overwrite non-empty disk mappings with an empty list!
            if len(self.month_master) == 0 and os.path.exists(filepath):
                try:
                    with open(filepath, "r", encoding="utf-8") as f:
                        disk_data = json.load(f)
                    if isinstance(disk_data, list) and len(disk_data) > 0:
                        self.log("[SAFETY GUARD] In-memory month_master is empty, but disk file has entries. Preserving disk data!")
                        self.month_master = disk_data
                        return
                except Exception:
                    pass
            with open(filepath, "w", encoding="utf-8") as f:
                json.dump(self.month_master, f, indent=4)
            base_filepath = os.path.join(BASE_DIR, "month_master.json")
            if os.path.abspath(filepath) != os.path.abspath(base_filepath) and len(self.month_master) > 0:
                try:
                    with open(base_filepath, "w", encoding="utf-8") as f:
                        json.dump(self.month_master, f, indent=4)
                except Exception:
                    pass
            self.log(f"[PERSISTENCE] Saved {len(self.month_master)} month master mappings to {filepath}.")
        except Exception as e:
            self.log(f"[PERSISTENCE ERROR] Failed to save month master: {e}")

    def load_ta_trades(self):
        try:
            filepath = get_data_filepath("ta_trades.json")
            if os.path.exists(filepath):
                with open(filepath, "r", encoding="utf-8") as f:
                    self.ta_trades = json.load(f)
                self.log(f"[PERSISTENCE] Loaded {len(self.ta_trades)} Trade Automation trades from {filepath}.")
            else:
                self.ta_trades = []
        except Exception as e:
            self.log(f"[PERSISTENCE ERROR] Failed to load ta trades: {e}")
            self.ta_trades = []

    def save_ta_trades(self):
        try:
            filepath = get_data_filepath("ta_trades.json")
            with open(filepath, "w", encoding="utf-8") as f:
                json.dump(self.ta_trades, f, indent=4)
        except Exception as e:
            self.log(f"[PERSISTENCE ERROR] Failed to save ta trades: {e}")

    def load_ta_configs(self):
        try:
            filepath = get_data_filepath("ta_configs.json")
            if os.path.exists(filepath):
                with open(filepath, "r", encoding="utf-8") as f:
                    self.ta_configs = json.load(f)
                self.log(f"[PERSISTENCE] Loaded {len(self.ta_configs)} Trade Automation configs from {filepath}.")
            else:
                self.ta_configs = []
        except Exception as e:
            self.log(f"[PERSISTENCE ERROR] Failed to load ta configs: {e}")
            self.ta_configs = []

    def save_ta_configs(self):
        try:
            filepath = get_data_filepath("ta_configs.json")
            with open(filepath, "w", encoding="utf-8") as f:
                json.dump(self.ta_configs, f, indent=4)
        except Exception as e:
            self.log(f"[PERSISTENCE ERROR] Failed to save ta configs: {e}")

    def load_ta_lots_trades(self):
        try:
            filepath = get_data_filepath("ta_lots_trades.json")
            if os.path.exists(filepath):
                with open(filepath, "r", encoding="utf-8") as f:
                    self.ta_lots_trades = json.load(f)
                self.log(f"[PERSISTENCE] Loaded {len(self.ta_lots_trades)} Trade Automation with Lots trades from {filepath}.")
            else:
                self.ta_lots_trades = []
        except Exception as e:
            self.log(f"[PERSISTENCE ERROR] Failed to load ta_lots trades: {e}")
            self.ta_lots_trades = []

    def save_ta_lots_trades(self):
        try:
            filepath = get_data_filepath("ta_lots_trades.json")
            with open(filepath, "w", encoding="utf-8") as f:
                json.dump(self.ta_lots_trades, f, indent=4)
        except Exception as e:
            self.log(f"[PERSISTENCE ERROR] Failed to save ta_lots trades: {e}")

    def load_ta_lots_configs(self):
        try:
            filepath = get_data_filepath("ta_lots_configs.json")
            if os.path.exists(filepath):
                with open(filepath, "r", encoding="utf-8") as f:
                    self.ta_lots_configs = json.load(f)
                self.log(f"[PERSISTENCE] Loaded {len(self.ta_lots_configs)} Trade Automation with Lots configs from {filepath}.")
            else:
                self.ta_lots_configs = []
        except Exception as e:
            self.log(f"[PERSISTENCE ERROR] Failed to load ta_lots configs: {e}")
            self.ta_lots_configs = []

    def save_ta_lots_configs(self):
        try:
            filepath = get_data_filepath("ta_lots_configs.json")
            with open(filepath, "w", encoding="utf-8") as f:
                json.dump(self.ta_lots_configs, f, indent=4)
        except Exception as e:
            self.log(f"[PERSISTENCE ERROR] Failed to save ta_lots configs: {e}")

    def load_daily_spread_stats(self):
        try:
            filepath = get_data_filepath("daily_spread_stats.json")
            if os.path.exists(filepath):
                with open(filepath, "r", encoding="utf-8") as f:
                    self.daily_spread_stats = json.load(f)
                self.log(f"[PERSISTENCE] Loaded daily spread stats for {len(self.daily_spread_stats)} date(s) from {filepath}.")
            else:
                self.daily_spread_stats = {}
        except Exception as e:
            self.log(f"[PERSISTENCE ERROR] Failed to load daily spread stats: {e}")
            self.daily_spread_stats = {}

    def save_daily_spread_stats(self):
        try:
            filepath = get_data_filepath("daily_spread_stats.json")
            with open(filepath, "w", encoding="utf-8") as f:
                json.dump(self.daily_spread_stats, f, indent=4)
        except Exception as e:
            pass

    def load_ai_opportunities(self):
        try:
            filepath = get_data_filepath("ai_opportunities.json")
            if os.path.exists(filepath):
                with open(filepath, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    today_str = get_ist_time_str("%Y-%m-%d")
                    # Keep today's opportunities
                    self.ai_opportunities_log = [o for o in data if o.get("date") == today_str]
                self.log(f"[AI ENGINE] Loaded {len(self.ai_opportunities_log)} opportunities for today.")
        except Exception as e:
            self.log(f"[AI ENGINE] Could not load ai_opportunities.json: {e}")
            self.ai_opportunities_log = []

    def save_ai_opportunities(self):
        try:
            filepath = get_data_filepath("ai_opportunities.json")
            with open(filepath, "w", encoding="utf-8") as f:
                json.dump(self.ai_opportunities_log, f, indent=4)
        except Exception as e:
            pass

    def load_manual_bot(self):
        try:
            filepath = get_data_filepath("manual_bot_config.json")
            if os.path.exists(filepath):
                with open(filepath, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    if isinstance(data, dict):
                        # Multi-bot tasks list
                        if "bots" in data and isinstance(data["bots"], list):
                            self.manual_bots = data["bots"]
                            for b in self.manual_bots:
                                b_filled = int(b.get("filled_orders", 0))
                                b_total = int(b.get("total_orders", 1))
                                if b.get("active", False):
                                    if b_filled >= b_total:
                                        b["active"] = False
                                        b["status_message"] = f"Completed: All {b_total} orders filled."
                                        b["why_waiting"] = "All orders completed"
                                    else:
                                        # PERSIST ACTIVE STATE ON RESTART: Do not stop unless user stops it!
                                        b["active"] = True
                                        b["status_message"] = f"Active: Resumed after restart ({b_filled}/{b_total} filled)"
                                        b["why_waiting"] = f"Monitoring spread for next order ({b_filled}/{b_total} filled)"
                        
                        # Primary/legacy single bot state
                        if "manual_bot" in data and isinstance(data["manual_bot"], dict):
                            self.manual_bot.update(data["manual_bot"])
                        else:
                            self.manual_bot.update(data)
                            
                        mb_filled = int(self.manual_bot.get("filled_orders", 0))
                        mb_total = int(self.manual_bot.get("total_orders", 1))
                        if self.manual_bot.get("active", False):
                            if mb_filled >= mb_total:
                                self.manual_bot["active"] = False
                                self.manual_bot["status_message"] = f"Completed: All {mb_total} orders filled."
                                self.manual_bot["why_waiting"] = "All orders completed"
                            else:
                                # PERSIST ACTIVE STATE ON RESTART: Do not stop unless user stops it!
                                self.manual_bot["active"] = True
                                self.manual_bot["status_message"] = f"Active: Resumed after restart ({mb_filled}/{mb_total} filled)"
                                self.manual_bot["why_waiting"] = f"Monitoring spread for next order ({mb_filled}/{mb_total} filled)"
                        else:
                            if not self.manual_bot.get("status_message"):
                                self.manual_bot["status_message"] = "Idle - Bot not running."
                                self.manual_bot["why_waiting"] = "Bot stopped by user or not started"
        except Exception as e:
            pass

    def save_manual_bot(self):
        try:
            filepath = get_data_filepath("manual_bot_config.json")
            with open(filepath, "w", encoding="utf-8") as f:
                payload = {
                    "manual_bot": self.manual_bot,
                    "bots": self.manual_bots
                }
                # Also copy top-level keys for 100% backward compatibility
                payload.update(self.manual_bot)
                json.dump(payload, f, indent=4)
        except Exception as e:
            pass

    def calculate_mcx_charges(self, direction: str, qty: int, petal_entry: float, mini_entry: float, petal_exit: float, mini_exit: float, petal_symbol: str = None, mini_symbol: str = None) -> float:
        p_sym = petal_symbol or self.petal_symbol
        m_sym = mini_symbol or self.mini_symbol
        ref_p = petal_entry * 10.0 if petal_entry < 10000.0 else petal_entry
        ch_info = calculate_trade_pair_charges(p_sym, m_sym, ref_p if ref_p > 1000.0 else 72000.0)
        base_charge = ch_info["total_charges"]
        return round(base_charge * max(1, qty), 2)

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
            try:
                if hasattr(self.smart_connect, "timeout"):
                    self.smart_connect.timeout = 15
                if hasattr(self.smart_connect, "_timeout"):
                    self.smart_connect._timeout = 15
                if hasattr(self.smart_connect, "request_timeout"):
                    self.smart_connect.request_timeout = 15
                
                # Mount resilient HTTPAdapter to auto-recover from server TCP socket resets
                from requests.adapters import HTTPAdapter
                from urllib3.util import Retry
                retries = Retry(
                    total=3,
                    backoff_factor=0.2,
                    status_forcelist=[500, 502, 503, 504],
                    raise_on_status=False
                )
                adapter = HTTPAdapter(max_retries=retries, pool_connections=10, pool_maxsize=10)
                if hasattr(self.smart_connect, "reqsession") and self.smart_connect.reqsession:
                    self.smart_connect.reqsession.mount("https://", adapter)
                    self.smart_connect.reqsession.mount("http://", adapter)
            except Exception:
                pass
            
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
        sym_u = (symbol or "").upper()
        if hasattr(self, "mcx_lotsize_cache") and self.mcx_lotsize_cache:
            if sym_u in self.mcx_lotsize_cache:
                return self.mcx_lotsize_cache[sym_u]
            if sym_u.removesuffix("FUT") in self.mcx_lotsize_cache:
                return self.mcx_lotsize_cache[sym_u.removesuffix("FUT")]
        spec = get_gold_contract_specs(sym_u)
        return spec.get("api_lot_multiplier", 1)

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

    def get_tokens_for_pair(self, petal_symbol: str, mini_symbol: str) -> tuple:
        p_tok = ""
        m_tok = ""
        for m in self.month_master:
            if m.get("petal_symbol") == petal_symbol and m.get("petal_token"):
                p_tok = m["petal_token"]
            if m.get("mini_symbol") == mini_symbol and m.get("mini_token"):
                m_tok = m["mini_token"]
        if not p_tok and self.petal_symbol == petal_symbol:
            p_tok = self.petal_token
        if not m_tok and self.mini_symbol == mini_symbol:
            m_tok = self.mini_token
        if not p_tok and petal_symbol:
            p_tok = self.resolve_scrip_token_via_api(petal_symbol)
        if not m_tok and mini_symbol:
            m_tok = self.resolve_scrip_token_via_api(mini_symbol)
        return p_tok, m_tok



# Global State Instance
system_state = TradingSystem()

def check_global_order_cooldown(cooldown_seconds: float = 10.0) -> tuple[bool, float]:
    """
    Enforces a strict 10-second gap between ANY order execution (Manual or Automation, Entry or Exit).
    Returns (can_execute: bool, remaining_seconds: float).
    """
    last_time = getattr(system_state, "last_global_order_time", 0.0)
    elapsed = time.time() - last_time
    if elapsed < cooldown_seconds:
        return False, round(cooldown_seconds - elapsed, 1)
    return True, 0.0

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
        "ta_lots_configs": getattr(system_state, "ta_lots_configs", []),
        "ta_lots_trades": getattr(system_state, "ta_lots_trades", []),
        "manual_bot": system_state.manual_bot,
        "manual_bots": getattr(system_state, "manual_bots", []),
        "ai_divergence_threshold": getattr(system_state, "ai_divergence_threshold", 80.0),
        "ai_opportunities_count": len(getattr(system_state, "ai_opportunities_log", [])),
        
        "logs": system_state.logs
    })

# ----------------- Order execution -----------------
# ----------------- Order execution -----------------
def is_liquidity_sufficient(petal_action: str, mini_action: str, qty: int, petal_symbol: str = None, mini_symbol: str = None) -> bool:
    p_spec = get_gold_contract_specs(petal_symbol or system_state.petal_symbol)
    m_spec = get_gold_contract_specs(mini_symbol or system_state.mini_symbol)
    required_petal = qty * p_spec["lots_per_unit_mini"]
    required_mini = qty * m_spec["lots_per_unit_mini"]
    
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
        petal_limit = p_spec["bid_ask_limit"]
        mini_limit = m_spec["bid_ask_limit"]
        if (isinstance(system_state.petal_depth, dict) and 
                "buy" in system_state.petal_depth and len(system_state.petal_depth["buy"]) > 0 and
                "sell" in system_state.petal_depth and len(system_state.petal_depth["sell"]) > 0):
            petal_bid = float(system_state.petal_depth["buy"][0]["price"])
            petal_ask = float(system_state.petal_depth["sell"][0]["price"])
            if (petal_ask - petal_bid) > petal_limit:
                system_state.log(f"[LIQUIDITY SHIELD] Trade skipped: {p_spec['type']} Bid-Ask gap too wide ({petal_ask - petal_bid:.2f} > {petal_limit:.2f}).")
                return False

        if (isinstance(system_state.mini_depth, dict) and 
                "buy" in system_state.mini_depth and len(system_state.mini_depth["buy"]) > 0 and
                "sell" in system_state.mini_depth and len(system_state.mini_depth["sell"]) > 0):
            mini_bid = float(system_state.mini_depth["buy"][0]["price"])
            mini_ask = float(system_state.mini_depth["sell"][0]["price"])
            if (mini_ask - mini_bid) > mini_limit:
                system_state.log(f"[LIQUIDITY SHIELD] Trade skipped: {m_spec['type']} Bid-Ask gap too wide ({mini_ask - mini_bid:.2f} > {mini_limit:.2f}).")
                return False
    except Exception as e:
        logger.warning(f"Error parsing depth for bid-ask gap check: {e}")

    return True

async def check_real_orders_status(order_ids: List[str]) -> Dict[str, Dict]:
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
                oid = str(o.get("orderid", ""))
                if oid in order_ids:
                    status = o.get("status", "").upper()
                    avg_price = 0.0
                    for k in ["averageprice", "avgprice", "price"]:
                        if o.get(k):
                            try:
                                avg_price = float(o[k])
                                if avg_price > 0:
                                    break
                            except (ValueError, TypeError):
                                pass
                    status_map[oid] = {"status": status, "average_price": avg_price}
            return status_map
        elif response and isinstance(response, dict) and "exceeding access rate" in str(response.get("message", "")).lower():
            system_state.log("[ANGELONE API] Rate limit warning: Exceeded access rate on orderBook(). Backing off 0.5s...")
            await asyncio.sleep(0.5)
    except Exception as e:
        if "exceeding access rate" in str(e).lower():
            system_state.log("[ANGELONE API] Rate limit hit checking order book. Pausing 0.5s...")
            await asyncio.sleep(0.5)
        else:
            system_state.log(f"[LIVE ORDER STATUS] Error checking order book: {e}")
    return {}

async def cancel_real_order(order_id: str, variety: str = "NORMAL") -> dict:
    if not system_state.smart_connect:
        return {"success": False, "already_traded": False}
    try:
        loop = asyncio.get_running_loop()
        res = await loop.run_in_executor(
            None,
            lambda: system_state.smart_connect.cancelOrder(order_id, variety)
        )
        if isinstance(res, dict):
            err_code = str(res.get("errorcode", "")).upper()
            msg = str(res.get("message", "")).lower()
            if res.get("status") == False and (err_code == "AB1007" or "bad input" in msg or "already" in msg):
                system_state.log(f"[LIVE ORDER] Cancel rejected for order {order_id}: Already executed/traded in broker ({err_code}).")
                return {"success": False, "already_traded": True, "data": res}
        system_state.log(f"[LIVE ORDER] Cancelled order {order_id}")
        return {"success": True, "already_traded": False, "data": res}
    except Exception as e:
        err_str = str(e).lower()
        if "ab1007" in err_str or "bad input" in err_str or "already" in err_str:
            system_state.log(f"[LIVE ORDER] Cancel rejected for order {order_id}: Already executed/traded in broker.")
            return {"success": False, "already_traded": True, "error": str(e)}
        system_state.log(f"[LIVE ORDER ERROR] Failed to cancel order {order_id}: {e}")
        return {"success": False, "already_traded": False, "error": str(e)}

async def check_dhan_orders_status(order_ids: List[str]) -> Dict[str, Dict]:
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
                        status = "COMPLETE"
                    else:
                        status = raw_status
                    avg_price = 0.0
                    for k in ["tradedPrice", "averagePrice", "price"]:
                        if o.get(k):
                            try:
                                avg_price = float(o[k])
                                if avg_price > 0:
                                    break
                            except (ValueError, TypeError):
                                pass
                    status_map[oid] = {"status": status, "average_price": avg_price}
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

async def check_groww_orders_status(order_ids: List[str]) -> Dict[str, Dict]:
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
                    status = "COMPLETE"
                elif raw_status == "OPEN":
                    status = "OPEN"
                elif raw_status in ["FAILED", "REJECTED"]:
                    status = "REJECTED"
                elif raw_status == "CANCELLED":
                    status = "CANCELLED"
                else:
                    status = raw_status
                avg_price = 0.0
                for k in ["average_price", "price"]:
                    if response.get(k):
                        try:
                            avg_price = float(response[k])
                            if avg_price > 0:
                                break
                        except (ValueError, TypeError):
                            pass
                status_map[oid] = {"status": status, "average_price": avg_price}
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
    # Resolve target symbols and tokens
    target_petal_symbol = alt_petal_symbol if alt_petal_symbol is not None else system_state.petal_symbol
    target_petal_token = alt_petal_token if alt_petal_token is not None else system_state.petal_token
    target_mini_symbol = alt_mini_symbol if alt_mini_symbol is not None else system_state.mini_symbol
    target_mini_token = alt_mini_token if alt_mini_token is not None else system_state.mini_token

    if qty is None:
        qty = system_state.trade_quantity
    p_spec = get_gold_contract_specs(target_petal_symbol)
    m_spec = get_gold_contract_specs(target_mini_symbol)
    required_petal = qty * p_spec["lots_per_unit_mini"]
    required_mini = qty * m_spec["lots_per_unit_mini"]
    
    direction = system_state.position_direction if not is_entry else ("Expansion" if petal_action == "BUY" else "Contraction")
    
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
            petal_limit = p_spec["bid_ask_limit"]
            mini_limit = m_spec["bid_ask_limit"]
            if (isinstance(petal_depth, dict) and 
                    "buy" in petal_depth and len(petal_depth["buy"]) > 0 and
                    "sell" in petal_depth and len(petal_depth["sell"]) > 0):
                petal_bid = float(petal_depth["buy"][0]["price"])
                petal_ask = float(petal_depth["sell"][0]["price"])
                if (petal_ask - petal_bid) > petal_limit:
                    system_state.log(f"[LIQUIDITY SHIELD] Trade skipped: {target_petal_symbol} Bid-Ask gap too wide ({petal_ask - petal_bid:.2f} > {petal_limit:.2f}).")
                    liquidity_ok = False
    
            if (isinstance(mini_depth, dict) and 
                    "buy" in mini_depth and len(mini_depth["buy"]) > 0 and
                    "sell" in mini_depth and len(mini_depth["sell"]) > 0):
                mini_bid = float(mini_depth["buy"][0]["price"])
                mini_ask = float(mini_depth["sell"][0]["price"])
                if (mini_ask - mini_bid) > mini_limit:
                    system_state.log(f"[LIQUIDITY SHIELD] Trade skipped: {target_mini_symbol} Bid-Ask gap too wide ({mini_ask - mini_bid:.2f} > {mini_limit:.2f}).")
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

        # Place both market order legs concurrently at the exact same millisecond
        system_state.order_execution_in_progress = True
        system_state.last_global_order_time = time.time()
        try:
            petal_order_id, mini_order_id = await asyncio.gather(
                place_order_func(target_petal_symbol, target_petal_token, petal_action, required_petal),
                place_order_func(target_mini_symbol, target_mini_token, mini_action, required_mini)
            )
            
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
            
            # Initial grace period: exchange processes market orders in ~50-100ms
            await asyncio.sleep(0.8)
            
            timeout = 8.0
            elapsed = 0.8
            interval = 1.2
            
            while elapsed < timeout:
                await asyncio.sleep(interval)
                elapsed += interval
                
                # Query status
                status_map = await check_status_func([petal_order_id, mini_order_id])
                
                if not petal_filled:
                    val = status_map.get(petal_order_id, {})
                    status = val.get("status", "") if isinstance(val, dict) else str(val).upper()
                    avg_p = val.get("average_price", 0.0) if isinstance(val, dict) else 0.0
                    if status in ["COMPLETE", "COMPLETED", "TRADED", "EXECUTED", "SUCCESS"]:
                        petal_filled = True
                        if avg_p and avg_p > 0:
                            petal_fill_price = avg_p
                        else:
                            petal_fill_price = petal_ltp
                        system_state.log(f"[LIVE MARKET FILL] Leg 1: {target_petal_symbol} filled @ MARKET {petal_fill_price:.2f} (Traded Avg)")
                    elif status in ["REJECTED", "CANCELLED"]:
                        system_state.log(f"[LIVE ORDER CANCEL/REJECT] Leg 1: {target_petal_symbol} order {status.lower()}")
                        break
                        
                if not mini_filled:
                    val = status_map.get(mini_order_id, {})
                    status = val.get("status", "") if isinstance(val, dict) else str(val).upper()
                    avg_p = val.get("average_price", 0.0) if isinstance(val, dict) else 0.0
                    if status in ["COMPLETE", "COMPLETED", "TRADED", "EXECUTED", "SUCCESS"]:
                        mini_filled = True
                        if avg_p and avg_p > 0:
                            mini_fill_price = avg_p
                        else:
                            mini_fill_price = mini_ltp
                        system_state.log(f"[LIVE MARKET FILL] Leg 2: {target_mini_symbol} filled @ MARKET {mini_fill_price:.2f} (Traded Avg)")
                    elif status in ["REJECTED", "CANCELLED"]:
                        system_state.log(f"[LIVE ORDER CANCEL/REJECT] Leg 2: {target_mini_symbol} order {status.lower()}")
                        break
                        
                if petal_filled and mini_filled:
                    break

            # Cancel/reverse if not completed
            if not petal_filled and not mini_filled:
                petal_cancel_res = await cancel_order_func(petal_order_id) if petal_order_id else {}
                mini_cancel_res = await cancel_order_func(mini_order_id) if mini_order_id else {}
                
                # Check if cancel was rejected by broker because orders were already executed/traded!
                p_already = isinstance(petal_cancel_res, dict) and petal_cancel_res.get("already_traded", False)
                m_already = isinstance(mini_cancel_res, dict) and mini_cancel_res.get("already_traded", False)
                
                if p_already or m_already:
                    system_state.log("[AB1007 DESYNC RECOVERY] Broker indicated orders were already executed in market! Recovering trade...")
                    await asyncio.sleep(1.0)
                    try:
                        final_status = await check_status_func([petal_order_id, mini_order_id])
                        if petal_order_id in final_status and final_status[petal_order_id].get("average_price"):
                            petal_fill_price = final_status[petal_order_id]["average_price"]
                        elif petal_ltp > 0:
                            petal_fill_price = petal_ltp
                        if mini_order_id in final_status and final_status[mini_order_id].get("average_price"):
                            mini_fill_price = final_status[mini_order_id]["average_price"]
                        elif mini_ltp > 0:
                            mini_fill_price = mini_ltp
                    except Exception:
                        pass
                    petal_filled = True
                    mini_filled = True
                    system_state.log(f"[AB1007 DESYNC RECOVERY] Position recovered! Leg 1: {petal_fill_price:.2f}, Leg 2: {mini_fill_price:.2f}")
                else:
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
        finally:
            system_state.order_execution_in_progress = False

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
    
    p_spec = get_gold_contract_specs(system_state.petal_symbol)
    m_spec = get_gold_contract_specs(system_state.mini_symbol)
    actual_exit_spread = (petal_exit * p_spec["price_scale_to_10g"]) - (mini_exit * m_spec["price_scale_to_10g"])
    expected_exit_spread = system_state.expected_exit_spread
    
    # P&L Formulas based on contract multipliers
    qty = system_state.trade_quantity
    if direction == "Expansion":  # Buy Petal/Ten/Guinea, Sell Mini
        p_pnl = (petal_exit - system_state.petal_entry_price) * p_spec["pnl_multiplier"] * qty
        m_pnl = (system_state.mini_entry_price - mini_exit) * m_spec["pnl_multiplier"] * qty
        exit_slippage = expected_exit_spread - actual_exit_spread
    else:  # Sell Petal/Ten/Guinea, Buy Mini
        p_pnl = (system_state.petal_entry_price - petal_exit) * p_spec["pnl_multiplier"] * qty
        m_pnl = (mini_exit - system_state.mini_entry_price) * m_spec["pnl_multiplier"] * qty
        exit_slippage = actual_exit_spread - expected_exit_spread
        
    trade_pnl = p_pnl + m_pnl
    charges = system_state.calculate_mcx_charges(
        direction, qty, system_state.petal_entry_price, system_state.mini_entry_price, petal_exit, mini_exit,
        petal_symbol=system_state.petal_symbol, mini_symbol=system_state.mini_symbol
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

# =====================================================================
# 🧠 AI & QUANTITATIVE ARBITRAGE INTELLIGENCE ENGINE (Claude Sonnet + Stats)
# =====================================================================
def get_contract_expiry_date(symbol: str):
    """
    Contract ki expiry date (datetime.date) ya None.
    1. Angel One scrip master ki expiry (mcx_expiry_cache), e.g. "30JAN2027"
    2. Warna symbol se, e.g. GOLDPETAL30JAN27FUT -> 30 Jan 2027
    """
    import re
    sym_u = (symbol or "").upper()
    cached = getattr(system_state, "mcx_expiry_cache", {}).get(sym_u)
    if cached:
        for fmt in ("%d%b%Y", "%d-%b-%Y", "%Y-%m-%d"):
            try:
                return datetime.strptime(str(cached).upper(), fmt).date()
            except ValueError:
                continue
    m = re.search(r"(\d{2})([A-Z]{3})(\d{2})FUT$", sym_u)
    if m:
        try:
            return datetime.strptime(f"{m.group(1)}{m.group(2)}{m.group(3)}", "%d%b%y").date()
        except ValueError:
            return None
    return None


class AIArbitrageEngine:
    """
    Minimal AI Assistant. Iske 4 hisse hain:
    1. Scanner (process_tick): har pair ka spread uske pichle 60 min ke average se kitna door hai, track karta hai.
    2. Pair table (get_multi_pair_matrix): Month Master pairs ka live depth spread + aaj ka asli High/Low.
    3. Suggestion (generate_claude_advisory): ek simple rule -
         Depth Buy spread aaj ke range ke neeche 30% me ho  -> Expansion (BUY spread)
         Depth Sell spread aaj ke range ke upar 30% me ho   -> Contraction (SELL spread)
         warna                                               -> WAIT
    4. Claude (optional): sirf upar ke facts samjhata hai / sawaal ka jawab deta hai. Koi number Claude nahi banata.
    Data na ho to koi number assume nahi hota - "--" dikhta hai aur decision WAIT hota hai.
    """
    ZONE_PCT = 30.0              # range ke bottom/top 30% me signal
    MIN_RANGE_PTS = 20.0         # aaj ka range isse chhota ho to signal nahi
    MIN_HISTORY_SECONDS = 600    # scanner ko signal dene se pehle kam se kam 10 min ka data chahiye
    DEFAULT_TARGET_PTS = 110.0
    DEFAULT_STEP_PTS = 35.0
    DEFAULT_LOTS = "1, 2, 2"
    EXPIRY_BLOCK_DAYS = 5        # expiry se itne (ya kam) din pehle us month ka suggestion band
    MARGIN_PER_LOT = 50000.0     # wahi margin jo terminal ka 'Used Margin' maanta hai
    MAX_OPEN_LOTS_PER_PAIR = 10  # risk check: ek month me isse zyada lots par warning
    STUCK_ADVERSE_PTS = 50.0     # monitor: spread entry se itna ulta jaaye to alert
    STUCK_DAYS = 3               # monitor: itne din se khuli aur profit nahi to alert
    NEAR_TARGET_PTS = 20.0       # monitor: target itna paas ho to alert
    SNAPSHOT_SECONDS = 60        # backtest ke liye har pair ka snapshot har 60 sec
    ALERT_CHECK_SECONDS = 30     # background alert check har 30 sec
    HISTORY_DIR = os.path.join(BASE_DIR, "ai_spread_history")
    CLAUDE_MODELS = ["claude-sonnet-5-5", "claude-haiku-5-5"]

    def __init__(self, state: TradingSystem):
        self.state = state
        self._last_tick_processed = {}
        self._spread_history = {}  # pair_key -> list of (timestamp, spread)
        self.claude_api_url = "https://api.anthropic.com/v1/messages"
        self._last_ai_analysis = None
        self._last_ai_analysis_time = 0.0
        self._last_snapshot = {}       # pair_key -> last snapshot time
        self._alerts = []              # newest first
        self._alert_seq = 0
        self._alert_keys_sent = set()

    def get_api_key(self) -> str:
        # 1. Check in-memory system_state
        mem_key = getattr(self.state, "anthropic_api_key", "").strip()
        if mem_key:
            return mem_key

        # 2. Check standard environment variables
        for var in ["ANTHROPIC_API_KEY", "CLAUDE_API_KEY", "ANTHROPIC_KEY"]:
            val = os.getenv(var, "").strip()
            if val:
                return val

        # 3. Direct disk read of .env files (bypasses PM2 frozen environment cache)
        candidate_paths = [
            os.path.join(BASE_DIR, ".env"),
            os.path.join(os.getcwd(), ".env"),
            ".env",
            os.path.expanduser("~/.env")
        ]
        for env_path in candidate_paths:
            if os.path.exists(env_path):
                try:
                    with open(env_path, "r", encoding="utf-8") as f:
                        for line in f:
                            l = line.strip()
                            for prefix in ["ANTHROPIC_API_KEY=", "CLAUDE_API_KEY=", "ANTHROPIC_KEY="]:
                                if l.startswith(prefix):
                                    key_val = l.split("=", 1)[1].strip().strip('"').strip("'")
                                    if key_val:
                                        os.environ["ANTHROPIC_API_KEY"] = key_val
                                        return key_val
                except Exception:
                    pass
        return ""

    # ----------------- 1. Scanner -----------------
    def calculate_rolling_mean(self, pair_key: str, cur_spread: float) -> float:
        history = self._spread_history.setdefault(pair_key, [])
        now = time.time()
        history.append((now, cur_spread))
        # Retain last 60 minutes of tick history for rolling channel calculation
        cutoff = now - 3600
        self._spread_history[pair_key] = [h for h in history if h[0] >= cutoff]
        if not self._spread_history[pair_key]:
            return cur_spread
        spreads = [h[1] for h in self._spread_history[pair_key]]
        return sum(spreads) / len(spreads)

    def get_rolling_info(self, pair_key: str) -> dict:
        """60-min average aur kitne minute ka data hai (server restart par reset hota hai)."""
        history = self._spread_history.get(pair_key, [])
        if not history:
            return {"mean": None, "minutes": 0}
        spreads = [h[1] for h in history]
        return {
            "mean": round(sum(spreads) / len(spreads), 2),
            "minutes": int((history[-1][0] - history[0][0]) / 60)
        }

    def process_tick(self, petal_sym: str, mini_sym: str, spread: float, buy_spread: float, sell_spread: float):
        if not petal_sym or not mini_sym or spread == 0.0:
            return

        pair_key = f"{petal_sym}_{mini_sym}"
        now_ts = time.time()

        # Throttle processing to once every 1 second per pair
        if now_ts - self._last_tick_processed.get(pair_key, 0) < 1.0:
            return
        self._last_tick_processed[pair_key] = now_ts
        self.record_spread_snapshot(petal_sym, mini_sym, buy_spread, sell_spread)

        rolling_mean = self.calculate_rolling_mean(pair_key, spread)
        divergence = abs(spread - rolling_mean)
        threshold = getattr(self.state, "ai_divergence_threshold", 80.0)

        ch_info = calculate_trade_pair_charges(petal_sym, mini_sym, self.state.symbol_ltps.get(mini_sym, 0.0) or 0.0)
        trade_grams = ch_info["grams"]
        pair_charges = ch_info["total_charges"]
        pair_slippage = ch_info["slippage_cushion"]
        pair_mult = ch_info["multiplier_10g_scale"]

        # 1. Update any existing ACTIVE signals for this pair
        for opp in self.state.ai_opportunities_log:
            if opp.get("status") == "Active" and opp.get("pair_key") == pair_key:
                entry_spread = opp["trigger_spread"]
                init_div = opp["divergence_pts"]
                if opp["direction"] == "Expansion":
                    current_gain = spread - entry_spread
                else:
                    current_gain = entry_spread - spread

                # Success: spread ne shuruaati divergence ka 60% wapas cover kar liya
                target_reversion = max(20.0, init_div * 0.6)
                # Stopped: spread 0.8x aur ulta chala gaya
                stop_loss = init_div * 0.8

                if current_gain >= target_reversion:
                    opp["status"] = "Reverted (Success)"
                elif current_gain <= -stop_loss:
                    opp["status"] = "Breakout / Stopped"
                else:
                    continue

                gross = round(current_gain * pair_mult, 2)
                opp["captured_pts"] = round(current_gain, 1)
                opp["exit_spread"] = round(spread, 2)
                opp["duration_seconds"] = int(now_ts - opp.get("created_ts", now_ts))
                opp["gross_profit"] = gross
                opp["charges"] = pair_charges
                opp["slippage_cushion"] = pair_slippage
                opp["net_profit"] = round(gross - pair_charges - pair_slippage, 2)
                opp["grams"] = trade_grams
                self.state.save_ai_opportunities()

        # 2. New signal - only once we have enough history for the average to mean something
        history = self._spread_history.get(pair_key, [])
        history_span = (history[-1][0] - history[0][0]) if len(history) > 1 else 0.0
        if divergence >= threshold and history_span >= self.MIN_HISTORY_SECONDS:
            recent_same_pair = [
                o for o in self.state.ai_opportunities_log
                if o.get("pair_key") == pair_key and (now_ts - o.get("created_ts", 0)) < 180.0
            ]
            if not recent_same_pair:
                new_opp = {
                    "id": f"opp_{int(now_ts)}_{random.randint(100, 999)}",
                    "created_ts": now_ts,
                    "timestamp": get_ist_time_str("%H:%M:%S"),
                    "date": get_ist_time_str("%Y-%m-%d"),
                    "pair": f"{petal_sym} vs {mini_sym}",
                    "pair_key": pair_key,
                    "petal_symbol": petal_sym,
                    "mini_symbol": mini_sym,
                    "trigger_spread": round(spread, 2),
                    "rolling_mean": round(rolling_mean, 2),
                    "divergence_pts": round(divergence, 1),
                    "direction": "Expansion" if spread < rolling_mean else "Contraction",
                    "status": "Active",
                    "depth_buy_spread": round(buy_spread, 2),
                    "depth_sell_spread": round(sell_spread, 2),
                    "captured_pts": 0.0,
                    "duration_seconds": 0,
                    "gross_profit": 0.0,
                    "charges": pair_charges,
                    "slippage_cushion": pair_slippage,
                    "net_profit": 0.0,
                    "grams": trade_grams
                }
                self.state.ai_opportunities_log.insert(0, new_opp)
                if len(self.state.ai_opportunities_log) > 50:
                    self.state.ai_opportunities_log.pop()
                self.state.save_ai_opportunities()

    def get_opportunities_summary_from_list(self, events: list) -> dict:
        reverted = [o for o in events if "Reverted" in o.get("status", "")]
        stopped = [o for o in events if "Stopped" in o.get("status", "")]
        active = [o for o in events if o.get("status") == "Active"]
        closed_count = len(reverted) + len(stopped)
        return {
            "total_opportunities": len(events),
            "reverted_count": len(reverted),
            "active_count": len(active),
            "stopped_count": len(stopped),
            # Win rate sirf band ho chuke signals par (active signals ka result abhi pata nahi)
            "win_rate_percent": round(len(reverted) / closed_count * 100.0, 1) if closed_count else None,
            "total_simulated_profit": round(sum(o.get("net_profit", 0.0) for o in reverted + stopped), 2)
        }

    def get_opportunities_summary(self) -> dict:
        return self.get_opportunities_summary_from_list(self.state.ai_opportunities_log)

    # ----------------- 2. Pair table -----------------
    def _pairs_to_evaluate(self) -> list:
        """Sirf woh pairs jo aap trade karte ho: active pair + Month Master pairs."""
        pairs = []
        candidates = [(self.state.petal_symbol, self.state.mini_symbol)]
        for m in getattr(self.state, "month_master", []) or []:
            candidates.append((m.get("petal_symbol", ""), m.get("mini_symbol", "")))
        for p, m in candidates:
            if p and m and (p, m) not in pairs:
                pairs.append((p, m))
        return pairs

    def _range_position(self, value, low, high):
        """0% = aaj ka Low, 100% = aaj ka High. Data kam ho to None."""
        if value is None or low is None or high is None:
            return None
        if (high - low) < self.MIN_RANGE_PTS:
            return None
        return round(max(0.0, min(100.0, (value - low) / (high - low) * 100.0)), 1)

    def get_multi_pair_matrix(self) -> list:
        matrix = [self.build_pair_row(p_sym, m_sym) for p_sym, m_sym in self._pairs_to_evaluate()]

        # Live data wale pairs pehle, unme active pair sabse upar
        matrix.sort(key=lambda r: (not r["has_live_data"], not r["is_active"]))
        return matrix

    def build_pair_row(self, p_sym: str, m_sym: str) -> dict:
        """Ek pair ka live data: LTP, depth spread, aaj ka High/Low, expiry. Data na ho to None."""
        today_str = get_ist_time_str("%Y-%m-%d")
        stats_today = self.state.daily_spread_stats.get(today_str, {})
        p_spec = get_gold_contract_specs(p_sym)
        m_spec = get_gold_contract_specs(m_sym)
        is_active = (p_sym == self.state.petal_symbol and m_sym == self.state.mini_symbol)
        pair_key = f"{p_sym}_{m_sym}"

        p_ltp = self.state.symbol_ltps.get(p_sym, 0.0) or (self.state.gold_petal_ltp if p_sym == self.state.petal_symbol else 0.0)
        m_ltp = self.state.symbol_ltps.get(m_sym, 0.0) or (self.state.gold_mini_ltp if m_sym == self.state.mini_symbol else 0.0)

        row = {
            "petal_symbol": p_sym,
            "mini_symbol": m_sym,
            "label": f"{p_sym} / {m_sym}",
            "pair_key": pair_key,
            "is_active": is_active,
            "has_live_data": False,
            "petal_ltp": None,
            "mini_ltp": None,
            "spread": None,
            "depth_buy_spread": None,
            "depth_sell_spread": None,
            "depth_buy_low": None,
            "depth_buy_high": None,
            "depth_sell_low": None,
            "depth_sell_high": None,
            "buy_pos_pct": None,
            "sell_pos_pct": None,
            "expiry_date": None,
            "days_to_expiry": None
        }

        # Pair ki expiry = dono legs me se jo pehle expire ho
        expiries = [d for d in (get_contract_expiry_date(p_sym), get_contract_expiry_date(m_sym)) if d]
        if expiries:
            first_expiry = min(expiries)
            row["expiry_date"] = first_expiry.strftime("%d %b %Y")
            row["days_to_expiry"] = (first_expiry - get_ist_time().date()).days

        if not p_ltp or p_ltp <= 0 or not m_ltp or m_ltp <= 0:
            return row

        p_scale = p_spec["price_scale_to_10g"]
        m_scale = m_spec["price_scale_to_10g"]
        row["has_live_data"] = True
        row["petal_ltp"] = round(p_ltp, 2)
        row["mini_ltp"] = round(m_ltp, 2)
        row["spread"] = round(p_ltp * p_scale - m_ltp * m_scale, 2)

        p_depth = self.state.symbol_depths.get(p_sym) or (self.state.petal_depth if p_sym == self.state.petal_symbol else None)
        m_depth = self.state.symbol_depths.get(m_sym) or (self.state.mini_depth if m_sym == self.state.mini_symbol else None)
        depth_ok = all(d and d.get("buy") and d.get("sell") for d in (p_depth, m_depth))
        if depth_ok:
            p_qty = p_spec["lots_per_unit_mini"]
            m_qty = m_spec["lots_per_unit_mini"]
            # BUY spread = BUY leg1 @ Ask, SELL leg2 @ Bid
            row["depth_buy_spread"] = round(
                get_depth_average_price(p_depth, "sell", p_qty, p_ltp) * p_scale
                - get_depth_average_price(m_depth, "buy", m_qty, m_ltp) * m_scale, 2)
            # SELL spread = SELL leg1 @ Bid, BUY leg2 @ Ask
            row["depth_sell_spread"] = round(
                get_depth_average_price(p_depth, "buy", p_qty, p_ltp) * p_scale
                - get_depth_average_price(m_depth, "sell", m_qty, m_ltp) * m_scale, 2)

        rec = stats_today.get(pair_key, {})
        row["depth_buy_low"] = rec.get("low_depth_buy_spread")
        row["depth_buy_high"] = rec.get("high_depth_buy_spread")
        row["depth_sell_low"] = rec.get("low_depth_sell_spread")
        row["depth_sell_high"] = rec.get("high_depth_sell_spread")
        row["buy_pos_pct"] = self._range_position(row["depth_buy_spread"], row["depth_buy_low"], row["depth_buy_high"])
        row["sell_pos_pct"] = self._range_position(row["depth_sell_spread"], row["depth_sell_low"], row["depth_sell_high"])
        return row

    # ----------------- 3. Suggestion -----------------
    def _evaluate_pair(self, row: dict) -> tuple:
        """Returns (decision, reason). decision = Expansion / Contraction / WAIT / EXPIRY."""
        days = row["days_to_expiry"]
        if days is not None and days <= self.EXPIRY_BLOCK_DAYS:
            when = "expire ho chuka" if days < 0 else ("aaj expiry hai" if days == 0 else f"sirf {days} din baaki")
            return "EXPIRY", (
                f"Expiry {row['expiry_date']} ko hai ({when}). Expiry ke last {self.EXPIRY_BLOCK_DAYS} din me "
                f"is month me nayi entry ka suggestion nahi diya jaata. Agla month dekhein."
            )
        if not row["has_live_data"]:
            return "WAIT", "Is pair ka live price abhi nahi aaya."
        if row["depth_buy_spread"] is None or row["depth_sell_spread"] is None:
            return "WAIT", "Order book depth nahi mili, isliye asli entry spread pata nahi."

        bp = row["buy_pos_pct"]
        sp = row["sell_pos_pct"]
        if bp is None and sp is None:
            return "WAIT", f"Aaj ka High/Low range abhi {self.MIN_RANGE_PTS:.0f} pts se chhota hai ya record nahi hua, isliye 'sasta/mehenga' tay nahi kar sakte."

        if bp is not None and bp <= self.ZONE_PCT:
            return "Expansion", (
                f"Depth Buy spread {row['depth_buy_spread']} aaj ke range "
                f"({row['depth_buy_low']} se {row['depth_buy_high']}) ke neeche {bp:.0f}% par hai, yaani sasta zone."
            )
        if sp is not None and sp >= 100.0 - self.ZONE_PCT:
            return "Contraction", (
                f"Depth Sell spread {row['depth_sell_spread']} aaj ke range "
                f"({row['depth_sell_low']} se {row['depth_sell_high']}) ke upar {sp:.0f}% par hai, yaani mehenga zone."
            )
        bp_txt = f"{bp:.0f}%" if bp is not None else "--"
        sp_txt = f"{sp:.0f}%" if sp is not None else "--"
        return "WAIT", f"Spread range ke beech me hai (Buy {bp_txt}, Sell {sp_txt}). Na sasta, na mehenga."

    def _build_facts(self, row: dict, summary: dict) -> list:
        def fmt(v):
            return "--" if v is None else f"{v}"

        facts = [f"Pair: {row['label']}"]
        if row["expiry_date"]:
            facts.append(f"Expiry: {row['expiry_date']} ({row['days_to_expiry']} din baaki)")
        else:
            facts.append("Expiry: pata nahi chala (symbol/scrip master se nahi nikla)")
        if not row["has_live_data"]:
            facts.append("Live price: nahi aaya")
            return facts
        facts.append(f"LTP: {row['petal_ltp']} / {row['mini_ltp']} | LTP spread: {row['spread']} pts")
        facts.append(
            f"Depth Buy spread: {fmt(row['depth_buy_spread'])} pts | Aaj Low {fmt(row['depth_buy_low'])}, High {fmt(row['depth_buy_high'])}"
        )
        facts.append(
            f"Depth Sell spread: {fmt(row['depth_sell_spread'])} pts | Aaj Low {fmt(row['depth_sell_low'])}, High {fmt(row['depth_sell_high'])}"
        )
        rolling = self.get_rolling_info(row["pair_key"])
        if rolling["mean"] is not None:
            facts.append(f"Pichle {rolling['minutes']} min ka average spread: {rolling['mean']} pts")
        win = summary.get("win_rate_percent")
        facts.append(
            f"Scanner aaj (is pair me): {summary.get('total_opportunities', 0)} signals "
            f"({summary.get('reverted_count', 0)} success, {summary.get('stopped_count', 0)} stopped, "
            f"{summary.get('active_count', 0)} chal rahe) | Win rate: {'--' if win is None else str(win) + '%'}"
        )
        return facts

    def _build_plan(self, row: dict, decision: str) -> dict:
        p_sym = row["petal_symbol"]
        m_sym = row["mini_symbol"]
        target_pts = self.DEFAULT_TARGET_PTS
        ch_info = calculate_trade_pair_charges(p_sym, m_sym, row["mini_ltp"] or 0.0)

        if decision == "Expansion":
            entry = row["depth_buy_spread"]
            target = round(entry + target_pts, 1)
            entry_text = f"BUY {p_sym} @ Ask + SELL {m_sym} @ Bid"
            exit_text = f"SELL {p_sym} @ Bid + BUY {m_sym} @ Ask"
            day_range = (row["depth_buy_high"] or 0) - (row["depth_buy_low"] or 0)
        else:
            entry = row["depth_sell_spread"]
            target = round(entry - target_pts, 1)
            entry_text = f"SELL {p_sym} @ Bid + BUY {m_sym} @ Ask"
            exit_text = f"BUY {p_sym} @ Ask + SELL {m_sym} @ Bid"
            day_range = (row["depth_sell_high"] or 0) - (row["depth_sell_low"] or 0)

        gross = round(target_pts * ch_info["multiplier_10g_scale"], 2)
        net = round(gross - ch_info["total_charges"] - ch_info["slippage_cushion"], 2)

        warnings = []
        if row["days_to_expiry"] is None:
            warnings.append("Is pair ki expiry date pata nahi chali. Entry se pehle khud expiry check karein.")
        if day_range < target_pts:
            warnings.append(
                f"Aaj ka poora range sirf {day_range:.0f} pts raha hai, jabki target {target_pts:.0f} pts hai. "
                f"Aaj ke hisaab se target bada hai."
            )

        return {
            "direction": decision,
            "entry_spread": entry,
            "target_spread": target,
            "target_pts": target_pts,
            "step": self.DEFAULT_STEP_PTS,
            "lots": self.DEFAULT_LOTS,
            "entry_text": entry_text,
            "exit_text": exit_text,
            "grams": ch_info["grams"],
            "gross_profit": gross,
            "charges": ch_info["total_charges"],
            "slippage_cushion": ch_info["slippage_cushion"],
            "est_net_profit": net,
            "warnings": warnings,
            "actionable_payload": {
                "direction": decision,
                "petal_symbol": p_sym,
                "mini_symbol": m_sym,
                "entry_diff": entry,
                "exit_gap": target_pts,
                "step": self.DEFAULT_STEP_PTS,
                "lots": self.DEFAULT_LOTS
            }
        }

    def build_suggestions(self) -> list:
        """Har pair (month) ka suggestion, sorted (best upar). Claude ke bina - fast aur free."""
        matrix = self.get_multi_pair_matrix()
        today_str = get_ist_time_str("%Y-%m-%d")
        today_signals = [o for o in self.state.ai_opportunities_log if o.get("date") == today_str]

        # Har pair (month) ka alag suggestion, taaki compare kar sakein
        suggestions = []
        for row in matrix:
            decision, reason = self._evaluate_pair(row)
            pair_summary = self.get_opportunities_summary_from_list(
                [o for o in today_signals if o.get("pair_key") == row["pair_key"]]
            )
            if decision == "Expansion":
                zone_depth = row["buy_pos_pct"]            # 0% = bilkul Low par = sabse achha
            elif decision == "Contraction":
                zone_depth = 100.0 - row["sell_pos_pct"]   # 0% = bilkul High par = sabse achha
            else:
                zone_depth = None
            suggestions.append({
                "pair": row["label"],
                "petal_symbol": row["petal_symbol"],
                "mini_symbol": row["mini_symbol"],
                "is_active": row["is_active"],
                "has_live_data": row["has_live_data"],
                "decision": decision,
                "reason": reason,
                "zone_depth_pct": zone_depth,
                "depth_buy_spread": row["depth_buy_spread"],
                "depth_buy_low": row["depth_buy_low"],
                "depth_buy_high": row["depth_buy_high"],
                "buy_pos_pct": row["buy_pos_pct"],
                "depth_sell_spread": row["depth_sell_spread"],
                "depth_sell_low": row["depth_sell_low"],
                "depth_sell_high": row["depth_sell_high"],
                "sell_pos_pct": row["sell_pos_pct"],
                "expiry_date": row["expiry_date"],
                "days_to_expiry": row["days_to_expiry"],
                "facts": self._build_facts(row, pair_summary),
                "plan": self._build_plan(row, decision) if decision in ("Expansion", "Contraction") else None,
                "is_best": False
            })
            if suggestions[-1]["plan"]:
                suggestions[-1]["plan"]["risk"] = self.risk_check(row, suggestions[-1]["plan"])

        # Order: signal wale pehle (jo zone me sabse andar ho woh upar), phir WAIT, phir bina data wale, sabse neeche expiry wale
        suggestions.sort(key=lambda s: (
            s["decision"] == "EXPIRY",
            s["plan"] is None,
            not s["has_live_data"],
            s["zone_depth_pct"] if s["zone_depth_pct"] is not None else 999.0
        ))
        if suggestions and suggestions[0]["plan"]:
            suggestions[0]["is_best"] = True
        return suggestions

    async def generate_claude_advisory(self, user_query: str = "") -> dict:
        suggestions = self.build_suggestions()
        result = {
            "source": "Local Rules",
            "time": get_ist_time_str("%H:%M:%S"),
            "suggestions": suggestions,
            "signal_count": len([s for s in suggestions if s["plan"]]),
            "expiry_blocked_count": len([s for s in suggestions if s["decision"] == "EXPIRY"]),
            "rules": (
                f"Rule: Depth Buy spread aaj ke range ke neeche {self.ZONE_PCT:.0f}% me ho to BUY spread (Expansion); "
                f"Depth Sell spread upar {self.ZONE_PCT:.0f}% me ho to SELL spread (Contraction); warna WAIT. "
                f"Jis month ki expiry {self.EXPIRY_BLOCK_DAYS} din ya usse kam door ho, uska suggestion nahi aata. "
                f"Target {self.DEFAULT_TARGET_PTS:.0f} pts, Step {self.DEFAULT_STEP_PTS:.0f}, Lots {self.DEFAULT_LOTS} (default settings). "
                f"Ek se zyada signal hon to jo spread zone me sabse andar hai (Low/High ke sabse paas) woh upar dikhta hai."
            ),
            "claude_reply": "",
            "api_error": ""
        }

        # Claude sirf tab jab user ne sawaal poocha ho
        if user_query:
            api_key = self.get_api_key()
            if not api_key:
                result["api_error"] = "Claude API key set nahi hai. '🔑 Claude API Key' se set karein."
            else:
                context = {
                    "time_ist": get_ist_time_str(),
                    "market_session": get_market_session_status(),
                    "rules": result["rules"],
                    "suggestions_per_pair": [
                        {k: s[k] for k in ("pair", "decision", "reason", "facts", "plan", "is_best")}
                        for s in suggestions
                    ],
                    "open_trades": self.get_open_trades_monitor()["trades"],
                    "today_journal_summary": self.get_daily_journal(get_ist_time_str("%Y-%m-%d"))["summary"]
                }
                reply, model, err = await self._ask_claude(api_key, context, user_query)
                if reply:
                    result["claude_reply"] = reply
                    result["source"] = f"Local Rules + Claude ({model})"
                else:
                    result["api_error"] = err

        self._last_ai_analysis = result
        self._last_ai_analysis_time = time.time()
        return result

    # ----------------- 5. Risk check (form bharne se pehle) -----------------
    def _open_positions(self) -> list:
        """Saari khuli positions ek format me: Main, Manual, Trade Automation, Lots Bot. Sirf padhta hai, kuch badalta nahi."""
        st = self.state
        out = []
        if st.is_in_position:
            out.append({
                "id": "main", "source": "Main Position",
                "petal_symbol": st.petal_symbol, "mini_symbol": st.mini_symbol,
                "direction": st.position_direction, "quantity": st.trade_quantity,
                "entry_spread": st.entry_spread, "exit_gap": None,
                "entry_date": getattr(st, "entry_date", "--"), "entry_time": getattr(st, "entry_time", "--"),
                "unrealized_pnl": st.unrealized_pnl
            })
        sources = [
            ("manual", "Manual", st.manual_trades),
            ("ta", "Trade Automation", st.ta_trades),
            ("lots", "Lots Bot", getattr(st, "ta_lots_trades", [])),
        ]
        for prefix, label, trades in sources:
            for t in trades:
                if t.get("status") != "Open":
                    continue
                out.append({
                    "id": f"{prefix}_{t.get('id')}",
                    "source": f"{label} #{t.get('id')}" + (f" (Order {t.get('order_index')})" if prefix == "lots" else ""),
                    "petal_symbol": t.get("petal_symbol") or st.petal_symbol,
                    "mini_symbol": t.get("mini_symbol") or st.mini_symbol,
                    "direction": t.get("direction"),
                    "quantity": t.get("quantity", 1),
                    "entry_spread": t.get("entry_spread"),
                    "exit_gap": t.get("exit_gap"),
                    "entry_date": t.get("entry_date", "--"),
                    "entry_time": t.get("entry_time", "--"),
                    "unrealized_pnl": t.get("unrealized_pnl", 0.0)
                })
        return out

    def risk_check(self, row: dict, plan: dict) -> dict:
        """Suggestion ko form me bharne se pehle: margin, lots, same month ki khuli position, expiry."""
        try:
            lots_list = [int(x.strip()) for x in str(plan["lots"]).split(",") if x.strip()]
        except ValueError:
            lots_list = []
        total_lots = sum(lots_list)
        margin_needed = total_lots * self.MARGIN_PER_LOT
        available = round(self.state.available_balance, 2)

        same_pair = [p for p in self._open_positions()
                     if p["petal_symbol"] == row["petal_symbol"] and p["mini_symbol"] == row["mini_symbol"]]
        open_lots = sum(int(p.get("quantity") or 0) for p in same_pair)
        same_dir = [p for p in same_pair if p["direction"] == plan["direction"]]
        opposite = [p for p in same_pair if p["direction"] != plan["direction"]]

        warnings = []
        if margin_needed > available:
            warnings.append(f"Margin kam hai: saare {total_lots} lots ke liye ~₹{margin_needed:,.0f} chahiye, available ₹{available:,.0f} hai.")
        if open_lots + total_lots > self.MAX_OPEN_LOTS_PER_PAIR:
            warnings.append(f"Is month me pehle se {open_lots} lots khule hain; {total_lots} aur lene par {open_lots + total_lots} ho jayenge (limit {self.MAX_OPEN_LOTS_PER_PAIR}).")
        if same_dir:
            warnings.append(f"Is month me pehle se {len(same_dir)} {plan['direction']} position(s) khuli hain ({sum(int(p['quantity'] or 0) for p in same_dir)} lots).")
        if opposite:
            warnings.append(f"Is month me ulti direction ki {len(opposite)} position(s) khuli hain. Nayi entry unse takra sakti hai.")
        days = row.get("days_to_expiry")
        if days is not None and days <= self.EXPIRY_BLOCK_DAYS * 2:
            warnings.append(f"Expiry {days} din me hai. Target na aaya to rollover ya expiry se pehle exit karna padega.")

        return {
            "total_lots": total_lots,
            "margin_needed": margin_needed,
            "available_balance": available,
            "open_lots_same_pair": open_lots,
            "warnings": warnings
        }

    # ----------------- 6. Open trade monitor -----------------
    def get_open_trades_monitor(self) -> dict:
        rows = {}
        trades = []
        for pos in self._open_positions():
            key = (pos["petal_symbol"], pos["mini_symbol"])
            if key not in rows:
                rows[key] = self.build_pair_row(*key)
            row = rows[key]
            direction = pos["direction"]
            entry = pos["entry_spread"]
            gap = pos["exit_gap"]

            # Exit hamesha ulti depth side par hota hai
            exit_now = row["depth_sell_spread"] if direction == "Expansion" else row["depth_buy_spread"]
            target = None
            if entry is not None and gap is not None:
                target = round(entry + gap, 2) if direction == "Expansion" else round(entry - gap, 2)

            moved = to_target = progress = None
            if exit_now is not None and entry is not None:
                moved = round(exit_now - entry if direction == "Expansion" else entry - exit_now, 1)
                if target is not None:
                    to_target = round(target - exit_now if direction == "Expansion" else exit_now - target, 1)
                    progress = round(moved / gap * 100.0, 0) if gap else None

            days_held = None
            try:
                days_held = (get_ist_time().date() - datetime.strptime(str(pos["entry_date"]), "%Y-%m-%d").date()).days
            except ValueError:
                pass

            alerts = []
            days = row["days_to_expiry"]
            if days is not None and days <= self.EXPIRY_BLOCK_DAYS:
                alerts.append({"type": "EXPIRY", "text": f"Expiry {row['expiry_date']} ({days} din) - position abhi khuli hai. Rollover ya exit plan karein."})
            if moved is not None and moved <= -self.STUCK_ADVERSE_PTS:
                alerts.append({"type": "ADVERSE", "text": f"Spread entry se {abs(moved):.0f} pts ulta gaya hai."})
            elif days_held is not None and days_held >= self.STUCK_DAYS and (moved is None or moved <= 0):
                alerts.append({"type": "STUCK", "text": f"{days_held} din se khuli hai aur abhi tak profit me nahi hai."})
            if to_target is not None and 0 < to_target <= self.NEAR_TARGET_PTS:
                alerts.append({"type": "NEAR_TARGET", "text": f"Target sirf {to_target:.0f} pts door hai."})

            if not row["has_live_data"] or exit_now is None:
                status = "NO_DATA"
            elif alerts and alerts[0]["type"] in ("EXPIRY", "ADVERSE", "STUCK"):
                status = alerts[0]["type"]
            elif moved is not None and moved > 0:
                status = "PROFIT"
            else:
                status = "LOSS"

            trades.append({
                "id": pos["id"],
                "source": pos["source"],
                "pair": row["label"],
                "direction": direction,
                "quantity": pos["quantity"],
                "entry_date": pos["entry_date"],
                "entry_time": pos["entry_time"],
                "days_held": days_held,
                "entry_spread": round(entry, 2) if entry is not None else None,
                "exit_spread_now": exit_now,
                "target_spread": target,
                "moved_pts": moved,
                "to_target_pts": to_target,
                "progress_pct": progress,
                "unrealized_pnl": round(pos["unrealized_pnl"] or 0.0, 2),
                "expiry_date": row["expiry_date"],
                "days_to_expiry": days,
                "buy_pos_pct": row["buy_pos_pct"],
                "sell_pos_pct": row["sell_pos_pct"],
                "status": status,
                "alerts": alerts
            })

        order = {"EXPIRY": 0, "ADVERSE": 1, "STUCK": 2, "LOSS": 3, "NO_DATA": 4, "PROFIT": 5}
        trades.sort(key=lambda t: order.get(t["status"], 9))
        return {
            "trades": trades,
            "total_open": len(trades),
            "total_lots": sum(int(t["quantity"] or 0) for t in trades),
            "total_unrealized": round(sum(t["unrealized_pnl"] for t in trades), 2),
            "alert_count": sum(len(t["alerts"]) for t in trades),
            "rules": (
                f"Alert tab: expiry {self.EXPIRY_BLOCK_DAYS} din ya kam | spread entry se {self.STUCK_ADVERSE_PTS:.0f}+ pts ulta | "
                f"{self.STUCK_DAYS}+ din se khuli aur profit nahi | target {self.NEAR_TARGET_PTS:.0f} pts ke andar. "
                f"Exit spread = Expansion me Depth Sell, Contraction me Depth Buy."
            )
        }

    # ----------------- 7. Daily trade journal -----------------
    def get_daily_journal(self, date_str: str) -> dict:
        history = [t for t in self.state.trade_history if str(t.get("date", ""))[:10] == date_str]
        done = [t for t in history if t.get("status") == "COMPLETED"]
        failed = [t for t in history if t.get("status") in ("FAILED", "CANCELLED")]

        rows = []
        for t in done:
            entry = t.get("actual_entry_spread") or t.get("entry_spread") or 0.0
            exit_ = t.get("actual_exit_spread") or t.get("exit_spread") or 0.0
            pts = (exit_ - entry) if t.get("direction") == "Expansion" else (entry - exit_)
            net = float(t.get("pnl", 0.0) or 0.0)
            charges = float(t.get("charges", 0.0) or 0.0)
            gross = float(t.get("gross_pnl", net + charges) or 0.0)
            rows.append({
                "entry_time": t.get("entry_time", "--"),
                "exit_time": t.get("exit_time", "--"),
                "source": t.get("reason", "--"),
                "pair": f"{t['petal_symbol']} / {t['mini_symbol']}" if t.get("petal_symbol") and t.get("mini_symbol") else "--",
                "direction": t.get("direction", "--"),
                "entry_spread": round(entry, 2),
                "exit_spread": round(exit_, 2),
                "points": round(pts, 1),
                "gross": round(gross, 2),
                "charges": round(charges, 2),
                "net": round(net, 2)
            })

        wins = [r for r in rows if r["net"] > 0]
        losses = [r for r in rows if r["net"] <= 0]
        by_source = {}
        for r in rows:
            b = by_source.setdefault(r["source"], {"trades": 0, "net": 0.0})
            b["trades"] += 1
            b["net"] = round(b["net"] + r["net"], 2)

        is_today = date_str == get_ist_time_str("%Y-%m-%d")
        signals = self.get_opportunities_summary_from_list(
            [o for o in self.state.ai_opportunities_log if o.get("date") == date_str]
        ) if is_today else None
        monitor = self.get_open_trades_monitor() if is_today else None

        summary = {
            "date": date_str,
            "trades": len(rows),
            "wins": len(wins),
            "losses": len(losses),
            "win_rate_percent": round(len(wins) / len(rows) * 100.0, 1) if rows else None,
            "gross": round(sum(r["gross"] for r in rows), 2),
            "charges": round(sum(r["charges"] for r in rows), 2),
            "net": round(sum(r["net"] for r in rows), 2),
            "best": max(rows, key=lambda r: r["net"]) if rows else None,
            "worst": min(rows, key=lambda r: r["net"]) if rows else None,
            "failed_attempts": len(failed),
            "by_source": by_source,
            "open_positions": monitor["total_open"] if monitor else None,
            "open_unrealized": monitor["total_unrealized"] if monitor else None,
            "scanner": signals
        }
        dates = sorted({str(t.get("date", ""))[:10] for t in self.state.trade_history if t.get("date")}, reverse=True)
        return {
            "summary": summary,
            "trades": rows,
            "failed": [{"time": t.get("entry_time") if t.get("entry_time") != "--" else t.get("exit_time"),
                        "direction": t.get("direction"), "reason": t.get("reason")} for t in failed],
            "available_dates": dates[:60],
            "note": "Trade usi din me dikhta hai jis din uski entry hui thi (trade history ki 'date')."
        }

    # ----------------- 8. Spread history recording + Backtest -----------------
    def _is_live_feed(self) -> bool:
        """Simulation mode me record nahi karte - warna backtest nakli data par chalega."""
        return getattr(self.state, "broker", "") in ("AngelOne", "Dhan", "Groww") and bool(getattr(self.state, "api_connected", False))

    def record_spread_snapshot(self, petal_sym: str, mini_sym: str, buy_spread: float, sell_spread: float):
        """Har pair ka har minute ek snapshot disk par (ai_spread_history/YYYY-MM-DD.jsonl). Backtest isi par chalta hai."""
        if buy_spread is None or sell_spread is None or not buy_spread or not sell_spread:
            return
        if not self._is_live_feed() or get_market_session_status() != "OPEN":
            return
        pair_key = f"{petal_sym}_{mini_sym}"
        now_ts = time.time()
        if now_ts - self._last_snapshot.get(pair_key, 0) < self.SNAPSHOT_SECONDS:
            return
        self._last_snapshot[pair_key] = now_ts
        try:
            os.makedirs(self.HISTORY_DIR, exist_ok=True)
            line = json.dumps({
                "ts": int(now_ts), "t": get_ist_time_str("%H:%M"),
                "p": petal_sym, "m": mini_sym,
                "b": round(buy_spread, 2), "s": round(sell_spread, 2),
                "mltp": round(self.state.symbol_ltps.get(mini_sym, 0.0) or 0.0, 2)
            })
            with open(os.path.join(self.HISTORY_DIR, f"{get_ist_time_str('%Y-%m-%d')}.jsonl"), "a", encoding="utf-8") as f:
                f.write(line + "\n")
        except Exception as e:
            logger.info(f"[AI HISTORY] snapshot save failed: {e}")

    def run_backtest(self, days: int = 30) -> dict:
        """
        Saved spread history par WAHI rule chalata hai jo live suggestion deta hai
        (aaj ka range, 30% zone, 110 target, expiry block). Har snapshot par sirf utna data use hota hai
        jitna us waqt tak pata tha. Simplification: har pair me ek waqt par ek hi position, 1 unit, averaging ladder nahi.
        """
        files = []
        if os.path.isdir(self.HISTORY_DIR):
            files = sorted(f for f in os.listdir(self.HISTORY_DIR) if f.endswith(".jsonl"))[-max(1, days):]
        snaps_by_pair = {}
        for fname in files:
            date_str = fname[:-6]
            try:
                with open(os.path.join(self.HISTORY_DIR, fname), "r", encoding="utf-8") as f:
                    for line in f:
                        try:
                            s = json.loads(line)
                        except ValueError:
                            continue
                        snaps_by_pair.setdefault((s["p"], s["m"]), []).append((date_str, s))
            except OSError:
                continue

        target_pts = self.DEFAULT_TARGET_PTS
        closed, still_open = [], []
        for (p_sym, m_sym), snaps in snaps_by_pair.items():
            snaps.sort(key=lambda x: x[1]["ts"])
            expiries = [d for d in (get_contract_expiry_date(p_sym), get_contract_expiry_date(m_sym)) if d]
            first_expiry = min(expiries) if expiries else None
            pos = None
            cur_day = None
            b_lo = b_hi = s_lo = s_hi = None
            for date_str, s in snaps:
                b, sl = s["b"], s["s"]
                if date_str != cur_day:
                    cur_day = date_str
                    b_lo = b_hi = b
                    s_lo = s_hi = sl
                b_lo, b_hi = min(b_lo, b), max(b_hi, b)
                s_lo, s_hi = min(s_lo, sl), max(s_hi, sl)
                day = datetime.strptime(date_str, "%Y-%m-%d").date()
                days_left = (first_expiry - day).days if first_expiry else None
                ch = calculate_trade_pair_charges(p_sym, m_sym, s.get("mltp") or 0.0)

                if pos:
                    exit_now = sl if pos["direction"] == "Expansion" else b
                    hit = exit_now >= pos["target"] if pos["direction"] == "Expansion" else exit_now <= pos["target"]
                    expired = days_left is not None and days_left <= 0
                    if hit or expired:
                        pts = (exit_now - pos["entry"]) if pos["direction"] == "Expansion" else (pos["entry"] - exit_now)
                        net = round(pts * ch["multiplier_10g_scale"] - ch["total_charges"] - ch["slippage_cushion"], 2)
                        closed.append(dict(pos, exit=exit_now, exit_at=f"{date_str} {s['t']}", points=round(pts, 1), net=net,
                                           result="Target" if hit else "Expiry exit",
                                           hold_minutes=int((s["ts"] - pos["ts"]) / 60)))
                        pos = None
                    continue

                row = {
                    "has_live_data": True,
                    "depth_buy_spread": b, "depth_sell_spread": sl,
                    "depth_buy_low": b_lo, "depth_buy_high": b_hi,
                    "depth_sell_low": s_lo, "depth_sell_high": s_hi,
                    "buy_pos_pct": self._range_position(b, b_lo, b_hi),
                    "sell_pos_pct": self._range_position(sl, s_lo, s_hi),
                    "days_to_expiry": days_left,
                    "expiry_date": first_expiry.strftime("%d %b %Y") if first_expiry else None
                }
                decision, _ = self._evaluate_pair(row)
                if decision in ("Expansion", "Contraction"):
                    entry = b if decision == "Expansion" else sl
                    pos = {
                        "pair": f"{p_sym} / {m_sym}", "direction": decision, "entry": entry,
                        "target": entry + target_pts if decision == "Expansion" else entry - target_pts,
                        "entry_at": f"{date_str} {s['t']}", "ts": s["ts"]
                    }
            if pos:
                last_date, last = snaps[-1]
                exit_now = last["s"] if pos["direction"] == "Expansion" else last["b"]
                pts = (exit_now - pos["entry"]) if pos["direction"] == "Expansion" else (pos["entry"] - exit_now)
                still_open.append(dict(pos, exit=exit_now, points=round(pts, 1), result="Abhi khuli (data khatam)"))

        closed.sort(key=lambda t: t["ts"])
        equity = peak = max_dd = 0.0
        for t in closed:
            equity += t["net"]
            peak = max(peak, equity)
            max_dd = max(max_dd, peak - equity)
        wins = [t for t in closed if t["net"] > 0]
        by_pair = {}
        for t in closed:
            b = by_pair.setdefault(t["pair"], {"trades": 0, "net": 0.0, "wins": 0})
            b["trades"] += 1
            b["net"] = round(b["net"] + t["net"], 2)
            b["wins"] += 1 if t["net"] > 0 else 0

        return {
            "days_of_data": len(files),
            "first_day": files[0][:-6] if files else None,
            "last_day": files[-1][:-6] if files else None,
            "snapshots": sum(len(v) for v in snaps_by_pair.values()),
            "trades": len(closed),
            "wins": len(wins),
            "losses": len(closed) - len(wins),
            "win_rate_percent": round(len(wins) / len(closed) * 100.0, 1) if closed else None,
            "net": round(sum(t["net"] for t in closed), 2),
            "avg_net": round(sum(t["net"] for t in closed) / len(closed), 2) if closed else None,
            "max_drawdown": round(max_dd, 2),
            "avg_hold_minutes": int(sum(t["hold_minutes"] for t in closed) / len(closed)) if closed else None,
            "by_pair": by_pair,
            "closed_trades": [{k: v for k, v in t.items() if k != "ts"} for t in closed[-100:]],
            "open_at_end": [{k: v for k, v in t.items() if k != "ts"} for t in still_open],
            "note": (
                "Data har minute ka snapshot hai (sirf live broker feed, market OPEN time). "
                f"Rule wahi hai jo live suggestion ka hai; target {target_pts:.0f} pts, 1 unit, ek pair me ek waqt par ek position, "
                "averaging ladder simulate nahi hoti. Charges + slippage cushion kaat ke Net."
            )
        }

    # ----------------- 9. Alerts (background) -----------------
    def _push_alert(self, key: str, level: str, text: str):
        """Ek alert din me ek hi baar (same key)."""
        day_key = f"{get_ist_time_str('%Y-%m-%d')}|{key}"
        if day_key in self._alert_keys_sent:
            return
        self._alert_keys_sent.add(day_key)
        self._alert_seq += 1
        self._alerts.insert(0, {
            "id": self._alert_seq,
            "time": get_ist_time_str("%H:%M:%S"),
            "level": level,
            "text": text
        })
        del self._alerts[100:]
        self.state.log(f"[AI ALERT] {text}")

    def check_alerts(self):
        if get_market_session_status() == "SUSPENDED":
            return
        for s in self.build_suggestions():
            if s["plan"]:
                label = "BUY SPREAD" if s["decision"] == "Expansion" else "SELL SPREAD"
                self._push_alert(
                    f"signal|{s['pair']}|{s['decision']}", "signal",
                    f"{s['pair']}: {label} signal @ {s['plan']['entry_spread']} (target {s['plan']['target_spread']}, net ~₹{s['plan']['est_net_profit']:,.0f})"
                )
        for t in self.get_open_trades_monitor()["trades"]:
            for a in t["alerts"]:
                self._push_alert(f"trade|{t['id']}|{a['type']}", "warning" if a["type"] != "NEAR_TARGET" else "info",
                                 f"{t['source']} ({t['pair']}): {a['text']}")

    async def alert_loop(self):
        while True:
            await asyncio.sleep(self.ALERT_CHECK_SECONDS)
            try:
                self.check_alerts()
            except Exception as e:
                logger.error(f"[AI ALERT LOOP] {e}")

    def get_alerts(self, after_id: int = 0) -> list:
        return [a for a in self._alerts if a["id"] > after_id]

    # ----------------- 4. Claude -----------------
    async def _ask_claude(self, api_key: str, context: dict, question: str) -> tuple:
        """Returns (reply_text, model, error)."""
        system_prompt = (
            "Tum AccuStock MCX Gold spread trading terminal ke assistant ho. Trader Hinglish me baat karta hai, "
            "tum bhi simple Hinglish me jawab do.\n"
            "Rules:\n"
            "1. Sirf diye gaye DATA ka use karo. Koi number, probability, news ya event khud se mat banao.\n"
            "2. Jawab ke liye data na ho to saaf bolo: 'yeh data mere paas nahi hai'.\n"
            "3. Tum trade execute nahi karte. Final decision trader ka hai. Risk hamesha batao.\n"
            "4. Jawab chhota rakho (6-8 lines), plain text, koi table nahi."
        )
        user_message = f"DATA:\n{json.dumps(context, indent=2, default=str)}\n\nSAWAL: {question}"
        headers = {
            "x-api-key": api_key,
            "anthropic-version": "2023-06-01",
            "content-type": "application/json"
        }

        models = []
        env_model = os.getenv("CLAUDE_MODEL", "").strip()
        if env_model:
            models.append(env_model)
        models += [m for m in self.CLAUDE_MODELS if m not in models]

        last_err = "Claude se jawab nahi mila"
        try:
            async with aiohttp.ClientSession() as session:
                for model in models:
                    payload = {
                        "model": model,
                        "max_tokens": 700,
                        "system": system_prompt,
                        "messages": [{"role": "user", "content": user_message}]
                    }
                    try:
                        async with session.post(self.claude_api_url, headers=headers, json=payload,
                                                timeout=aiohttp.ClientTimeout(total=25)) as resp:
                            if resp.status == 200:
                                data = await resp.json()
                                text = "".join(b.get("text", "") for b in data.get("content", []) if b.get("type") == "text").strip()
                                if text:
                                    return text, model, ""
                                last_err = "Claude ne khaali jawab diya"
                                continue
                            err_text = await resp.text()
                            try:
                                err_msg = json.loads(err_text).get("error", {}).get("message", f"HTTP {resp.status}")
                            except Exception:
                                err_msg = err_text[:200] or f"HTTP {resp.status}"
                            if resp.status == 401:
                                return "", model, "API key galat hai (Anthropic ne reject kiya)."
                            if resp.status == 404:
                                logger.info(f"[CLAUDE API] Model '{model}' available nahi, agla try kar rahe hain.")
                                last_err = err_msg
                                continue
                            return "", model, err_msg
                    except asyncio.TimeoutError:
                        last_err = f"{model}: timeout"
                        continue
        except Exception as e:
            logger.error(f"[CLAUDE API ERROR] {e}")
            last_err = str(e)
        return "", "", last_err


# Global AI Engine Instance
ai_arbitrage_engine = AIArbitrageEngine(system_state)

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
            p_spec = get_gold_contract_specs(system_state.petal_symbol)
            m_spec = get_gold_contract_specs(system_state.mini_symbol)
            system_state.entry_spread = (system_state.petal_entry_price * p_spec["price_scale_to_10g"]) - (system_state.mini_entry_price * m_spec["price_scale_to_10g"])
            
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

async def run_ta_entry(mapping: dict, direction: str, qty: int, expected_spread: float, paper_mode: bool, exit_gap: float = 100.0):
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
            p_spec = get_gold_contract_specs(mapping["petal_symbol"])
            m_spec = get_gold_contract_specs(mapping["mini_symbol"])
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
                "entry_spread": (result["petal_fill_price"] * p_spec["price_scale_to_10g"]) - (result["mini_fill_price"] * m_spec["price_scale_to_10g"]),
                "expected_entry_spread": expected_spread,
                "exit_gap": exit_gap,
                "petal_entry_type": result["petal_order_type"],
                "mini_entry_type": result["mini_order_type"],
                "petal_exit_price": 0.0,
                "mini_exit_price": 0.0,
                "exit_spread": 0.0,
                "actual_exit_spread": 0.0,
                "paper_mode": paper_mode,
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
            p_spec = get_gold_contract_specs(mapping["petal_symbol"])
            m_spec = get_gold_contract_specs(mapping["mini_symbol"])
            petal_exit = result["petal_fill_price"]
            mini_exit = result["mini_fill_price"]
            petal_exit_type = result["petal_order_type"]
            mini_exit_type = result["mini_order_type"]
            
            actual_exit_spread = (petal_exit * p_spec["price_scale_to_10g"]) - (mini_exit * m_spec["price_scale_to_10g"])
            expected_exit_spread = system_state.depth_sell_spread if direction == "Expansion" else system_state.depth_buy_spread
            
            if direction == "Expansion":
                p_pnl = (petal_exit - trade["petal_entry_price"]) * p_spec["pnl_multiplier"] * qty
                m_pnl = (trade["mini_entry_price"] - mini_exit) * m_spec["pnl_multiplier"] * qty
                exit_slippage = expected_exit_spread - actual_exit_spread
            else:
                p_pnl = (trade["petal_entry_price"] - petal_exit) * p_spec["pnl_multiplier"] * qty
                m_pnl = (mini_exit - trade["mini_entry_price"]) * m_spec["pnl_multiplier"] * qty
                exit_slippage = actual_exit_spread - expected_exit_spread
                
            trade_pnl = p_pnl + m_pnl
            charges = system_state.calculate_mcx_charges(
                direction, qty, trade["petal_entry_price"], trade["mini_entry_price"], petal_exit, mini_exit,
                petal_symbol=mapping["petal_symbol"], mini_symbol=mapping["mini_symbol"]
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
            system_state.last_ta_exit_time = time.time()
            system_state.log(f"[TA EXIT] Squared off trade ID {trade['id']}. Net PnL: INR {net_pnl:+.2f}")
    except Exception as e:
        system_state.log(f"[TA EXIT ERROR] {e}")
    finally:
        system_state.ta_execution_in_progress = False
        await broadcast_system_state()

async def run_trade_automation_checks():
    if system_state.ta_execution_in_progress or system_state.system_status in ["Halted", "Hold"]:
        return
        
    # 1. Autonomous Exit Monitoring for ALL Open Trade Automation trades (Instance-Independent)
    open_trades = [t for t in system_state.ta_trades if t.get("status") == "Open"]
    for trade in open_trades:
        p_sym = trade.get("petal_symbol")
        m_sym = trade.get("mini_symbol")
        
        # Find matching live stat in system_state.month_master_live
        live_stat = None
        for stat in system_state.month_master_live:
            if stat.get("petal_symbol") == p_sym and stat.get("mini_symbol") == m_sym:
                live_stat = stat
                break
                
        if not live_stat:
            # Fallback if active streamed instrument matches
            if system_state.petal_symbol == p_sym and system_state.mini_symbol == m_sym:
                live_stat = {
                    "depth_buy_spread": system_state.depth_buy_spread,
                    "depth_sell_spread": system_state.depth_sell_spread
                }
            else:
                continue
                
        buy_spread = live_stat["depth_buy_spread"]
        sell_spread = live_stat["depth_sell_spread"]
        
        direction = trade.get("direction", "Expansion")
        entry_spread = float(trade.get("entry_spread", 0.0))
        t_exit_gap = float(trade.get("exit_gap", 100.0))
        
        exit_triggered = False
        if direction == "Expansion":
            if sell_spread >= entry_spread + t_exit_gap:
                exit_triggered = True
        elif direction == "Contraction":
            if buy_spread <= entry_spread - t_exit_gap:
                exit_triggered = True
                
        if exit_triggered:
            # Enforce 10-second time gap between ANY order execution globally to protect against slippage and API rate limits
            can_exec, rem = check_global_order_cooldown()
            if not can_exec:
                continue

            # Find month mapping
            mapping = None
            for m in system_state.month_master:
                if m.get("petal_symbol") == p_sym and m.get("mini_symbol") == m_sym:
                    mapping = m
                    break
            if not mapping:
                mapping = {
                    "petal_symbol": p_sym,
                    "petal_token": getattr(system_state, "petal_token", "") if system_state.petal_symbol == p_sym else "",
                    "mini_symbol": m_sym,
                    "mini_token": getattr(system_state, "mini_token", "") if system_state.mini_symbol == m_sym else ""
                }
                
            paper_mode = trade.get("paper_mode", True)
            for cfg in system_state.ta_configs:
                c_idx = cfg.get("month_idx", -1)
                if 0 <= c_idx < len(system_state.month_master):
                    cm = system_state.month_master[c_idx]
                    if cm.get("petal_symbol") == p_sym and cm.get("mini_symbol") == m_sym:
                        paper_mode = cfg.get("paper_mode", True)
                        break
                        
            system_state.log(f"[TA TRIGGER] Autonomous Exit met for trade ID {trade['id']} ({p_sym}/{m_sym}). Entry: {entry_spread:.2f}, Exit: {sell_spread if direction == 'Expansion' else buy_spread:.2f} (Target Gap: {t_exit_gap:.2f})")
            await run_ta_exit(trade, mapping, paper_mode)
            return # Process one action at a time to prevent concurrency conflicts

    # 2. Check Entries / Grid Averaging for active bot instances
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
        pair_open_trades = [t for t in system_state.ta_trades if t["status"] == "Open" and t["petal_symbol"] == p_sym and t["mini_symbol"] == m_sym]
        
        direction = config.get("direction", "Expansion")
        qty = config.get("quantity", 1)
        entry_diff = config.get("entry_diff", 500.0)
        averaging_step = config.get("averaging_step", 50.0)
        exit_gap = config.get("exit_gap", 100.0)
        paper_mode = config.get("paper_mode", True)
        
        # Check entries
        num_open = len(pair_open_trades)
        max_orders = config.get("max_orders", 5)
        
        if num_open >= max_orders:
            # Reached max order limit for this bot instance - skip taking new entry/averaging trades
            continue

        # Enforce minimum 10-second time gap between ANY order execution globally
        can_exec, rem = check_global_order_cooldown()
        if not can_exec:
            continue

        # Fixed Anchor Grid (Slippage-Independent)
        # Order 1 (num_open=0): Base +- (0 * Step) -> e.g. entry_diff directly (1010)
        # Order 2 (num_open=1): Base +- (1 * Step) -> e.g. 1010 +- 30 = 1040 (or 980)
        # Order 3 (num_open=2): Base +- (2 * Step) -> e.g. 1010 +- 60 = 1070 (or 950)
        order_index = num_open + 1
        if direction == "Expansion":
            target_spread = entry_diff - (num_open * averaging_step)
        else:
            target_spread = entry_diff + (num_open * averaging_step)

        entry_triggered = False
        if direction == "Expansion":
            if buy_spread <= target_spread:
                entry_triggered = True
        elif direction == "Contraction":
            if sell_spread >= target_spread:
                entry_triggered = True

        if entry_triggered:
            config["last_order_time"] = time.time()
            trigger_label = "First" if num_open == 0 else f"Averaging #{num_open+1}"
            system_state.log(f"[TA TRIGGER] {trigger_label} (Order #{order_index}) entry met for {p_sym}/{m_sym}. Spread: {buy_spread if direction == 'Expansion' else sell_spread:.2f} (Target: {target_spread:.2f}, Base: {entry_diff:.2f}, Step: {averaging_step:.2f})")
            await run_ta_entry(mapping, direction, qty, buy_spread if direction == "Expansion" else sell_spread, paper_mode, exit_gap)
            return

# ==============================================================================
# 🌟 NEW MODULE: Trade Automation with Lots (Tiered / Custom Lots Grid)
# ==============================================================================
async def run_ta_lots_entry(mapping: dict, direction: str, qty: int, expected_spread: float, paper_mode: bool = True, exit_gap: float = 100.0, order_index: any = 1, total_orders: int = 1):
    if system_state.ta_lots_execution_in_progress:
        return
    system_state.ta_lots_execution_in_progress = True
    try:
        petal_action = "BUY" if direction == "Expansion" else "SELL"
        mini_action = "SELL" if direction == "Expansion" else "BUY"
        
        result = await execute_trade(
            petal_action, mini_action,
            check_liquidity=False,
            is_entry=True,
            qty=qty,
            alt_petal_symbol=mapping["petal_symbol"],
            alt_petal_token=mapping.get("petal_token"),
            alt_mini_symbol=mapping["mini_symbol"],
            alt_mini_token=mapping.get("mini_token"),
            paper_mode_override=paper_mode
        )
        if result["success"]:
            p_spec = get_gold_contract_specs(mapping["petal_symbol"])
            m_spec = get_gold_contract_specs(mapping["mini_symbol"])
            trade_id = len(system_state.ta_lots_trades) + 1
            new_trade = {
                "id": trade_id,
                "order_index": order_index,
                "total_orders": total_orders,
                "direction": direction,
                "quantity": qty,
                "status": "Open",
                "entry_time": get_ist_time_str("%H:%M:%S"),
                "entry_date": get_ist_time_str("%Y-%m-%d"),
                "petal_symbol": mapping["petal_symbol"],
                "mini_symbol": mapping["mini_symbol"],
                "petal_entry_price": result["petal_fill_price"],
                "mini_entry_price": result["mini_fill_price"],
                "entry_spread": (result["petal_fill_price"] * p_spec["price_scale_to_10g"]) - (result["mini_fill_price"] * m_spec["price_scale_to_10g"]),
                "expected_entry_spread": expected_spread,
                "exit_gap": exit_gap,
                "petal_entry_type": result["petal_order_type"],
                "mini_entry_type": result["mini_order_type"],
                "petal_exit_price": 0.0,
                "mini_exit_price": 0.0,
                "exit_spread": 0.0,
                "actual_exit_spread": 0.0,
                "paper_mode": paper_mode,
                "petal_exit_type": "--",
                "mini_exit_type": "--",
                "exit_time": "--",
                "exit_date": "--",
                "pnl": 0.0,
                "charges": 0.0
            }
            system_state.ta_lots_trades.append(new_trade)
            system_state.save_ta_lots_trades()
            system_state.log(f"[TA LOTS ENTRY] Filled Order #{order_index} ({qty} lot{'s' if qty > 1 else ''}) {direction} entry. Expected: {expected_spread:.2f}, Filled: {new_trade['entry_spread']:.2f}. Petal: {new_trade['petal_entry_price']:.2f}, Mini: {new_trade['mini_entry_price']:.2f}")
    except Exception as e:
        system_state.log(f"[TA LOTS ENTRY ERROR] {e}")
    finally:
        system_state.ta_lots_execution_in_progress = False
        await broadcast_system_state()

async def run_ta_lots_exit(trade: dict, mapping: dict, paper_mode: bool = True):
    if system_state.ta_lots_execution_in_progress:
        return
    system_state.ta_lots_execution_in_progress = True
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
            p_spec = get_gold_contract_specs(mapping["petal_symbol"])
            m_spec = get_gold_contract_specs(mapping["mini_symbol"])
            petal_exit = result["petal_fill_price"]
            mini_exit = result["mini_fill_price"]
            petal_exit_type = result["petal_order_type"]
            mini_exit_type = result["mini_order_type"]
            
            actual_exit_spread = (petal_exit * p_spec["price_scale_to_10g"]) - (mini_exit * m_spec["price_scale_to_10g"])
            expected_exit_spread = system_state.depth_sell_spread if direction == "Expansion" else system_state.depth_buy_spread
            
            if direction == "Expansion":
                p_pnl = (petal_exit - trade["petal_entry_price"]) * p_spec["pnl_multiplier"] * qty
                m_pnl = (trade["mini_entry_price"] - mini_exit) * m_spec["pnl_multiplier"] * qty
                exit_slippage = expected_exit_spread - actual_exit_spread
            else:
                p_pnl = (trade["petal_entry_price"] - petal_exit) * p_spec["pnl_multiplier"] * qty
                m_pnl = (mini_exit - trade["mini_entry_price"]) * m_spec["pnl_multiplier"] * qty
                exit_slippage = actual_exit_spread - expected_exit_spread
                
            trade_pnl = p_pnl + m_pnl
            charges = system_state.calculate_mcx_charges(
                direction, qty, trade["petal_entry_price"], trade["mini_entry_price"], petal_exit, mini_exit,
                petal_symbol=mapping["petal_symbol"], mini_symbol=mapping["mini_symbol"]
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
            
            # Record in trade history
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
                "reason": "TA-Lots-Exit",
                "details": f"Trade Automation with Lots ID {trade['id']} (Order #{trade.get('order_index', 1)}, Qty {qty}) closed. Net P&L: {net_pnl:.2f}."
            }
            system_state.trade_history.append(history_record)
            
            system_state.save_ta_lots_trades()
            system_state.save_trade_history()
            system_state.last_ta_lots_exit_time = time.time()
            system_state.log(f"[TA LOTS EXIT] Squared off trade ID {trade['id']} ({qty} lots). Net PnL: INR {net_pnl:+.2f}")
    except Exception as e:
        system_state.log(f"[TA LOTS EXIT ERROR] {e}")
    finally:
        system_state.ta_lots_execution_in_progress = False
        await broadcast_system_state()

async def run_trade_automation_lots_checks():
    if getattr(system_state, "ta_lots_execution_in_progress", False) or system_state.system_status in ["Halted", "Hold"]:
        return
        
    # 1. Autonomous Exit Monitoring for Trade Automation with Lots
    open_trades = [t for t in getattr(system_state, "ta_lots_trades", []) if t.get("status") == "Open"]
    for trade in open_trades:
        p_sym = trade.get("petal_symbol")
        m_sym = trade.get("mini_symbol")
        
        live_stat = None
        for stat in system_state.month_master_live:
            if stat.get("petal_symbol") == p_sym and stat.get("mini_symbol") == m_sym:
                live_stat = stat
                break
                
        if not live_stat:
            if system_state.petal_symbol == p_sym and system_state.mini_symbol == m_sym:
                live_stat = {
                    "depth_buy_spread": system_state.depth_buy_spread,
                    "depth_sell_spread": system_state.depth_sell_spread
                }
            else:
                continue
                
        buy_spread = live_stat["depth_buy_spread"]
        sell_spread = live_stat["depth_sell_spread"]
        
        direction = trade.get("direction", "Expansion")
        entry_spread = float(trade.get("entry_spread", 0.0))
        t_exit_gap = float(trade.get("exit_gap", 100.0))
        
        exit_triggered = False
        if direction == "Expansion":
            if sell_spread >= entry_spread + t_exit_gap:
                exit_triggered = True
        elif direction == "Contraction":
            if buy_spread <= entry_spread - t_exit_gap:
                exit_triggered = True
                
        if exit_triggered:
            # Enforce 10-second time gap between ANY order execution globally to avoid slippage and rate limits
            can_exec, rem = check_global_order_cooldown()
            if not can_exec:
                continue

            mapping = None
            for m in system_state.month_master:
                if m.get("petal_symbol") == p_sym and m.get("mini_symbol") == m_sym:
                    mapping = m
                    break
            if not mapping:
                mapping = {
                    "petal_symbol": p_sym,
                    "petal_token": getattr(system_state, "petal_token", "") if system_state.petal_symbol == p_sym else "",
                    "mini_symbol": m_sym,
                    "mini_token": getattr(system_state, "mini_token", "") if system_state.mini_symbol == m_sym else ""
                }
                
            paper_mode = trade.get("paper_mode", True)
            for cfg in getattr(system_state, "ta_lots_configs", []):
                c_idx = cfg.get("month_idx", -1)
                if 0 <= c_idx < len(system_state.month_master):
                    cm = system_state.month_master[c_idx]
                    if cm.get("petal_symbol") == p_sym and cm.get("mini_symbol") == m_sym:
                        paper_mode = cfg.get("paper_mode", True)
                        break
                        
            system_state.log(f"[TA LOTS TRIGGER] Autonomous Exit met for trade ID {trade['id']} ({p_sym}/{m_sym}, Qty: {trade.get('quantity', 1)}). Entry: {entry_spread:.2f}, Exit: {sell_spread if direction == 'Expansion' else buy_spread:.2f} (Target Gap: {t_exit_gap:.2f})")
            await run_ta_lots_exit(trade, mapping, paper_mode)
            return

    # 2. Check Entries for Trade Automation with Lots
    for config in getattr(system_state, "ta_lots_configs", []):
        if not config.get("enabled", False):
            continue
            
        idx = config.get("month_idx", -1)
        if idx < 0 or idx >= len(system_state.month_master):
            continue
            
        mapping = system_state.month_master[idx]
        p_sym = mapping["petal_symbol"]
        m_sym = mapping["mini_symbol"]
        
        live_stat = None
        for stat in system_state.month_master_live:
            if stat["petal_symbol"] == p_sym and stat["mini_symbol"] == m_sym:
                live_stat = stat
                break
                
        if not live_stat:
            continue
            
        buy_spread = live_stat["depth_buy_spread"]
        sell_spread = live_stat["depth_sell_spread"]
        
        # Get active (Open) Trade Automation with Lots trades for this specific month pair
        pair_open_trades = [t for t in getattr(system_state, "ta_lots_trades", []) if t["status"] == "Open" and t["petal_symbol"] == p_sym and t["mini_symbol"] == m_sym]
        num_open = len(pair_open_trades)
        
        # Parse custom tiered lots sequence
        lots_list = config.get("lots_list")
        if not lots_list and config.get("lots_str"):
            try:
                lots_list = [int(x.strip()) for x in str(config["lots_str"]).split(",") if x.strip() and int(x.strip()) > 0]
            except Exception:
                lots_list = []
        if not lots_list:
            lots_list = [config.get("quantity", 1)]
            
        # Reset ladder progress if there are no open trades for this pair (fresh cycle)
        if num_open == 0:
            config["level_index"] = 0
            config["lot_index"] = 0
        else:
            # Smart sync: prevent re-placing orders if trades are already open (e.g. after file update or restart)
            total_open_lots = sum(int(t.get("quantity", 1)) for t in pair_open_trades)
            accum = 0
            calc_lvl = 0
            calc_sub = 0
            for l_idx, l_count in enumerate(lots_list):
                if accum + l_count <= total_open_lots:
                    accum += l_count
                    calc_lvl = l_idx + 1
                    calc_sub = 0
                else:
                    calc_lvl = l_idx
                    calc_sub = total_open_lots - accum
                    break

            curr_lvl = config.get("level_index", 0)
            curr_sub = config.get("lot_index", 0)
            # Advance progress if open trades show we are already further ahead
            if (calc_lvl > curr_lvl) or (calc_lvl == curr_lvl and calc_sub > curr_sub):
                config["level_index"] = calc_lvl
                config["lot_index"] = calc_sub

        level_idx = config.get("level_index", 0)
        lot_idx = config.get("lot_index", 0)

        # Check if all levels in the sequence are exhausted
        if level_idx >= len(lots_list):
            continue

        lots_in_this_level = lots_list[level_idx]
        if lot_idx >= lots_in_this_level:
            level_idx += 1
            lot_idx = 0
            config["level_index"] = level_idx
            config["lot_index"] = lot_idx
            if level_idx >= len(lots_list):
                continue
            lots_in_this_level = lots_list[level_idx]

        # Enforce minimum 10-second time gap between ANY order execution globally
        can_exec, rem = check_global_order_cooldown()
        if not can_exec:
            continue

        direction = config.get("direction", "Expansion")
        entry_diff = config.get("entry_diff", 500.0)
        averaging_step = config.get("averaging_step", 50.0)
        exit_gap = config.get("exit_gap", 100.0)
        paper_mode = config.get("paper_mode", True)

        if direction == "Expansion":
            target_spread = entry_diff - (level_idx * averaging_step)
        else:
            target_spread = entry_diff + (level_idx * averaging_step)

        entry_triggered = False
        if direction == "Expansion":
            if buy_spread <= target_spread:
                entry_triggered = True
        elif direction == "Contraction":
            if sell_spread >= target_spread:
                entry_triggered = True

        if entry_triggered:
            config["last_order_time"] = time.time()
            current_qty = 1  # Always execute 1 lot per order to preserve market depth and prevent slippage
            order_index_str = f"{level_idx + 1}.{lot_idx + 1}"

            next_lot_idx = lot_idx + 1
            next_level_idx = level_idx
            if next_lot_idx >= lots_in_this_level:
                next_level_idx = level_idx + 1
                next_lot_idx = 0

            config["level_index"] = next_level_idx
            config["lot_index"] = next_lot_idx
            system_state.save_ta_lots_configs()

            trigger_label = f"Order #{level_idx + 1} (Lot {lot_idx + 1}/{lots_in_this_level})"
            system_state.log(f"[TA LOTS TRIGGER] {trigger_label} ({current_qty} lot) entry met for {p_sym}/{m_sym}. Spread: {buy_spread if direction == 'Expansion' else sell_spread:.2f} (Target: {target_spread:.2f}, Level #{level_idx + 1}, Base: {entry_diff:.2f}, Step: {averaging_step:.2f})")
            await run_ta_lots_entry(mapping, direction, current_qty, buy_spread if direction == "Expansion" else sell_spread, paper_mode, exit_gap, order_index_str, len(lots_list))
            return

async def execute_netting_manual_trades(new_direction: str, qty: int, expected_entry_spread: float, pending_trade: dict = None,
                                       petal_symbol: str = None, mini_symbol: str = None, trade_source: str = "MANUAL") -> dict:
    global system_state
    
    # Target symbols & tokens
    p_sym = petal_symbol or (pending_trade.get("petal_symbol") if pending_trade else None) or system_state.petal_symbol
    m_sym = mini_symbol or (pending_trade.get("mini_symbol") if pending_trade else None) or system_state.mini_symbol
    p_tok, m_tok = system_state.get_tokens_for_pair(p_sym, m_sym)
    
    # 1. Calculate how much quantity we can net FOR THIS SPECIFIC PAIR
    opposite_trades = [
        t for t in system_state.manual_trades 
        if t.get("status") == "Open" and t.get("direction") != new_direction
        and (t.get("petal_symbol", system_state.petal_symbol) == p_sym and t.get("mini_symbol", system_state.mini_symbol) == m_sym)
    ]
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
        system_state.log(f"[NETTING] Executing offsetting orders for quantity {net_qty} in direction {new_direction} for {p_sym}/{m_sym}...")
        result = await execute_trade(
            petal_action, mini_action, 
            check_liquidity=True, 
            is_entry=True, 
            qty=net_qty,
            alt_petal_symbol=p_sym,
            alt_petal_token=p_tok,
            alt_mini_symbol=m_sym,
            alt_mini_token=m_tok
        )
        if result["success"]:
            p_spec = get_gold_contract_specs(p_sym)
            m_spec = get_gold_contract_specs(m_sym)
            petal_exit = result["petal_fill_price"]
            mini_exit = result["mini_fill_price"]
            petal_exit_type = result["petal_order_type"]
            mini_exit_type = result["mini_order_type"]
            actual_exit_spread = (petal_exit * p_spec["price_scale_to_10g"]) - (mini_exit * m_spec["price_scale_to_10g"])
            expected_exit_spread = expected_entry_spread
            
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
                        p_pnl = (petal_exit - t.get("petal_entry_price", 0.0)) * p_spec["pnl_multiplier"] * t_qty
                        m_pnl = (t.get("mini_entry_price", 0.0) - mini_exit) * m_spec["pnl_multiplier"] * t_qty
                        exit_slippage = expected_exit_spread - actual_exit_spread
                    else:
                        p_pnl = (t.get("petal_entry_price", 0.0) - petal_exit) * p_spec["pnl_multiplier"] * t_qty
                        m_pnl = (mini_exit - t.get("mini_entry_price", 0.0)) * m_spec["pnl_multiplier"] * t_qty
                        exit_slippage = actual_exit_spread - expected_exit_spread
                        
                    trade_pnl = p_pnl + m_pnl
                    charges = system_state.calculate_mcx_charges(
                        t_dir, t_qty, t.get("petal_entry_price", 0.0), t.get("mini_entry_price", 0.0), petal_exit, mini_exit,
                        petal_symbol=p_sym, mini_symbol=m_sym
                    )
                    net_pnl = trade_pnl - charges
                    
                    t["petal_pnl"] = p_pnl
                    t["mini_pnl"] = m_pnl
                    t["pnl"] = net_pnl
                    t["charges"] = charges
                    t["exit_time"] = get_ist_time_str("%H:%M:%S")
                    t["exit_date"] = get_ist_time_str("%Y-%m-%d")
                    t["exit_slippage"] = exit_slippage
                    
                    system_state.realized_pnl += net_pnl
                    system_state.total_trades += 1
                    if net_pnl > 0:
                        system_state.winning_trades += 1
                        
                    history_record = {
                        "id": len(system_state.trade_history) + 1,
                        "date": t.get("entry_date", get_ist_time_str("%Y-%m-%d")),
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
                        p_pnl = (petal_exit - t.get("petal_entry_price", 0.0)) * p_spec["pnl_multiplier"] * closed_qty
                        m_pnl = (t.get("mini_entry_price", 0.0) - mini_exit) * m_spec["pnl_multiplier"] * closed_qty
                        exit_slippage = expected_exit_spread - actual_exit_spread
                    else:
                        p_pnl = (t.get("petal_entry_price", 0.0) - petal_exit) * p_spec["pnl_multiplier"] * closed_qty
                        m_pnl = (mini_exit - t.get("mini_entry_price", 0.0)) * m_spec["pnl_multiplier"] * closed_qty
                        exit_slippage = actual_exit_spread - expected_exit_spread
                        
                    trade_pnl = p_pnl + m_pnl
                    charges = system_state.calculate_mcx_charges(
                        t_dir, closed_qty, t.get("petal_entry_price", 0.0), t.get("mini_entry_price", 0.0), petal_exit, mini_exit,
                        petal_symbol=p_sym, mini_symbol=m_sym
                    )
                    net_pnl = trade_pnl - charges
                    
                    system_state.realized_pnl += net_pnl
                    system_state.total_trades += 1
                    if net_pnl > 0:
                        system_state.winning_trades += 1
                        
                    history_record = {
                        "id": len(system_state.trade_history) + 1,
                        "date": t.get("entry_date", get_ist_time_str("%Y-%m-%d")),
                        "direction": t_dir,
                        "status": "COMPLETED",
                        "entry_time": t.get("entry_time"),
                        "exit_time": get_ist_time_str("%H:%M:%S"),
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
                    pending_trade["petal_symbol"] = p_sym
                    pending_trade["mini_symbol"] = m_sym
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
        system_state.log(f"[NETTING] Opening remaining quantity {open_qty} in direction {new_direction} for {p_sym}/{m_sym}...")
        result_open = await execute_trade(
            petal_action, mini_action, 
            check_liquidity=True, 
            is_entry=True, 
            qty=open_qty,
            alt_petal_symbol=p_sym,
            alt_petal_token=p_tok,
            alt_mini_symbol=m_sym,
            alt_mini_token=m_tok
        )
        if result_open["success"]:
            p_spec_open = get_gold_contract_specs(p_sym)
            m_spec_open = get_gold_contract_specs(m_sym)
            petal_price = result_open["petal_fill_price"]
            mini_price = result_open["mini_fill_price"]
            entry_spread = (petal_price * p_spec_open["price_scale_to_10g"]) - (mini_price * m_spec_open["price_scale_to_10g"])
            
            if new_direction == "Expansion":
                entry_slippage = entry_spread - expected_entry_spread
            else:
                entry_slippage = expected_entry_spread - entry_spread
                
            if pending_trade:
                pending_trade["status"] = "Open"
                pending_trade["trade_source"] = pending_trade.get("trade_source", trade_source)
                pending_trade["quantity"] = open_qty
                pending_trade["petal_symbol"] = p_sym
                pending_trade["mini_symbol"] = m_sym
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
                    "trade_source": trade_source,
                    "quantity": open_qty,
                    "trigger_diff": None,
                    "status": "Open",
                    "entry_time": get_ist_time_str("%H:%M:%S"),
                    "entry_date": get_ist_time_str("%Y-%m-%d"),
                    "petal_symbol": p_sym,
                    "mini_symbol": m_sym,
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

async def trigger_manual_trade_execution(trade: dict, expected_entry_spread: float = None):
    global system_state
    direction = trade["direction"]
    p_sym = trade.get("petal_symbol") or system_state.petal_symbol
    m_sym = trade.get("mini_symbol") or system_state.mini_symbol
    
    if expected_entry_spread is None:
        if p_sym == system_state.petal_symbol and m_sym == system_state.mini_symbol:
            expected_entry_spread = system_state.depth_buy_spread if direction == "Expansion" else system_state.depth_sell_spread
        else:
            for stat in system_state.month_master_live:
                if stat.get("petal_symbol") == p_sym and stat.get("mini_symbol") == m_sym:
                    expected_entry_spread = stat["depth_buy_spread"] if direction == "Expansion" else stat["depth_sell_spread"]
                    break
            if expected_entry_spread is None:
                expected_entry_spread = 0.0
                
    system_state.log(f"[MANUAL TRIGGER] Pending manual trade ID {trade['id']} ({p_sym}/{m_sym}) triggered. Processing netting/entry...")
    
    result = await execute_netting_manual_trades(
        direction, 
        trade["quantity"], 
        expected_entry_spread, 
        pending_trade=trade,
        petal_symbol=p_sym,
        mini_symbol=m_sym
    )
    if not result["success"]:
        trade["status"] = "Failed"
        trade["reason"] = result.get("reason", "Unknown execution error")
        system_state.log(f"[MANUAL TRIGGER ERROR] Pending trade ID {trade['id']} execution failed: {trade['reason']}")
        system_state.save_manual_trades()
        
    await broadcast_system_state()

async def run_manual_bot_check():
    global system_state
    if getattr(system_state, "mbot_execution_in_progress", False) or system_state.system_status in ["Halted", "Hold"]:
        return
        
    # Collect all active bot tasks
    active_bots = []
    if hasattr(system_state, "manual_bots") and isinstance(system_state.manual_bots, list):
        for b in system_state.manual_bots:
            if b.get("active", False):
                active_bots.append(b)
                
    if not active_bots and system_state.manual_bot.get("active", False):
        active_bots.append(system_state.manual_bot)
        
    if not active_bots:
        return

    now_ts = time.time()
    state_changed = False

    for bot in active_bots:
        total_orders = int(bot.get("total_orders", 5))
        filled_orders = int(bot.get("filled_orders", 0))
        if filled_orders >= total_orders:
            bot["active"] = False
            bot["status_message"] = f"Completed: All {total_orders} orders successfully filled."
            bot["why_waiting"] = f"Completed: All {total_orders} orders filled."
            state_changed = True
            continue

        # Check 10-second cooldown
        last_time = float(bot.get("last_order_time", 0.0))
        cooldown = float(bot.get("order_delay", 10.0))
        time_diff = now_ts - last_time
        if last_time > 0 and time_diff < cooldown:
            rem = int(cooldown - time_diff)
            bot["status_message"] = f"Cooldown: waiting {rem}s ({filled_orders}/{total_orders} filled)"
            bot["why_waiting"] = f"Cooldown: waiting {rem}s before next order"
            continue

        p_sym = bot.get("petal_symbol") or system_state.petal_symbol
        m_sym = bot.get("mini_symbol") or system_state.mini_symbol
        direction = bot.get("direction", "Expansion")
        trigger_diff = float(bot.get("trigger_diff", 1400.0))
        diff_gap = float(bot.get("diff_gap", 20.0))
        qty = int(bot.get("quantity", 1))

        # Resolve live spread
        live_spread = None
        if p_sym == system_state.petal_symbol and m_sym == system_state.mini_symbol:
            live_spread = system_state.depth_buy_spread if direction == "Expansion" else system_state.depth_sell_spread
        else:
            for stat in getattr(system_state, "month_master_live", []):
                if stat.get("petal_symbol") == p_sym and stat.get("mini_symbol") == m_sym:
                    live_spread = stat.get("depth_buy_spread") if direction == "Expansion" else stat.get("depth_sell_spread")
                    break

        if live_spread is None or live_spread <= 0:
            bot["why_waiting"] = "Waiting for live market feed & quote data"
            continue

        bot["live_spread"] = round(live_spread, 2)

        # Tolerance range check:
        # Expansion: (trigger_diff - diff_gap) <= spread <= trigger_diff
        # Contraction: trigger_diff <= spread <= (trigger_diff + diff_gap)
        is_in_range = False
        if direction == "Expansion":
            min_p = trigger_diff - diff_gap
            max_p = trigger_diff
            if min_p <= live_spread <= max_p:
                is_in_range = True
        elif direction == "Contraction":
            min_p = trigger_diff
            max_p = trigger_diff + diff_gap
            if min_p <= live_spread <= max_p:
                is_in_range = True

        if not is_in_range:
            bot["status_message"] = f"Scanning: spread {live_spread:.2f} outside target [{min_p:.1f} - {max_p:.1f}] ({filled_orders}/{total_orders} filled)"
            if live_spread < min_p:
                bot["why_waiting"] = f"Spread {live_spread:.2f} is below target range [{min_p:.1f} - {max_p:.1f}]"
            else:
                bot["why_waiting"] = f"Spread {live_spread:.2f} is above target range [{min_p:.1f} - {max_p:.1f}]"
            continue

        # Check execution lock or halted / hold state
        if system_state.execution_in_progress or system_state.system_status in ["Halted", "Hold"]:
            bot["why_waiting"] = "Execution lock in progress or system Halted/Hold"
            continue

        # Double check disk persistence to guard against any process desync or duplicate processes
        try:
            cfg_path = get_data_filepath("manual_bot_config.json")
            if os.path.exists(cfg_path):
                with open(cfg_path, "r", encoding="utf-8") as f_chk:
                    disk_cfg = json.load(f_chk)
                disk_bots = disk_cfg.get("bots", [])
                for db in disk_bots:
                    if str(db.get("id")) == str(bot.get("id")):
                        disk_filled = int(db.get("filled_orders", 0))
                        if disk_filled > filled_orders:
                            filled_orders = disk_filled
                            bot["filled_orders"] = disk_filled
                        if disk_filled >= total_orders or not db.get("active", True):
                            bot["active"] = False
                            break
        except Exception:
            pass

        if filled_orders >= total_orders or not bot.get("active", True):
            state_changed = True
            continue

        can_exec, rem = check_global_order_cooldown()
        if not can_exec:
            continue

        order_idx = filled_orders + 1
        bot_label = bot.get("id", "MAIN")
        system_state.log(f"[MANUAL BOT {bot_label}] Triggered Order #{order_idx}/{total_orders} for {p_sym}/{m_sym} at spread {live_spread:.2f} (Target Window: {min_p:.1f} - {max_p:.1f})...")
        bot["last_order_time"] = time.time()
        
        # Pessimistic reservation: advance filled_orders immediately and persist to prevent duplicate or concurrent execution
        bot["filled_orders"] = order_idx
        if bot["filled_orders"] >= total_orders:
            bot["active"] = False
        system_state.save_manual_bot()
        
        result = await execute_netting_manual_trades(
            direction, qty, live_spread,
            petal_symbol=p_sym, mini_symbol=m_sym,
            trade_source="MANUAL_BOT"
        )
        if result.get("success", False):
            if "trades" not in bot or not isinstance(bot["trades"], list):
                bot["trades"] = []
            bot_trade_record = {
                "bot_id": bot.get("id", "MAIN"),
                "order_num": order_idx,
                "total_orders": total_orders,
                "time": get_ist_time_str("%H:%M:%S"),
                "date": get_ist_time_str("%Y-%m-%d"),
                "petal_symbol": p_sym,
                "mini_symbol": m_sym,
                "direction": direction,
                "target_diff": trigger_diff,
                "filled_spread": live_spread,
                "petal_fill_price": result.get("petal_fill_price", 0.0),
                "mini_fill_price": result.get("mini_fill_price", 0.0),
                "quantity": qty,
                "status": "Filled"
            }
            bot["trades"].insert(0, bot_trade_record)
            if len(bot["trades"]) > 50:
                bot["trades"] = bot["trades"][:50]

            # Also mirror to main system_state.manual_bot["trades"]
            if "trades" not in system_state.manual_bot or not isinstance(system_state.manual_bot["trades"], list):
                system_state.manual_bot["trades"] = []
            system_state.manual_bot["trades"].insert(0, bot_trade_record)
            if len(system_state.manual_bot["trades"]) > 100:
                system_state.manual_bot["trades"] = system_state.manual_bot["trades"][:100]

            if bot["filled_orders"] >= total_orders:
                bot["active"] = False
                bot["status_message"] = f"Completed: All {total_orders} orders filled successfully."
                bot["why_waiting"] = f"Completed: All {total_orders} orders filled."
            else:
                bot["status_message"] = f"Order #{order_idx} filled. 10s cooldown started ({bot['filled_orders']}/{total_orders})."
                bot["why_waiting"] = f"Order #{order_idx} filled. 10s cooldown before Order #{order_idx + 1}"
            state_changed = True
        else:
            bot["filled_orders"] = order_idx - 1
            bot["active"] = True
            bot["status_message"] = f"Order #{order_idx} failed: {result.get('reason', 'Execution error')}"
            bot["why_waiting"] = f"Order #{order_idx} execution error: {result.get('reason', 'Execution error')}"
            system_state.log(f"[MANUAL BOT ERROR] Order #{order_idx} execution failed: {result.get('reason')}")
            state_changed = True

    if state_changed:
        system_state.save_manual_bot()
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

def update_daily_high_low_stats(p_sym: str, m_sym: str, spread_val: float, p_ltp: float, m_ltp: float, buy_spread: float = None, sell_spread: float = None):
    """
    Tracks daily High and Low for Spread, Depth Buy Spread, Depth Sell Spread, Petal Price, and Mini Price.
    Persists to daily_spread_stats.json.
    """
    if not p_sym or not m_sym:
        return
    if (p_ltp is None or p_ltp <= 0) and (m_ltp is None or m_ltp <= 0):
        return

    try:
        now_ist = get_ist_time()
        today_str = now_ist.strftime("%Y-%m-%d")
        sess_stat = get_market_session_status()
        
        if today_str not in system_state.daily_spread_stats:
            system_state.daily_spread_stats[today_str] = {}
            
        pair_key = f"{p_sym}_{m_sym}"
        month = extract_month_from_symbol(p_sym)
        if month == "UNKNOWN":
            month = extract_month_from_symbol(m_sym)

        if pair_key not in system_state.daily_spread_stats[today_str]:
            system_state.daily_spread_stats[today_str][pair_key] = {
                "date": today_str,
                "month": month,
                "petal_symbol": p_sym,
                "mini_symbol": m_sym,
                "month_group": f"{p_sym} / {m_sym}",
                "high_spread": None,
                "low_spread": None,
                "high_depth_buy_spread": None,
                "low_depth_buy_spread": None,
                "high_depth_sell_spread": None,
                "low_depth_sell_spread": None,
                "last_spread": None,
                "petal_high": None,
                "petal_low": None,
                "petal_last": None,
                "mini_high": None,
                "mini_low": None,
                "mini_last": None,
                "open_spread_905": None,
                "close_spread_2327": None,
                "last_updated": now_ist.strftime("%H:%M:%S")
            }

        rec = system_state.daily_spread_stats[today_str][pair_key]
        cur_spread = round(spread_val, 2)
        cur_p = round(p_ltp, 2) if p_ltp and p_ltp > 0 else None
        cur_m = round(m_ltp, 2) if m_ltp and m_ltp > 0 else None
        h_changed = False

        # Capture 09:05 Open Spread
        if now_ist.hour == 9 and 5 <= now_ist.minute <= 6 and rec.get("open_spread_905") is None:
            rec["open_spread_905"] = cur_spread
            h_changed = True

        # Capture 23:27 Close Spread
        if now_ist.hour == 23 and now_ist.minute >= 26 and rec.get("close_spread_2327") is None:
            rec["close_spread_2327"] = cur_spread
            h_changed = True

        # High and Low tracking (excluding HOLD & SUSPENDED)
        if sess_stat not in ["HOLD", "SUSPENDED"]:
            # Generic Spread High / Low
            if rec.get("high_spread") is None or cur_spread > rec["high_spread"]:
                rec["high_spread"] = cur_spread
                h_changed = True
            if rec.get("low_spread") is None or cur_spread < rec["low_spread"]:
                rec["low_spread"] = cur_spread
                h_changed = True

            # Explicit Depth Buy Spread High / Low (What you pay to BUY the spread)
            if buy_spread is not None:
                cur_b = round(buy_spread, 2)
                if rec.get("high_depth_buy_spread") is None or cur_b > rec["high_depth_buy_spread"]:
                    rec["high_depth_buy_spread"] = cur_b
                    h_changed = True
                if rec.get("low_depth_buy_spread") is None or cur_b < rec["low_depth_buy_spread"]:
                    rec["low_depth_buy_spread"] = cur_b
                    h_changed = True

            # Explicit Depth Sell Spread High / Low (What you receive to SELL the spread)
            if sell_spread is not None:
                cur_s = round(sell_spread, 2)
                if rec.get("high_depth_sell_spread") is None or cur_s > rec["high_depth_sell_spread"]:
                    rec["high_depth_sell_spread"] = cur_s
                    h_changed = True
                if rec.get("low_depth_sell_spread") is None or cur_s < rec["low_depth_sell_spread"]:
                    rec["low_depth_sell_spread"] = cur_s
                    h_changed = True

            # Petal Price High / Low
            if cur_p is not None and cur_p > 0:
                if rec.get("petal_high") is None or cur_p > rec["petal_high"]:
                    rec["petal_high"] = cur_p
                    h_changed = True
                if rec.get("petal_low") is None or cur_p < rec["petal_low"]:
                    rec["petal_low"] = cur_p
                    h_changed = True

            # Mini Price High / Low
            if cur_m is not None and cur_m > 0:
                if rec.get("mini_high") is None or cur_m > rec["mini_high"]:
                    rec["mini_high"] = cur_m
                    h_changed = True
                if rec.get("mini_low") is None or cur_m < rec["mini_low"]:
                    rec["mini_low"] = cur_m
                    h_changed = True

        rec["last_spread"] = cur_spread
        if cur_p is not None:
            rec["petal_last"] = cur_p
        if cur_m is not None:
            rec["mini_last"] = cur_m
        rec["last_updated"] = now_ist.strftime("%H:%M:%S")

        # Periodic / throttled save to avoid high disk I/O
        if h_changed:
            now_ts = time.time()
            if now_ts - getattr(system_state, "_last_daily_stats_save", 0) > 3.0:
                system_state._last_daily_stats_save = now_ts
                system_state.save_daily_spread_stats()
    except Exception as e:
        logger.error(f"[DAILY STATS] Error updating stats: {e}")

def record_depth_spread(*args, **kwargs):
    # Raw depth tick logging disabled as requested; High/Low summary is tracked via update_daily_high_low_stats
    pass

async def process_market_data(data: dict):
    global system_state
    
    p_spec = get_gold_contract_specs(system_state.petal_symbol)
    m_spec = get_gold_contract_specs(system_state.mini_symbol)
    
    petal_ltp = data["petal_ltp"]
    mini_ltp = data["mini_ltp"]
    spread = (petal_ltp * p_spec["price_scale_to_10g"]) - (mini_ltp * m_spec["price_scale_to_10g"])
    
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
    p_req_qty = qty * p_spec["lots_per_unit_mini"]
    m_req_qty = qty * m_spec["lots_per_unit_mini"]
    
    # Calculate depth-based spreads
    # depth_buy_spread (We Buy Leg 1, Sell Leg 2):
    avg_petal_buy = get_depth_average_price(system_state.petal_depth, "sell", p_req_qty, petal_ltp)
    avg_mini_sell = get_depth_average_price(system_state.mini_depth, "buy", m_req_qty, mini_ltp)
    system_state.depth_buy_spread = (avg_petal_buy * p_spec["price_scale_to_10g"]) - (avg_mini_sell * m_spec["price_scale_to_10g"])
    
    # depth_sell_spread (We Sell Leg 1, Buy Leg 2):
    avg_petal_sell = get_depth_average_price(system_state.petal_depth, "buy", p_req_qty, petal_ltp)
    avg_mini_buy = get_depth_average_price(system_state.mini_depth, "sell", m_req_qty, mini_ltp)
    system_state.depth_sell_spread = (avg_petal_sell * p_spec["price_scale_to_10g"]) - (avg_mini_buy * m_spec["price_scale_to_10g"])
    
    # Track Daily High / Low for Spread & Prices
    if petal_ltp > 0 and mini_ltp > 0:
        update_daily_high_low_stats(
            system_state.petal_symbol,
            system_state.mini_symbol,
            spread,
            petal_ltp,
            mini_ltp,
            buy_spread=system_state.depth_buy_spread,
            sell_spread=system_state.depth_sell_spread
        )
        ai_arbitrage_engine.process_tick(
            system_state.petal_symbol,
            system_state.mini_symbol,
            spread,
            system_state.depth_buy_spread,
            system_state.depth_sell_spread
        )
            
    # Live leg and portfolio P&L calculations (Corrected to dynamic contract multipliers)
    if system_state.is_in_position:
        direction = system_state.position_direction
        if direction == "Expansion":  # Buy Leg 1, Sell Leg 2
            system_state.petal_pnl = (petal_ltp - system_state.petal_entry_price) * p_spec["pnl_multiplier"] * qty
            system_state.mini_pnl = (system_state.mini_entry_price - mini_ltp) * m_spec["pnl_multiplier"] * qty
        else:  # Sell Leg 1, Buy Leg 2
            system_state.petal_pnl = (system_state.petal_entry_price - petal_ltp) * p_spec["pnl_multiplier"] * qty
            system_state.mini_pnl = (mini_ltp - system_state.mini_entry_price) * m_spec["pnl_multiplier"] * qty
            
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
            t_petal_symbol = trade.get("petal_symbol") or system_state.petal_symbol
            t_mini_symbol = trade.get("mini_symbol") or system_state.mini_symbol
            t_p_spec = get_gold_contract_specs(t_petal_symbol)
            t_m_spec = get_gold_contract_specs(t_mini_symbol)
            t_petal_ltp = system_state.symbol_ltps.get(t_petal_symbol) or petal_ltp
            t_mini_ltp = system_state.symbol_ltps.get(t_mini_symbol) or mini_ltp
            
            if t_dir == "Expansion":
                t_petal_pnl = (t_petal_ltp - trade.get("petal_entry_price", 0.0)) * t_p_spec["pnl_multiplier"] * t_qty
                t_mini_pnl = (trade.get("mini_entry_price", 0.0) - t_mini_ltp) * t_m_spec["pnl_multiplier"] * t_qty
            else:
                t_petal_pnl = (trade.get("petal_entry_price", 0.0) - t_petal_ltp) * t_p_spec["pnl_multiplier"] * t_qty
                t_mini_pnl = (t_mini_ltp - trade.get("mini_entry_price", 0.0)) * t_m_spec["pnl_multiplier"] * t_qty
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
            t_p_spec = get_gold_contract_specs(t_petal_symbol)
            t_m_spec = get_gold_contract_specs(t_mini_symbol)
            t_petal_ltp = system_state.symbol_ltps.get(t_petal_symbol) or petal_ltp
            t_mini_ltp = system_state.symbol_ltps.get(t_mini_symbol) or mini_ltp
            
            if t_dir == "Expansion":
                t_petal_pnl = (t_petal_ltp - trade.get("petal_entry_price", 0.0)) * t_p_spec["pnl_multiplier"] * t_qty
                t_mini_pnl = (trade.get("mini_entry_price", 0.0) - t_mini_ltp) * t_m_spec["pnl_multiplier"] * t_qty
            else:
                t_petal_pnl = (trade.get("petal_entry_price", 0.0) - t_petal_ltp) * t_p_spec["pnl_multiplier"] * t_qty
                t_mini_pnl = (t_mini_ltp - trade.get("mini_entry_price", 0.0)) * t_m_spec["pnl_multiplier"] * t_qty
            trade["petal_pnl"] = t_petal_pnl
            trade["mini_pnl"] = t_mini_pnl
            trade["unrealized_pnl"] = t_petal_pnl + t_mini_pnl
            ta_unrealized_pnl += trade["unrealized_pnl"]
            ta_used_margin += 50000.0 * t_qty
            
    # Calculate live Trade Automation with Lots trades P&Ls and margins
    ta_lots_unrealized_pnl = 0.0
    ta_lots_used_margin = 0.0
    for trade in getattr(system_state, "ta_lots_trades", []):
        if trade.get("status") == "Open":
            t_qty = trade.get("quantity", 1)
            t_dir = trade.get("direction")
            t_petal_symbol = trade.get("petal_symbol")
            t_mini_symbol = trade.get("mini_symbol")
            t_p_spec = get_gold_contract_specs(t_petal_symbol)
            t_m_spec = get_gold_contract_specs(t_mini_symbol)
            t_petal_ltp = system_state.symbol_ltps.get(t_petal_symbol) or petal_ltp
            t_mini_ltp = system_state.symbol_ltps.get(t_mini_symbol) or mini_ltp
            
            if t_dir == "Expansion":
                t_petal_pnl = (t_petal_ltp - trade.get("petal_entry_price", 0.0)) * t_p_spec["pnl_multiplier"] * t_qty
                t_mini_pnl = (trade.get("mini_entry_price", 0.0) - t_mini_ltp) * t_m_spec["pnl_multiplier"] * t_qty
            else:
                t_petal_pnl = (trade.get("petal_entry_price", 0.0) - t_petal_ltp) * t_p_spec["pnl_multiplier"] * t_qty
                t_mini_pnl = (t_mini_ltp - trade.get("mini_entry_price", 0.0)) * t_m_spec["pnl_multiplier"] * t_qty
            trade["petal_pnl"] = t_petal_pnl
            trade["mini_pnl"] = t_mini_pnl
            trade["unrealized_pnl"] = t_petal_pnl + t_mini_pnl
            ta_lots_unrealized_pnl += trade["unrealized_pnl"]
            ta_lots_used_margin += 50000.0 * t_qty

    system_state.used_margin += manual_used_margin + ta_used_margin + ta_lots_used_margin
    system_state.total_pnl = system_state.realized_pnl + system_state.unrealized_pnl + manual_unrealized_pnl + ta_unrealized_pnl + ta_lots_unrealized_pnl
    system_state.available_balance = system_state.total_capital - system_state.used_margin + system_state.total_pnl
    if system_state.total_capital > 0:
        system_state.returns_percentage = (system_state.total_pnl / system_state.total_capital) * 100.0
    else:
        system_state.returns_percentage = 0.0
        
    session_status = get_market_session_status()
    if session_status == "SUSPENDED":
        # Check specifically for evening shutdown window (23:27 to 23:59)
        now = get_ist_time()
        if now.hour == 23 and now.minute >= 27:
            system_state.log("[EMERGENCY SHUTDOWN] Time is 23:27 or later. Shutting down server immediately.")
            print("[EMERGENCY SHUTDOWN] Time is 23:27 or later. Shutting down server immediately.")
            os._exit(0)

    # If the session is HOLD or SUSPENDED, skip all trade execution and automated checks.
    if session_status in ["HOLD", "SUSPENDED"]:
        await broadcast_system_state()
        return

    # Process pending manual trade triggers across ALL month pairs independently (skip if Hold or Halted)
    if system_state.system_status not in ["Halted", "Hold"]:
        for trade in system_state.manual_trades:
            if trade.get("status") == "Pending":
                t_petal_symbol = trade.get("petal_symbol") or system_state.petal_symbol
                t_mini_symbol = trade.get("mini_symbol") or system_state.mini_symbol
                
                # Resolve live depth spreads for this specific pair
                live_stat = None
                if t_petal_symbol == system_state.petal_symbol and t_mini_symbol == system_state.mini_symbol:
                    live_stat = {
                        "depth_buy_spread": system_state.depth_buy_spread,
                        "depth_sell_spread": system_state.depth_sell_spread
                    }
                else:
                    for stat in system_state.month_master_live:
                        if stat.get("petal_symbol") == t_petal_symbol and stat.get("mini_symbol") == t_mini_symbol:
                            live_stat = stat
                            break
                
                if not live_stat:
                    continue
                    
                buy_spread = live_stat["depth_buy_spread"]
                sell_spread = live_stat["depth_sell_spread"]
                
                triggered = False
                expected_spread = 0.0
                if trade.get("direction") == "Expansion":
                    if buy_spread <= trade.get("trigger_diff", 0.0):
                        triggered = True
                        expected_spread = buy_spread
                elif trade.get("direction") == "Contraction":
                    if sell_spread >= trade.get("trigger_diff", 0.0):
                        triggered = True
                        expected_spread = sell_spread
                
                if triggered:
                    can_exec, rem = check_global_order_cooldown()
                    if not can_exec:
                        continue
                    trade["status"] = "Executing"
                    system_state.last_global_order_time = time.time()
                    asyncio.create_task(trigger_manual_trade_execution(trade, expected_spread))
                    break
        
    # Check execution lock or halted / hold state: skip automations to prevent overlaps
    if system_state.execution_in_progress or system_state.system_status in ["Halted", "Hold"]:
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
            can_exec, rem = check_global_order_cooldown()
            if can_exec:
                buffer = system_state.spread_buffer
                
                # Expansion Entry Condition: depth_buy_spread <= entry_threshold + buffer
                if system_state.depth_buy_spread <= (system_state.entry_threshold + buffer):
                    asyncio.create_task(run_auto_entry("Expansion", "BUY", "SELL", system_state.entry_threshold))
                    
                # Contraction Entry Condition: depth_sell_spread >= target_threshold - buffer (Only if Contraction is enabled)
                elif system_state.auto_contraction_enabled and (system_state.depth_sell_spread >= (system_state.target_threshold - buffer)):
                    asyncio.create_task(run_auto_entry("Contraction", "SELL", "BUY", system_state.target_threshold))
 
    # Process Trade Automation Strategy Checks (Entries & Autonomous Exits)
    if system_state.ta_configs or any(t.get("status") == "Open" for t in system_state.ta_trades):
        asyncio.create_task(run_trade_automation_checks())

    # Process Trade Automation with Lots Strategy Checks
    if getattr(system_state, "ta_lots_configs", None) or any(t.get("status") == "Open" for t in getattr(system_state, "ta_lots_trades", [])):
        asyncio.create_task(run_trade_automation_lots_checks())

    # Process Manual BOT Strategy Checks (Tolerance Range & 10s Cooldown)
    if system_state.manual_bot.get("active", False) or any(b.get("active", False) for b in getattr(system_state, "manual_bots", [])):
        asyncio.create_task(run_manual_bot_check())

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
        
        if p_ltp > 0 and m_ltp > 0:
            p_spec = get_gold_contract_specs(p_sym)
            m_spec = get_gold_contract_specs(m_sym)
            p_scale = p_spec["price_scale_to_10g"]
            m_scale = m_spec["price_scale_to_10g"]
            
            mm_spread = (p_ltp * p_scale) - (m_ltp * m_scale)
            
            p_depth = p_q.get("depth")
            if not p_depth or not p_depth.get("buy") or not p_depth.get("sell"):
                p_depth = generate_simulated_depth(p_ltp)
                
            m_depth = m_q.get("depth")
            if not m_depth or not m_depth.get("buy") or not m_depth.get("sell"):
                m_depth = generate_simulated_depth(m_ltp)
                
            mm_qty = system_state.trade_quantity
            p_req_qty = mm_qty * p_spec["lots_per_unit_mini"]
            m_req_qty = mm_qty * m_spec["lots_per_unit_mini"]
            
            avg_p_buy = get_depth_average_price(p_depth, "sell", p_req_qty, p_ltp)
            avg_m_sell = get_depth_average_price(m_depth, "buy", m_req_qty, m_ltp)
            mm_buy_spread = (avg_p_buy * p_scale) - (avg_m_sell * m_scale)
            
            avg_p_sell = get_depth_average_price(p_depth, "buy", p_req_qty, p_ltp)
            avg_m_buy = get_depth_average_price(m_depth, "sell", m_req_qty, m_ltp)
            mm_sell_spread = (avg_p_sell * p_scale) - (avg_m_buy * m_scale)
            
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

            # Record Daily High / Low statistics for Spread and Prices (Petal & Mini)
            if mapping.get("capture_data", True):
                update_daily_high_low_stats(p_sym, m_sym, mm_spread, p_ltp, m_ltp, buy_spread=mm_buy_spread, sell_spread=mm_sell_spread)
                ai_arbitrage_engine.process_tick(p_sym, m_sym, mm_spread, mm_buy_spread, mm_sell_spread)
            
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

# ----------------- Safe Angel One Market Quotes Fetcher -----------------
def safe_fetch_angelone_market_quotes(smart_client, tokens: list) -> dict:
    if not smart_client or not tokens:
        return {}
    for attempt in range(2):
        try:
            res = smart_client.getMarketData(
                mode="FULL",
                exchangeTokens={"MCX": tokens}
            )
            if isinstance(res, dict):
                return res
            if attempt == 0:
                time.sleep(0.15)
                continue
            return {}
        except Exception as e:
            err_str = str(e).lower()
            if attempt == 0 and any(k in err_str for k in ["connection", "reset", "abort", "peer", "timeout", "broken pipe"]):
                try:
                    if hasattr(smart_client, "reqsession") and smart_client.reqsession:
                        smart_client.reqsession.close()
                except Exception:
                    pass
                time.sleep(0.15)
                continue
            raise e
    return {}

# ----------------- Dynamic Random Walk Ticker (Live Ticks) -----------------
async def live_mcx_ticker_task():
    # Fluctuate realistic prices resembling MCX commodity values
    petal_base = 7200.0
    mini_base = 71150.0
    tick_count = 0
    cached_quotes_map = {}
    consecutive_feed_errors = 0
    
    while True:
        try:
            # If an order is currently executing and verifying fill with broker, pause quote requests to avoid hitting API rate limits
            if getattr(system_state, "order_execution_in_progress", False):
                await asyncio.sleep(1.0)
                continue

            tick_count += 1
            # 1. Try to fetch from Active Broker API
            if system_state.broker == "AngelOne":
                if not system_state.smart_connect:
                    system_state.api_connected = False
                else:
                    try:
                        # Priority: Query active contracts on every tick; rotate full month master every 3rd tick (~3-4s)
                        primary_tokens = [t for t in [system_state.petal_token, system_state.mini_token] if t]
                        tokens_to_query = set(primary_tokens)
                        if tick_count % 3 == 0:
                            for m in system_state.month_master:
                                if m.get("petal_token"):
                                    tokens_to_query.add(m["petal_token"])
                                if m.get("mini_token"):
                                    tokens_to_query.add(m["mini_token"])
                        tokens_to_query = [t for t in tokens_to_query if t]

                        loop = asyncio.get_running_loop()
                        market_quotes = await loop.run_in_executor(
                            None,
                            lambda: safe_fetch_angelone_market_quotes(system_state.smart_connect, tokens_to_query)
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
                                        cached_quotes_map[tok] = item
                                        if tok == system_state.petal_token:
                                            petal_quote = item
                                        elif tok == system_state.mini_token:
                                            mini_quote = item
                        
                        # Smooth fallback for active contracts if single tick had glitch
                        if not petal_quote and system_state.petal_token in cached_quotes_map:
                            petal_quote = cached_quotes_map[system_state.petal_token]
                        if not mini_quote and system_state.mini_token in cached_quotes_map:
                            mini_quote = cached_quotes_map[system_state.mini_token]

                        system_state.month_master_live = calculate_month_master_live_stats(cached_quotes_map)
 
                        petal_ltp = float(petal_quote.get("ltp", 0.0))
                        mini_ltp = float(mini_quote.get("ltp", 0.0))
                        
                        if petal_ltp > 0 and mini_ltp > 0:
                            consecutive_feed_errors = 0
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
                            consecutive_feed_errors += 1
                            if consecutive_feed_errors >= 3:
                                system_state.api_connected = False
                    except Exception as e:
                        consecutive_feed_errors += 1
                        if consecutive_feed_errors >= 3:
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
                            
                        # Priority: Query active contracts on every tick; rotate full month master every 3rd tick (~3-4s)
                        primary_tokens = [t for t in [a1_petal_token, a1_mini_token] if t]
                        tokens_to_query = set(primary_tokens)
                        if tick_count % 3 == 0:
                            for m in system_state.month_master:
                                if m.get("petal_token"):
                                    tokens_to_query.add(m["petal_token"])
                                if m.get("mini_token"):
                                    tokens_to_query.add(m["mini_token"])
                        tokens_to_query = [t for t in tokens_to_query if t]

                        loop = asyncio.get_running_loop()
                        market_quotes = await loop.run_in_executor(
                            None,
                            lambda: safe_fetch_angelone_market_quotes(system_state.smart_connect, tokens_to_query)
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
                                        cached_quotes_map[tok] = item
                                        if tok == a1_petal_token:
                                            petal_quote = item
                                        elif tok == a1_mini_token:
                                            mini_quote = item
                        
                        # Smooth fallback for active contracts if single tick had glitch
                        if not petal_quote and a1_petal_token in cached_quotes_map:
                            petal_quote = cached_quotes_map[a1_petal_token]
                        if not mini_quote and a1_mini_token in cached_quotes_map:
                            mini_quote = cached_quotes_map[a1_mini_token]

                        system_state.month_master_live = calculate_month_master_live_stats(cached_quotes_map)
                                            
                        petal_ltp = float(petal_quote.get("ltp", 0.0))
                        mini_ltp = float(mini_quote.get("ltp", 0.0))
                        
                        if petal_ltp > 0 and mini_ltp > 0:
                            consecutive_feed_errors = 0
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
                            consecutive_feed_errors += 1
                            if consecutive_feed_errors >= 3:
                                system_state.api_connected = False
                    except Exception as e:
                        consecutive_feed_errors += 1
                        if consecutive_feed_errors >= 3:
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
                lotsize_raw = item.get("lotsize", "1")
                try:
                    lotsize_val = int(lotsize_raw)
                except (ValueError, TypeError):
                    lotsize_val = 1
                if exch == "MCX" and symbol and token:
                    sym_u = symbol.upper()
                    system_state.mcx_tokens_cache[sym_u] = token
                    system_state.mcx_tokens_cache[sym_u.removesuffix("FUT")] = token
                    system_state.mcx_tokens_cache[f"{sym_u.removesuffix('FUT')}FUT"] = token
                    system_state.mcx_official_symbols[token] = sym_u
                    system_state.mcx_lotsize_cache[sym_u] = lotsize_val
                    system_state.mcx_lotsize_cache[sym_u.removesuffix("FUT")] = lotsize_val
                    system_state.mcx_lotsize_cache[f"{sym_u.removesuffix('FUT')}FUT"] = lotsize_val
                    system_state.mcx_lotsize_cache[token] = lotsize_val
                    if item.get("expiry"):
                        system_state.mcx_expiry_cache[sym_u] = item.get("expiry")
                    if any(symbol.startswith(p) for p in ["GOLDPETAL", "GOLDM", "GOLDTEN", "GOLD10", "GOLDGUINEA"]):
                        results.append({
                            "symbol": symbol,
                            "token": token,
                            "expiry": item.get("expiry"),
                            "name": item.get("name"),
                            "lotsize": lotsize_val
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
                        if any(sym_u.startswith(p) for p in ["GOLDPETAL", "GOLDM", "GOLDTEN", "GOLD10", "GOLDGUINEA"]):
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
    asyncio.create_task(ai_arbitrage_engine.alert_loop())
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
            "ta_lots_configs": getattr(system_state, "ta_lots_configs", []),
            "ta_lots_trades": getattr(system_state, "ta_lots_trades", []),
            "manual_bot": system_state.manual_bot,
            "manual_bots": getattr(system_state, "manual_bots", []),
            
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
    petal_symbol: str = None
    mini_symbol: str = None

@app.post("/api/entry")
async def api_entry(payload: EntryPayload, token: str = None, authorization: str = Header(None)):
    verify_token(token, authorization)
    
    session_status = get_market_session_status()
    if session_status == "HOLD":
        raise HTTPException(status_code=400, detail="Trading is suspended during market hold window.")
    elif session_status == "SUSPENDED":
        raise HTTPException(status_code=400, detail="Trading is suspended after market close (23:27 - 09:00).")

    if system_state.system_status == "Halted":
        raise HTTPException(status_code=400, detail="Terminal is Halted due to Kill Switch or Stop Loss.")
        
    if payload.direction not in ["Expansion", "Contraction"]:
        raise HTTPException(status_code=400, detail="Invalid trade direction selected.")
        
    qty = payload.quantity if payload.quantity is not None else system_state.trade_quantity
    target_petal = payload.petal_symbol or system_state.petal_symbol
    target_mini = payload.mini_symbol or system_state.mini_symbol
    
    if payload.trigger_diff is not None:
        # Create pending manual trade
        trade_id = len(system_state.manual_trades) + 1
        new_trade = {
            "id": trade_id,
            "direction": payload.direction,
            "quantity": qty,
            "trigger_diff": payload.trigger_diff,
            "status": "Pending",
            "entry_time": get_ist_time_str("%H:%M:%S"),
            "entry_date": get_ist_time_str("%Y-%m-%d"),
            "petal_symbol": target_petal,
            "mini_symbol": target_mini,
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
        system_state.log(f"MANUAL PENDING ENTRY CREATED: ID {trade_id}, Dir {payload.direction}, Trigger Diff {payload.trigger_diff}, Qty {qty} ({target_petal}/{target_mini})")
        await broadcast_system_state()
        return {"status": "SUCCESS", "message": f"Pending manual trade ID {trade_id} created."}
        
    # Immediate execution
    can_exec, rem = check_global_order_cooldown()
    if not can_exec:
        raise HTTPException(status_code=400, detail=f"Order cooldown active: Please wait {rem}s before placing new order (minimum 10s gap between orders).")
    system_state.last_global_order_time = time.time()
    
    expected_entry_spread = system_state.depth_buy_spread if payload.direction == "Expansion" else system_state.depth_sell_spread
    
    result = await execute_netting_manual_trades(
        payload.direction, qty, expected_entry_spread,
        petal_symbol=target_petal, mini_symbol=target_mini
    )
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
        
    can_exec, rem = check_global_order_cooldown()
    if not can_exec:
        raise HTTPException(status_code=400, detail=f"Order cooldown active: Please wait {rem}s before squaring off (minimum 10s gap between orders).")
    system_state.last_global_order_time = time.time()
        
    if system_state.position_direction == "Expansion":
        system_state.expected_exit_spread = system_state.depth_sell_spread
    else:
        system_state.expected_exit_spread = system_state.depth_buy_spread
        
    await execute_position_exit(exit_reason="Manual-Exit")
    return {"status": "SUCCESS", "message": "Position closed successfully."}

class ExitManualPayload(BaseModel):
    trade_id: Union[int, str]

@app.post("/api/cancel-manual")
async def api_cancel_manual(payload: ExitManualPayload, token: str = None, authorization: str = Header(None)):
    verify_token(token, authorization)
    
    target_id = str(payload.trade_id).strip()
    new_trades = []
    removed_any = False
    for i, t in enumerate(system_state.manual_trades):
        t_id = str(t.get("id", "")).strip()
        idx_str = str(i + 1)
        if t_id == target_id or idx_str == target_id:
            removed_any = True
            system_state.log(f"MANUAL TRADE ID {t.get('id', target_id)} (Status: {t.get('status')}) CANCELLED AND REMOVED.")
        else:
            new_trades.append(t)
            
    system_state.manual_trades = new_trades
    system_state.save_manual_trades()
    await broadcast_system_state()
    return {"status": "SUCCESS", "message": f"Manual trade ID {target_id} cancelled and removed."}

@app.post("/api/exit-manual")
async def api_exit_manual(payload: ExitManualPayload, token: str = None, authorization: str = Header(None)):
    verify_token(token, authorization)
    
    trade = None
    target_id = str(payload.trade_id).strip()
    for i, t in enumerate(system_state.manual_trades):
        t_id = str(t.get("id", "")).strip()
        if t_id == target_id or str(i + 1) == target_id:
            trade = t
            break
            
    if not trade:
        return {"status": "SUCCESS", "message": f"Manual trade ID {payload.trade_id} not found or already closed."}
        
    status_lower = str(trade.get("status", "")).strip().lower()
    
    # If trade is NOT Open (e.g. Pending, pending, Failed, Cancelled, empty, etc.), cancel and remove it!
    if status_lower != "open":
        matched_id = trade.get("id")
        system_state.manual_trades = [t for t in system_state.manual_trades if str(t.get("id")) != str(matched_id)]
        system_state.log(f"MANUAL PENDING/INACTIVE ENTRY ID {matched_id} CANCELLED AND REMOVED.")
        system_state.save_manual_trades()
        await broadcast_system_state()
        return {"status": "SUCCESS", "message": f"Pending trade ID {matched_id} cancelled and removed."}
        
    can_exec, rem = check_global_order_cooldown()
    if not can_exec:
        raise HTTPException(status_code=400, detail=f"Order cooldown active: Please wait {rem}s before squaring off (minimum 10s gap between orders).")
    system_state.last_global_order_time = time.time()
        
    direction = trade["direction"]
    petal_action = "SELL" if direction == "Expansion" else "BUY"
    mini_action = "BUY" if direction == "Expansion" else "SELL"
    
    p_sym = trade.get("petal_symbol") or system_state.petal_symbol
    m_sym = trade.get("mini_symbol") or system_state.mini_symbol
    p_tok, m_tok = system_state.get_tokens_for_pair(p_sym, m_sym)
    
    # Resolve live spread for this pair
    if p_sym == system_state.petal_symbol and m_sym == system_state.mini_symbol:
        expected_exit_spread = system_state.depth_sell_spread if direction == "Expansion" else system_state.depth_buy_spread
    else:
        for stat in system_state.month_master_live:
            if stat.get("petal_symbol") == p_sym and stat.get("mini_symbol") == m_sym:
                expected_exit_spread = stat["depth_sell_spread"] if direction == "Expansion" else stat["depth_buy_spread"]
                break
        else:
            expected_exit_spread = 0.0
            
    system_state.log(f"[MANUAL EXIT] Squaring off manual trade ID {trade['id']} ({direction}) for {p_sym}/{m_sym}...")
    
    result = await execute_trade(
        petal_action, mini_action, 
        check_liquidity=False, 
        is_entry=False, 
        qty=trade["quantity"],
        alt_petal_symbol=p_sym,
        alt_petal_token=p_tok,
        alt_mini_symbol=m_sym,
        alt_mini_token=m_tok
    )
    if not result["success"]:
        raise HTTPException(status_code=400, detail=f"Square off trade execution failed: {result['reason']}")
        
    petal_exit = result["petal_fill_price"]
    mini_exit = result["mini_fill_price"]
    petal_exit_type = result["petal_order_type"]
    mini_exit_type = result["mini_order_type"]
    
    p_spec = get_gold_contract_specs(p_sym)
    m_spec = get_gold_contract_specs(m_sym)
    actual_exit_spread = (petal_exit * p_spec["price_scale_to_10g"]) - (mini_exit * m_spec["price_scale_to_10g"])
    expected_exit_spread = system_state.depth_sell_spread if direction == "Expansion" else system_state.depth_buy_spread
    
    qty = trade["quantity"]
    if direction == "Expansion":
        p_pnl = (petal_exit - trade["petal_entry_price"]) * p_spec["pnl_multiplier"] * qty
        m_pnl = (trade["mini_entry_price"] - mini_exit) * m_spec["pnl_multiplier"] * qty
        exit_slippage = expected_exit_spread - actual_exit_spread
    else:
        p_pnl = (trade["petal_entry_price"] - petal_exit) * p_spec["pnl_multiplier"] * qty
        m_pnl = (mini_exit - trade["mini_entry_price"]) * m_spec["pnl_multiplier"] * qty
        exit_slippage = actual_exit_spread - expected_exit_spread
        
    trade_pnl = p_pnl + m_pnl
    charges = system_state.calculate_mcx_charges(
        direction, qty, trade["petal_entry_price"], trade["mini_entry_price"], petal_exit, mini_exit,
        petal_symbol=p_sym, mini_symbol=m_sym
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
    trade["exit_time"] = get_ist_time_str("%H:%M:%S")
    trade["exit_date"] = get_ist_time_str("%Y-%m-%d")
    
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
    trade_id: Union[int, str]

@app.post("/api/dismiss-manual")
async def api_dismiss_manual(payload: DismissManualPayload, token: str = None, authorization: str = Header(None)):
    verify_token(token, authorization)
    target_id = str(payload.trade_id).strip()
    system_state.manual_trades = [t for i, t in enumerate(system_state.manual_trades) if str(t.get("id", "")).strip() != target_id and str(i + 1) != target_id]
    system_state.save_manual_trades()
    await broadcast_system_state()
    return {"status": "SUCCESS", "message": f"Manual trade ID {payload.trade_id} dismissed."}

@app.post("/api/clear-manual-trades")
async def api_clear_manual_trades(token: str = None, authorization: str = Header(None)):
    verify_token(token, authorization)
    initial_count = len(system_state.manual_trades)
    # Keep only Open trades, clear all Cancelled, Failed, Closed trades
    system_state.manual_trades = [t for t in system_state.manual_trades if str(t.get("status", "")).strip().lower() == "open"]
    removed_count = initial_count - len(system_state.manual_trades)
    system_state.save_manual_trades()
    await broadcast_system_state()
    system_state.log(f"MANUAL TRADES CLEANUP: Removed {removed_count} inactive trades.")
    return {"status": "SUCCESS", "message": f"Removed {removed_count} inactive trades."}

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
            system_state.manual_trades.remove(trade)
            system_state.log(f"MANUAL PENDING ENTRY ID {trade.get('id')} CANCELLED AND REMOVED due to Kill Switch.")
        elif trade.get("status") == "Open":
            direction = trade["direction"]
            petal_action = "SELL" if direction == "Expansion" else "BUY"
            mini_action = "BUY" if direction == "Expansion" else "SELL"
            qty = trade["quantity"]
            p_sym = trade.get("petal_symbol") or system_state.petal_symbol
            m_sym = trade.get("mini_symbol") or system_state.mini_symbol
            p_tok, m_tok = system_state.get_tokens_for_pair(p_sym, m_sym)
            p_spec = get_gold_contract_specs(p_sym)
            m_spec = get_gold_contract_specs(m_sym)
            
            result = await execute_trade(
                petal_action, mini_action, 
                check_liquidity=False, 
                is_entry=False, 
                qty=qty,
                alt_petal_symbol=p_sym,
                alt_petal_token=p_tok,
                alt_mini_symbol=m_sym,
                alt_mini_token=m_tok
            )
            if result["success"]:
                petal_exit = result["petal_fill_price"]
                mini_exit = result["mini_fill_price"]
                actual_exit_spread = (petal_exit * p_spec["price_scale_to_10g"]) - (mini_exit * m_spec["price_scale_to_10g"])
                expected_exit_spread = system_state.depth_sell_spread if direction == "Expansion" else system_state.depth_buy_spread
                
                if direction == "Expansion":
                    p_pnl = (petal_exit - trade["petal_entry_price"]) * p_spec["pnl_multiplier"] * qty
                    m_pnl = (trade["mini_entry_price"] - mini_exit) * m_spec["pnl_multiplier"] * qty
                    exit_slippage = expected_exit_spread - actual_exit_spread
                else:
                    p_pnl = (trade["petal_entry_price"] - petal_exit) * p_spec["pnl_multiplier"] * qty
                    m_pnl = (mini_exit - trade["mini_entry_price"]) * m_spec["pnl_multiplier"] * qty
                    exit_slippage = actual_exit_spread - expected_exit_spread
                    
                trade_pnl = p_pnl + m_pnl
                charges = system_state.calculate_mcx_charges(
                    direction, qty, trade["petal_entry_price"], trade["mini_entry_price"], petal_exit, mini_exit,
                    petal_symbol=p_sym, mini_symbol=m_sym
                )
                net_pnl = trade_pnl - charges
                
                trade["status"] = "Closed"
                trade["petal_exit_price"] = petal_exit
                trade["mini_exit_price"] = mini_exit
                trade["pnl"] = net_pnl
                trade["charges"] = charges
                trade["exit_time"] = get_ist_time_str("%H:%M:%S")
                trade["exit_date"] = get_ist_time_str("%Y-%m-%d")
                
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
            # Find config to get paper mode, fallback to trade's own paper_mode
            paper_mode = trade.get("paper_mode", True)
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

class ToggleHoldPayload(BaseModel):
    action: Optional[str] = None

@app.post("/api/toggle-hold-system")
async def api_toggle_hold_system(payload: Optional[ToggleHoldPayload] = None, token: str = None, authorization: str = Header(None)):
    verify_token(token, authorization)
    
    current_status = getattr(system_state, "system_status", "Active")
    desired = payload.action.strip().lower() if (payload and payload.action) else None
    
    if desired == "hold" or (desired is None and current_status != "Hold"):
        system_state.system_status = "Hold"
        system_state.log("[SYSTEM HOLD] ALL TRADE TRIGGERS ARE NOW ON HOLD. You can safely add, modify, or delete orders.")
    else:
        # Before resuming, reload all configs and manual trades from disk to guarantee deleted orders NEVER re-execute
        system_state.load_manual_trades()
        system_state.load_ta_configs()
        system_state.load_ta_trades()
        if hasattr(system_state, "load_ta_lots_configs"):
            system_state.load_ta_lots_configs()
        if hasattr(system_state, "load_ta_lots_trades"):
            system_state.load_ta_lots_trades()
        if hasattr(system_state, "load_manual_bot"):
            system_state.load_manual_bot()
            
        system_state.system_status = "Active"
        system_state.log("[SYSTEM RESUME] System resumed to ACTIVE. Fresh state reloaded from disk; all deleted orders purged.")
        
    await broadcast_system_state()
    return {
        "status": "SUCCESS", 
        "system_status": system_state.system_status,
        "message": f"System status set to {system_state.system_status}."
    }

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
    auto_sl_enabled: bool = False
    auto_sl_val: float = -3000.0
    auto_square_off_enabled: bool = False
    auto_square_off_time: str = "23:30"
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
    petal_symbol: str
    petal_token: str
    mini_symbol: str
    mini_token: str
    capture_data: Optional[bool] = True

class MonthMasterPayload(BaseModel):
    mappings: List[MonthMasterMapping]

@app.get("/api/month-master")
async def api_get_month_master(token: str = None, authorization: str = Header(None)):
    verify_token(token, authorization)
    return {"status": "SUCCESS", "mappings": system_state.month_master}

@app.post("/api/month-master")
async def api_post_month_master(payload: MonthMasterPayload, token: str = None, authorization: str = Header(None)):
    verify_token(token, authorization)
    system_state.month_master = [m.dict() for m in payload.mappings]
    system_state.save_month_master()
    await broadcast_system_state()
    return {"status": "SUCCESS", "message": "Month Master mappings updated successfully."}

class MonthMasterToggleCapturePayload(BaseModel):
    index: int
    capture_data: bool

@app.post("/api/month-master-toggle-capture")
async def api_month_master_toggle_capture(payload: MonthMasterToggleCapturePayload, token: str = None, authorization: str = Header(None)):
    verify_token(token, authorization)
    if 0 <= payload.index < len(system_state.month_master):
        system_state.month_master[payload.index]["capture_data"] = payload.capture_data
        system_state.save_month_master()
        await broadcast_system_state()
        return {"status": "SUCCESS", "message": "Capture setting updated"}
    raise HTTPException(status_code=400, detail="Invalid month master index")

class ManualBotStartPayload(BaseModel):
    id: Optional[str] = None
    direction: str = "Expansion"
    petal_symbol: str = ""
    mini_symbol: str = ""
    trigger_diff: float = 1400.0
    diff_gap: float = 20.0
    total_orders: int = 5
    quantity: int = 1

class ManualBotStopPayload(BaseModel):
    id: Optional[str] = None
    stop_all: Optional[bool] = False

class ManualBotDeletePayload(BaseModel):
    id: str

@app.post("/api/manual-bot/start")
async def api_manual_bot_start(payload: ManualBotStartPayload, token: str = None, authorization: str = Header(None)):
    verify_token(token, authorization)
    
    if payload.direction not in ["Expansion", "Contraction"]:
        raise HTTPException(status_code=400, detail="Invalid trade direction.")
    if payload.total_orders < 1 or payload.total_orders > 50:
        raise HTTPException(status_code=400, detail="Total orders must be between 1 and 50.")
    if payload.quantity < 1:
        raise HTTPException(status_code=400, detail="Quantity must be at least 1.")

    p_sym = payload.petal_symbol or system_state.petal_symbol
    m_sym = payload.mini_symbol or system_state.mini_symbol

    min_p = payload.trigger_diff - payload.diff_gap if payload.direction == "Expansion" else payload.trigger_diff
    max_p = payload.trigger_diff if payload.direction == "Expansion" else payload.trigger_diff + payload.diff_gap

    if not hasattr(system_state, "manual_bots") or not isinstance(system_state.manual_bots, list):
        system_state.manual_bots = []

    # Check if editing an existing task
    target_bot = None
    if payload.id:
        for b in system_state.manual_bots:
            if str(b.get("id")) == str(payload.id):
                target_bot = b
                break

    if target_bot:
        # Edit existing bot task
        target_bot["active"] = True
        target_bot["direction"] = payload.direction
        target_bot["petal_symbol"] = p_sym
        target_bot["mini_symbol"] = m_sym
        target_bot["trigger_diff"] = float(payload.trigger_diff)
        target_bot["diff_gap"] = float(payload.diff_gap)
        target_bot["total_orders"] = int(payload.total_orders)
        target_bot["quantity"] = int(payload.quantity)
        target_bot["status_message"] = f"Active: Scanning {payload.direction} spread [{min_p:.1f} - {max_p:.1f}]"
        target_bot["why_waiting"] = f"Scanning spread in [{min_p:.1f} - {max_p:.1f}]"
        system_state.manual_bot = target_bot
        system_state.log(f"[MANUAL BOT] Updated Task #{payload.id} for {p_sym}/{m_sym}: Target {payload.trigger_diff}, Gap {payload.diff_gap}, Orders {payload.total_orders}")
    else:
        # Add new bot task
        bot_idx = len(system_state.manual_bots) + 1
        new_id = f"MB-{bot_idx}"
        new_bot = {
            "id": new_id,
            "active": True,
            "direction": payload.direction,
            "petal_symbol": p_sym,
            "mini_symbol": m_sym,
            "trigger_diff": float(payload.trigger_diff),
            "diff_gap": float(payload.diff_gap),
            "total_orders": int(payload.total_orders),
            "filled_orders": 0,
            "quantity": int(payload.quantity),
            "order_delay": 10.0,
            "last_order_time": 0.0,
            "status_message": f"Active: Scanning {payload.direction} spread in range [{min_p:.1f} - {max_p:.1f}]",
            "why_waiting": f"Scanning spread in [{min_p:.1f} - {max_p:.1f}]",
            "created_time": get_ist_time_str("%H:%M:%S"),
            "trades": []
        }
        system_state.manual_bots.append(new_bot)
        system_state.manual_bot = new_bot
        system_state.log(f"[MANUAL BOT] Added New Task #{new_id} for {p_sym}/{m_sym}: Target {payload.trigger_diff}, Gap {payload.diff_gap} [{min_p:.1f} - {max_p:.1f}], Orders {payload.total_orders}, Qty {payload.quantity}, 10s Cooldown")

    system_state.save_manual_bot()
    await broadcast_system_state()
    return {"status": "SUCCESS", "message": "Manual BOT configured and started.", "manual_bot": system_state.manual_bot, "manual_bots": system_state.manual_bots}

@app.post("/api/manual-bot/stop")
async def api_manual_bot_stop(payload: Optional[ManualBotStopPayload] = None, token: str = None, authorization: str = Header(None)):
    verify_token(token, authorization)
    bot_id = payload.id if payload else None
    stop_all = payload.stop_all if payload else False

    if stop_all or not bot_id:
        system_state.manual_bot["active"] = False
        system_state.manual_bot["status_message"] = "Stopped by user."
        system_state.manual_bot["why_waiting"] = "Stopped by user."
        for b in getattr(system_state, "manual_bots", []):
            b["active"] = False
            b["status_message"] = "Stopped by user."
            b["why_waiting"] = "Stopped by user."
        system_state.log("[MANUAL BOT] All tasks stopped by user.")
    else:
        for b in getattr(system_state, "manual_bots", []):
            if str(b.get("id")) == str(bot_id):
                b["active"] = False
                b["status_message"] = "Stopped by user."
                b["why_waiting"] = "Stopped by user."
                system_state.log(f"[MANUAL BOT] Task #{bot_id} stopped by user.")
                break
        if str(system_state.manual_bot.get("id")) == str(bot_id):
            system_state.manual_bot["active"] = False
            system_state.manual_bot["status_message"] = "Stopped by user."
            system_state.manual_bot["why_waiting"] = "Stopped by user."

    system_state.save_manual_bot()
    await broadcast_system_state()
    return {"status": "SUCCESS", "message": "Manual BOT stopped."}

@app.post("/api/manual-bot/delete")
async def api_manual_bot_delete(payload: ManualBotDeletePayload, token: str = None, authorization: str = Header(None)):
    verify_token(token, authorization)
    bot_id = payload.id
    if hasattr(system_state, "manual_bots") and isinstance(system_state.manual_bots, list):
        system_state.manual_bots = [b for b in system_state.manual_bots if str(b.get("id")) != str(bot_id)]
    system_state.save_manual_bot()
    system_state.log(f"[MANUAL BOT] Task #{bot_id} deleted by user.")
    await broadcast_system_state()
    return {"status": "SUCCESS", "message": f"Task #{bot_id} deleted."}

class TAConfigItem(BaseModel):
    month_idx: int
    entry_diff: float
    averaging_step: float
    exit_gap: float
    quantity: int
    direction: str
    paper_mode: bool
    enabled: bool
    max_orders: Optional[int] = 5

class TAConfigPayload(BaseModel):
    configs: List[TAConfigItem]

@app.post("/api/ta-config")
async def api_post_ta_config(payload: TAConfigPayload, token: str = None, authorization: str = Header(None)):
    verify_token(token, authorization)
    
    session_status = get_market_session_status()
    if session_status == "SUSPENDED":
        has_enabled = any(c.dict().get("enabled") for c in payload.configs)
        if has_enabled:
            raise HTTPException(status_code=400, detail="Cannot enable Trade Automation configs after market close (23:27 - 09:00).")

    system_state.ta_configs = [c.dict() for c in payload.configs]
    system_state.save_ta_configs()
    system_state.log(f"Trade Automation configs updated: {len(system_state.ta_configs)} active instance(s).")
    await broadcast_system_state()
    return {"status": "SUCCESS", "message": "Trade Automation configurations updated successfully."}

@app.post("/api/clear-ta-trades")
async def api_clear_ta_trades(token: str = None, authorization: str = Header(None)):
    verify_token(token, authorization)
    count = len(system_state.ta_trades)
    system_state.ta_trades = []
    system_state.save_ta_trades()
    system_state.log(f"[TA] Cleared all {count} Trade Automation trades.")
    await broadcast_system_state()
    return {"status": "SUCCESS", "message": f"Cleared all {count} Trade Automation trades successfully."}

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
        
    # Find config for the trade's month mapping to get the paper mode, fallback to trade's own paper_mode
    paper_mode = trade.get("paper_mode", True)
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

class TAEditTradePayload(BaseModel):
    trade_id: Union[int, str]
    exit_gap: Optional[float] = None
    target_spread: Optional[float] = None
    trade_type: Optional[str] = None

@app.post("/api/ta-edit-trade")
async def api_ta_edit_trade(payload: TAEditTradePayload, token: str = None, authorization: str = Header(None)):
    verify_token(token, authorization)
    
    trade = None
    is_lots = False
    target_id_str = str(payload.trade_id).strip()
    req_type = (payload.trade_type or "").strip().lower()
    
    # 1. Search specified list first if trade_type is provided
    if req_type == "lots":
        for t in getattr(system_state, "ta_lots_trades", []):
            if str(t.get("id", "")).strip() == target_id_str:
                trade = t
                is_lots = True
                break
    elif req_type == "ta":
        for t in system_state.ta_trades:
            if str(t.get("id", "")).strip() == target_id_str:
                trade = t
                is_lots = False
                break
                
    # 2. If not found or if matched trade is already closed/inactive,
    # PRIORITIZE finding an OPEN trade across both lists!
    if not trade or str(trade.get("status", "")).strip().lower() not in ["open", "active"]:
        open_lots_candidate = None
        for t in getattr(system_state, "ta_lots_trades", []):
            if str(t.get("id", "")).strip() == target_id_str and str(t.get("status", "")).strip().lower() in ["open", "active"]:
                open_lots_candidate = t
                break
                
        open_ta_candidate = None
        for t in system_state.ta_trades:
            if str(t.get("id", "")).strip() == target_id_str and str(t.get("status", "")).strip().lower() in ["open", "active"]:
                open_ta_candidate = t
                break
                
        if req_type == "lots" and open_lots_candidate:
            trade = open_lots_candidate
            is_lots = True
        elif req_type == "ta" and open_ta_candidate:
            trade = open_ta_candidate
            is_lots = False
        elif open_lots_candidate:
            trade = open_lots_candidate
            is_lots = True
        elif open_ta_candidate:
            trade = open_ta_candidate
            is_lots = False

    # 3. Fallback to any trade matching ID if still none found (to give proper error response)
    if not trade:
        if req_type == "lots":
            for t in getattr(system_state, "ta_lots_trades", []):
                if str(t.get("id", "")).strip() == target_id_str:
                    trade = t
                    is_lots = True
                    break
        else:
            for t in system_state.ta_trades:
                if str(t.get("id", "")).strip() == target_id_str:
                    trade = t
                    is_lots = False
                    break
            if not trade:
                for t in getattr(system_state, "ta_lots_trades", []):
                    if str(t.get("id", "")).strip() == target_id_str:
                        trade = t
                        is_lots = True
                        break
                
    if not trade:
        raise HTTPException(status_code=404, detail=f"Trade Automation trade #{target_id_str} not found.")
        
    if str(trade.get("status", "")).strip().lower() not in ["open", "active"]:
        raise HTTPException(status_code=400, detail=f"Trade #{trade.get('id')} is already {trade.get('status', 'Closed')}. Only OPEN trades can have their target edited.")
        
    direction = str(trade.get("direction", "Expansion")).capitalize()
    entry_spread = float(trade.get("entry_spread", 0.0))
    old_gap = float(trade.get("exit_gap", 100.0))
    
    if payload.exit_gap is not None and str(payload.exit_gap).strip() != "":
        new_gap = float(payload.exit_gap)
    elif payload.target_spread is not None and str(payload.target_spread).strip() != "":
        target_spread = float(payload.target_spread)
        if direction == "Expansion":
            new_gap = target_spread - entry_spread
        else:
            new_gap = entry_spread - target_spread
    else:
        raise HTTPException(status_code=400, detail="Either exit_gap or target_spread must be provided.")
        
    if new_gap <= 0:
        raise HTTPException(status_code=400, detail=f"Target gap must be greater than 0 (got {new_gap:.2f}).")
        
    trade["exit_gap"] = round(new_gap, 2)
    if is_lots:
        system_state.save_ta_lots_trades()
    else:
        system_state.save_ta_trades()
    
    target_val = (entry_spread + new_gap) if direction == "Expansion" else (entry_spread - new_gap)
    system_state.log(f"[TA EDIT] {'[LOTS] ' if is_lots else ''}Trade ID {trade['id']} target updated: Gap {old_gap:.2f} -> {new_gap:.2f} (Target Spread: {target_val:.2f})")
    
    await broadcast_system_state()
    return {
        "status": "SUCCESS", 
        "message": f"Trade ID {trade['id']} target updated successfully.", 
        "exit_gap": round(new_gap, 2), 
        "target_spread": round(target_val, 2)
    }

# ----------------- Trade Automation with Lots REST Endpoints -----------------
class TALotsConfigItem(BaseModel):
    month_idx: int
    entry_diff: float
    averaging_step: float
    exit_gap: float
    lots_str: str
    lots_list: Optional[List[int]] = None
    direction: str
    paper_mode: bool
    enabled: bool
    level_index: Optional[int] = None
    lot_index: Optional[int] = None
    last_order_time: Optional[float] = None

class TALotsConfigPayload(BaseModel):
    configs: List[TALotsConfigItem]

@app.post("/api/ta-lots-config")
async def api_post_ta_lots_config(payload: TALotsConfigPayload, token: str = None, authorization: str = Header(None)):
    verify_token(token, authorization)
    
    session_status = get_market_session_status()
    if session_status == "SUSPENDED":
        has_enabled = any(c.dict().get("enabled") for c in payload.configs)
        if has_enabled:
            raise HTTPException(status_code=400, detail="Cannot enable Trade Automation with Lots configs after market close (23:27 - 09:00).")

    configs_list = []
    old_configs = getattr(system_state, "ta_lots_configs", [])
    for idx, c in enumerate(payload.configs):
        c_dict = c.dict()
        # Parse lots list if not already parsed
        if not c_dict.get("lots_list") and c_dict.get("lots_str"):
            try:
                c_dict["lots_list"] = [int(x.strip()) for x in str(c_dict["lots_str"]).split(",") if x.strip() and int(x.strip()) > 0]
            except Exception:
                c_dict["lots_list"] = [1]

        # Match with old config to preserve progress if not explicitly passed
        matching_old = None
        for old in old_configs:
            if old.get("month_idx") == c_dict.get("month_idx") and old.get("direction") == c_dict.get("direction"):
                matching_old = old
                break

        if matching_old:
            if c_dict.get("level_index") is None:
                c_dict["level_index"] = matching_old.get("level_index", 0)
            if c_dict.get("lot_index") is None:
                c_dict["lot_index"] = matching_old.get("lot_index", 0)
            if c_dict.get("last_order_time") is None:
                c_dict["last_order_time"] = matching_old.get("last_order_time", 0.0)
        else:
            if c_dict.get("level_index") is None:
                c_dict["level_index"] = 0
            if c_dict.get("lot_index") is None:
                c_dict["lot_index"] = 0
            if c_dict.get("last_order_time") is None:
                c_dict["last_order_time"] = 0.0

        configs_list.append(c_dict)

    system_state.ta_lots_configs = configs_list
    system_state.save_ta_lots_configs()
    system_state.log(f"Trade Automation with Lots configs updated: {len(system_state.ta_lots_configs)} instance(s).")
    await broadcast_system_state()
    return {"status": "SUCCESS", "message": "Trade Automation with Lots configurations updated successfully."}

@app.post("/api/clear-ta-lots-trades")
async def api_clear_ta_lots_trades(token: str = None, authorization: str = Header(None)):
    verify_token(token, authorization)
    count = len(getattr(system_state, "ta_lots_trades", []))
    system_state.ta_lots_trades = []
    system_state.save_ta_lots_trades()
    for cfg in getattr(system_state, "ta_lots_configs", []):
        cfg["level_index"] = 0
        cfg["lot_index"] = 0
    system_state.save_ta_lots_configs()
    system_state.log(f"[TA LOTS] Cleared all {count} Trade Automation with Lots trades and reset ladders.")
    await broadcast_system_state()
    return {"status": "SUCCESS", "message": f"Cleared all {count} Trade Automation with Lots trades successfully."}

class TALotsExitTradePayload(BaseModel):
    trade_id: int

@app.post("/api/ta-lots-exit-trade")
async def api_ta_lots_exit_trade(payload: TALotsExitTradePayload, token: str = None, authorization: str = Header(None)):
    verify_token(token, authorization)
    
    trade = None
    for t in getattr(system_state, "ta_lots_trades", []):
        if t["id"] == payload.trade_id:
            trade = t
            break
            
    if not trade:
        raise HTTPException(status_code=404, detail="Trade Automation with Lots trade not found.")
        
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
        
    paper_mode = trade.get("paper_mode", True)
    for config in getattr(system_state, "ta_lots_configs", []):
        idx = config.get("month_idx", -1)
        if 0 <= idx < len(system_state.month_master):
            m = system_state.month_master[idx]
            if m["petal_symbol"] == trade["petal_symbol"] and m["mini_symbol"] == trade["mini_symbol"]:
                paper_mode = config.get("paper_mode", True)
                break
                
    system_state.log(f"[TA LOTS MANUAL EXIT] Squaring off Trade ID {trade['id']}...")
    await run_ta_lots_exit(trade, mapping, paper_mode)
    return {"status": "SUCCESS", "message": "Trade Automation with Lots trade closed successfully."}

class TALotsDismissTradePayload(BaseModel):
    trade_id: int

@app.post("/api/ta-lots-dismiss-trade")
async def api_ta_lots_dismiss_trade(payload: TALotsDismissTradePayload, token: str = None, authorization: str = Header(None)):
    verify_token(token, authorization)
    system_state.ta_lots_trades = [t for t in getattr(system_state, "ta_lots_trades", []) if t["id"] != payload.trade_id]
    system_state.save_ta_lots_trades()
    await broadcast_system_state()
    return {"status": "SUCCESS", "message": "Trade dismissed successfully."}

@app.post("/api/update-rules")
async def api_update_rules(payload: UpdateParamsPayload, token: str = None, authorization: str = Header(None)):
    verify_token(token, authorization)
    
    session_status = get_market_session_status()
    if session_status == "SUSPENDED":
        if payload.auto_trading_enabled:
            raise HTTPException(status_code=400, detail="Cannot enable Auto Trading after market close (23:27 - 09:00).")

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
    system_state.save_rules()
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

def generate_spread_price_high_low_csv() -> StringIO:
    # Ensure current live contracts are recorded in stats before export
    now_ist = get_ist_time()
    today_str = now_ist.strftime("%Y-%m-%d")
    if system_state.petal_symbol and system_state.mini_symbol:
        p_ltp = system_state.gold_petal_ltp
        m_ltp = system_state.gold_mini_ltp
        if p_ltp > 0 and m_ltp > 0:
            update_daily_high_low_stats(
                system_state.petal_symbol,
                system_state.mini_symbol,
                system_state.spread,
                p_ltp,
                m_ltp
            )

    csv_buffer = StringIO()
    writer = csv.writer(csv_buffer)
    writer.writerow([
        "Date",
        "Time (IST)",
        "Month",
        "Contract Group",
        "Petal Symbol",
        "Mini Symbol",
        "Spread High",
        "Spread Low",
        "Current Spread",
        "Petal Price High",
        "Petal Price Low",
        "Current Petal Price",
        "Mini Price High",
        "Mini Price Low",
        "Current Mini Price",
        "Open Spread (09:05)",
        "Close Spread (23:27)"
    ])

    all_dates = sorted(system_state.daily_spread_stats.keys(), reverse=True)
    for d in all_dates:
        day_dict = system_state.daily_spread_stats[d]
        for pair_key, record in day_dict.items():
            s_high = record.get("high_spread")
            s_low = record.get("low_spread")
            s_cur = record.get("last_spread")
            
            p_high = record.get("petal_high")
            p_low = record.get("petal_low")
            p_cur = record.get("petal_last")
            
            m_high = record.get("mini_high")
            m_low = record.get("mini_low")
            m_cur = record.get("mini_last")
            
            open_val = record.get("open_spread_905")
            close_val = record.get("close_spread_2327")
            
            writer.writerow([
                record.get("date", d),
                record.get("last_updated", "--"),
                record.get("month", extract_month_from_symbol(record.get("petal_symbol", ""))),
                record.get("month_group", pair_key),
                record.get("petal_symbol", "--"),
                record.get("mini_symbol", "--"),
                f"{s_high:.2f}" if s_high is not None else "--",
                f"{s_low:.2f}" if s_low is not None else "--",
                f"{s_cur:.2f}" if s_cur is not None else "--",
                f"{p_high:.2f}" if p_high is not None else "--",
                f"{p_low:.2f}" if p_low is not None else "--",
                f"{p_cur:.2f}" if p_cur is not None else "--",
                f"{m_high:.2f}" if m_high is not None else "--",
                f"{m_low:.2f}" if m_low is not None else "--",
                f"{m_cur:.2f}" if m_cur is not None else "--",
                f"{open_val:.2f}" if open_val is not None else "--",
                f"{close_val:.2f}" if close_val is not None else "--"
            ])

    csv_buffer.seek(0)
    return csv_buffer

# REST CSV Spread & Price High / Low Report exporter endpoint
@app.get("/api/export-depth-spread")
async def api_export_depth_spread(token: str = None, authorization: str = Header(None)):
    verify_token(token, authorization)
    
    csv_buffer = generate_spread_price_high_low_csv()
    content = csv_buffer.getvalue()
    
    try:
        csv_file = get_data_filepath("depth_spread_history.csv")
        with open(csv_file, "w", encoding="utf-8") as f:
            f.write(content)
    except Exception:
        pass

    headers = {"Content-Disposition": "attachment; filename=spread_price_high_low_report.csv"}
    return StreamingResponse(iter([content]), media_type="text/csv", headers=headers)

# REST CSV Active & Pending Manual Trades exporter endpoint (Cleaned with IST Timestamps)
@app.get("/api/export-manual-csv")
async def api_export_manual_csv(token: str = None, authorization: str = Header(None)):
    verify_token(token, authorization)
    
    csv_buffer = StringIO()
    writer = csv.writer(csv_buffer)
    
    # Cleaned Headers (10 unwanted columns removed, explicit IST time)
    writer.writerow([
        "Manual Trade ID", "Entry Date", "Entry Time (IST)", "Leg 1 Symbol", "Leg 2 Symbol", 
        "Direction", "Status", "Quantity", "Trigger Diff Target", 
        "Actual Entry Spread", "Expected Entry Spread", "Entry Slippage", 
        "Petal Entry Price", "Mini Entry Price", 
        "Exit Date", "Exit Time (IST)", "Actual Exit Spread", "Expected Exit Spread", "Exit Slippage",
        "Petal Exit Price", "Mini Exit Price"
    ])
    
    # Records
    for trade in system_state.manual_trades:
        writer.writerow([
            trade.get("id"),
            trade.get("entry_date"),
            trade.get("entry_time"),
            trade.get("petal_symbol", system_state.petal_symbol),
            trade.get("mini_symbol", system_state.mini_symbol),
            trade.get("direction"),
            trade.get("status"),
            trade.get("quantity"),
            trade.get("trigger_diff") if trade.get("trigger_diff") is not None else "Immediate",
            trade.get("entry_spread"),
            trade.get("expected_entry_spread"),
            trade.get("entry_slippage"),
            trade.get("petal_entry_price"),
            trade.get("mini_entry_price"),
            trade.get("exit_date"),
            trade.get("exit_time"),
            trade.get("actual_exit_spread"),
            trade.get("exit_spread"),
            trade.get("exit_slippage"),
            trade.get("petal_exit_price"),
            trade.get("mini_exit_price")
        ])
        
    csv_buffer.seek(0)
    headers = {"Content-Disposition": "attachment; filename=manual_trades.csv"}
    return StreamingResponse(iter([csv_buffer.getvalue()]), media_type="text/csv", headers=headers)

# REST CSV Month Master Daily Spreads exporter endpoint (Point 1 & 2)
@app.get("/api/export-month-master-spreads-csv")
async def api_export_month_master_spreads_csv(token: str = None, authorization: str = Header(None)):
    verify_token(token, authorization)
    
    csv_buffer = generate_spread_price_high_low_csv()
    content = csv_buffer.getvalue()
    
    try:
        csv_file = get_data_filepath("month_master_daily_spreads.csv")
        with open(csv_file, "w", encoding="utf-8") as f:
            f.write(content)
    except Exception:
        pass

    headers = {"Content-Disposition": "attachment; filename=month_master_daily_spreads.csv"}
    return StreamingResponse(iter([content]), media_type="text/csv", headers=headers)

# REST CSV All Trades Master exporter endpoint (Point 3)
@app.get("/api/export-all-trades-csv")
async def api_export_all_trades_csv(token: str = None, authorization: str = Header(None)):
    verify_token(token, authorization)
    
    csv_buffer = StringIO()
    writer = csv.writer(csv_buffer)
    
    # Headers with Order Type first
    writer.writerow([
        "Order Type", "Trade ID", "Status", "Date", "Entry Time (IST)", "Exit Time (IST)",
        "Leg 1 Symbol", "Leg 2 Symbol", "Direction", "Quantity",
        "Target Diff", "Filled Entry Spread", "Actual Exit Spread",
        "Petal Entry Price", "Mini Entry Price", "Petal Exit Price", "Mini Exit Price",
        "Net PnL", "Charges", "Details"
    ])
    
    # 1. Manual & Manual BOT Trades
    for trade in system_state.manual_trades:
        source = trade.get("trade_source", "MANUAL")
        writer.writerow([
            source,
            trade.get("id"),
            trade.get("status"),
            trade.get("entry_date", "--"),
            trade.get("entry_time", "--"),
            trade.get("exit_time", "--"),
            trade.get("petal_symbol", system_state.petal_symbol),
            trade.get("mini_symbol", system_state.mini_symbol),
            trade.get("direction"),
            trade.get("quantity"),
            trade.get("trigger_diff") if trade.get("trigger_diff") is not None else "--",
            trade.get("entry_spread", "--"),
            trade.get("actual_exit_spread", "--"),
            trade.get("petal_entry_price", "--"),
            trade.get("mini_entry_price", "--"),
            trade.get("petal_exit_price", "--"),
            trade.get("mini_exit_price", "--"),
            trade.get("pnl", "--"),
            trade.get("charges", "--"),
            trade.get("reason", "Manual Trade")
        ])
        
    # 2. Trade Automation Trades
    for trade in getattr(system_state, "ta_trades", []):
        writer.writerow([
            "TRADE_AUTOMATION",
            trade.get("id"),
            trade.get("status"),
            trade.get("entry_date", "--"),
            trade.get("entry_time", "--"),
            trade.get("exit_time", "--"),
            trade.get("petal_symbol", "--"),
            trade.get("mini_symbol", "--"),
            trade.get("direction"),
            trade.get("quantity"),
            trade.get("entry_diff", "--"),
            trade.get("entry_spread", "--"),
            trade.get("exit_spread", "--"),
            trade.get("petal_entry_price", "--"),
            trade.get("mini_entry_price", "--"),
            trade.get("petal_exit_price", "--"),
            trade.get("mini_exit_price", "--"),
            trade.get("pnl", "--"),
            trade.get("charges", "--"),
            f"Exit Gap: {trade.get('exit_gap', '--')}"
        ])
        
    # 3. Automated Strategy Completed History
    for trade in system_state.trade_history:
        writer.writerow([
            trade.get("reason", "AUTO_ROBOT"),
            trade.get("id"),
            trade.get("status", "COMPLETED"),
            trade.get("date", "--"),
            trade.get("entry_time", "--"),
            trade.get("exit_time", "--"),
            system_state.petal_symbol,
            system_state.mini_symbol,
            trade.get("direction"),
            trade.get("quantity", system_state.trade_quantity),
            trade.get("entry_spread", "--"),
            trade.get("actual_entry_spread", "--"),
            trade.get("actual_exit_spread", "--"),
            trade.get("petal_entry", "--"),
            trade.get("mini_entry", "--"),
            trade.get("petal_exit", "--"),
            trade.get("mini_exit", "--"),
            trade.get("pnl", "--"),
            trade.get("charges", "--"),
            trade.get("details", "--")
        ])
        
    csv_buffer.seek(0)
    headers = {"Content-Disposition": "attachment; filename=all_trades_history.csv"}
    return StreamingResponse(iter([csv_buffer.getvalue()]), media_type="text/csv", headers=headers)

# ----------------- 🧠 AI Arbitrage Intelligence Hub Endpoints -----------------
@app.get("/api/ai/dashboard-data")
async def api_get_ai_dashboard_data(token: str = None, authorization: str = Header(None)):
    verify_token(token, authorization)
    today_str = get_ist_time_str("%Y-%m-%d")
    # Sirf scanner ke asli signals (aaj ke) - koi simulated/demo entry nahi
    signals = [o for o in system_state.ai_opportunities_log if o.get("date") == today_str]
    return {
        "status": "SUCCESS",
        "matrix": ai_arbitrage_engine.get_multi_pair_matrix(),
        "signals": signals,
        "summary": ai_arbitrage_engine.get_opportunities_summary_from_list(signals),
        "threshold": getattr(system_state, "ai_divergence_threshold", 80.0),
        "has_claude_api_key": bool(ai_arbitrage_engine.get_api_key())
    }


class SetClaudeKeyPayload(BaseModel):
    api_key: str

@app.post("/api/ai/set-claude-key")
async def api_set_claude_key(payload: SetClaudeKeyPayload, token: str = None, authorization: str = Header(None)):
    verify_token(token, authorization)
    clean_key = payload.api_key.strip()
    if not clean_key:
        raise HTTPException(status_code=400, detail="API key cannot be empty")

    if not clean_key.startswith("sk-ant-"):
        raise HTTPException(status_code=400, detail="Invalid API key format. Anthropic Claude keys must start with 'sk-ant-'")

    # Pre-flight check: Validate key directly against Anthropic API
    validation_err = ""
    models_found = []
    try:
        test_headers = {
            "x-api-key": clean_key,
            "anthropic-version": "2023-06-01"
        }
        async with aiohttp.ClientSession() as session:
            async with session.get("https://api.anthropic.com/v1/models", headers=test_headers, timeout=aiohttp.ClientTimeout(total=6)) as test_resp:
                if test_resp.status == 200:
                    m_data = await test_resp.json()
                    models_found = [x.get("id") for x in m_data.get("data", []) if x.get("id")]
                elif test_resp.status == 401:
                    t_err = await test_resp.text()
                    try:
                        j_err = json.loads(t_err)
                        validation_err = j_err.get("error", {}).get("message", "HTTP 401 Unauthorized")
                    except Exception:
                        validation_err = "HTTP 401 Unauthorized"
    except Exception as e:
        logger.info(f"[CLAUDE VALIDATE] Preflight check note: {e}")

    if validation_err:
        raise HTTPException(
            status_code=400,
            detail=f"Anthropic rejected this API key ({validation_err}). Please click 'Create Key' in console.anthropic.com and copy the newly generated secret key (starts with sk-ant-api03-...)."
        )
    
    # Set in memory
    system_state.anthropic_api_key = clean_key
    os.environ["ANTHROPIC_API_KEY"] = clean_key
    
    # Save directly to .env
    env_file = os.path.join(BASE_DIR, ".env")
    lines = []
    found = False
    if os.path.exists(env_file):
        try:
            with open(env_file, "r", encoding="utf-8") as f:
                lines = f.readlines()
            for idx, l in enumerate(lines):
                if l.strip().startswith("ANTHROPIC_API_KEY="):
                    lines[idx] = f"ANTHROPIC_API_KEY={clean_key}\n"
                    found = True
                    break
        except Exception:
            pass
    if not found:
        lines.append(f"ANTHROPIC_API_KEY={clean_key}\n")
    try:
        with open(env_file, "w", encoding="utf-8") as f:
            f.writelines(lines)
    except Exception as e:
        logger.warning(f"Failed to persist key to .env: {e}")

    # Clear cached analysis and run fresh analysis with Claude
    ai_arbitrage_engine._last_ai_analysis = None
    new_advisory = await ai_arbitrage_engine.generate_claude_advisory()
    return {
        "status": "SUCCESS",
        "message": "Claude API Key validated & saved successfully to server.",
        "models_found": models_found,
        "advisory": new_advisory
    }

class SetThresholdPayload(BaseModel):
    threshold: float

@app.post("/api/ai/set-threshold")
async def api_set_ai_threshold(payload: SetThresholdPayload, token: str = None, authorization: str = Header(None)):
    verify_token(token, authorization)
    if payload.threshold < 10.0 or payload.threshold > 500.0:
        raise HTTPException(status_code=400, detail="Threshold must be between 10.0 and 500.0 points")
    system_state.ai_divergence_threshold = round(payload.threshold, 1)
    return {
        "status": "SUCCESS",
        "threshold": system_state.ai_divergence_threshold,
        "message": f"AI Scanner threshold set to {system_state.ai_divergence_threshold} pts."
    }

class AnalyzeClaudePayload(BaseModel):
    query: Optional[str] = ""

@app.post("/api/ai/analyze-claude")
async def api_analyze_claude(payload: AnalyzeClaudePayload = None, token: str = None, authorization: str = Header(None)):
    verify_token(token, authorization)
    user_q = payload.query if payload else ""
    advisory = await ai_arbitrage_engine.generate_claude_advisory(user_query=user_q)
    return {
        "status": "SUCCESS",
        "advisory": advisory
    }

@app.post("/api/ai/clear-opportunities")
async def api_clear_opportunities(token: str = None, authorization: str = Header(None)):
    verify_token(token, authorization)
    system_state.ai_opportunities_log = []
    system_state.save_ai_opportunities()
    return {"status": "SUCCESS", "message": "Today's opportunity log cleared."}

@app.get("/api/ai/monitor")
async def api_ai_monitor(token: str = None, authorization: str = Header(None)):
    verify_token(token, authorization)
    return {"status": "SUCCESS", **ai_arbitrage_engine.get_open_trades_monitor()}

@app.get("/api/ai/journal")
async def api_ai_journal(date: str = None, token: str = None, authorization: str = Header(None)):
    verify_token(token, authorization)
    date_str = date or get_ist_time_str("%Y-%m-%d")
    try:
        datetime.strptime(date_str, "%Y-%m-%d")
    except ValueError:
        raise HTTPException(status_code=400, detail="Date format YYYY-MM-DD hona chahiye")
    return {"status": "SUCCESS", **ai_arbitrage_engine.get_daily_journal(date_str)}

@app.get("/api/ai/backtest")
async def api_ai_backtest(days: int = 30, token: str = None, authorization: str = Header(None)):
    verify_token(token, authorization)
    days = max(1, min(365, days))
    result = await asyncio.get_running_loop().run_in_executor(None, ai_arbitrage_engine.run_backtest, days)
    return {"status": "SUCCESS", **result}

@app.get("/api/ai/alerts")
async def api_ai_alerts(after: int = 0, token: str = None, authorization: str = Header(None)):
    verify_token(token, authorization)
    return {"status": "SUCCESS", "alerts": ai_arbitrage_engine.get_alerts(after)}

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

@app.get("/manifest.json")
async def get_manifest():
    return FileResponse(os.path.join(BASE_DIR, "manifest.json"), media_type="application/manifest+json")

@app.get("/icon.svg")
async def get_icon():
    return FileResponse(os.path.join(BASE_DIR, "icon.svg"), media_type="image/svg+xml")

@app.get("/apple-touch-icon.png")
async def get_apple_touch_icon():
    return FileResponse(os.path.join(BASE_DIR, "icon.svg"), media_type="image/svg+xml")

if __name__ == "__main__":
    host = os.getenv("HOST", "127.0.0.1")
    port = int(os.getenv("PORT", 7890))
    logger.info(f"Starting server on http://{host}:{port}")
    uvicorn.run("main:app", host=host, port=port, log_level="info")

