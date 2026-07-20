# Logic Guide: Gold Spread Arbitrage Paper Trading

This document explains the step-by-step logic, mathematics, and simulations used in the **Gold Spread Arbitrage Terminal** when running in **Paper Trading Mode**. Use this as a reference to understand how the bot operates, calculates profits, and simulates trades.

---

## 📌 1. Core Concept: Spread Arbitrage (100:1 Ratio)

The strategy tracks pricing differences between two highly correlated Gold contracts on the Multi Commodity Exchange (MCX):
1. **Leg 1: GOLDPETAL** (Lot size = 1 Gram, Price quoted per 1 Gram)
2. **Leg 2: GOLDMINI** (Lot size = 100 Grams, Price quoted per 10 Grams)

To maintain a balanced position (where the physical amount of gold is equal on both sides), the system enforces a **100:1 quantity ratio**:
* **GOLDPETAL Quantity**: `100 * Trade Quantity` (Equivalent to `100 * Trade Quantity` grams)
* **GOLDMINI Quantity**: `Trade Quantity` (Equivalent to `Trade Quantity * 100` grams)

*Example:* If your trade quantity is `1` lot, the bot will trade **100 lots of GOLDPETAL** and **1 lot of GOLDMINI**. This balances exactly 100 grams of gold on both legs.

---

## 📊 2. Price and Spread Math

To compare the prices on a common 10-Gram gold scale (creating a clean 4-digit representation for UI display and threshold matching), the GOLDPETAL price is multiplied by 10.0 and GOLDMINI is kept at 1.0 (quoted per 10 grams):

$$\text{Spread} = (\text{GOLDPETAL LTP} \times 10.0) - \text{GOLDMINI LTP}$$

### Live Depth-Based Spreads
Arbitrage trades are scanned and executed based on the volume-weighted average price (VWAP) spreads calculated from depth:
1. **Depth Buy Spread (Entry for Expansion / Exit for Contraction)**:
   * We BUY GOLDPETAL $\rightarrow$ matched at GOLDPETAL Ask price (lowest seller price).
   * We SELL GOLDMINI $\rightarrow$ matched at GOLDMINI Bid price (highest buyer price).
   * $$\text{depth\_buy\_spread} = (\text{Avg Petal Ask} \times 10.0) - \text{Avg Mini Bid}$$

2. **Depth Sell Spread (Exit for Expansion / Entry for Contraction)**:
   * We SELL GOLDPETAL $\rightarrow$ matched at GOLDPETAL Bid price (highest buyer price).
   * We BUY GOLDMINI $\rightarrow$ matched at GOLDMINI Ask price (lowest seller price).
   * $$\text{depth\_sell\_spread} = (\text{Avg Petal Bid} \times 10.0) - \text{Avg Mini Ask}$$

> [!NOTE]
> Since we trade a 100-gram physical transaction but display spreads on a 10-gram scale, the **actual Rupees P&L is equal to 10 times the spread change**. E.g., if the spread expands by `40` (from `1,600` to `1,640`), the net profit is $40 \times 10 = 400$ Rupees (for quantity = 1).

---

## 🛡️ 3. Execution Shield: Depth Guard & Liquidity Check

Before initiating any trade, the system runs strict validation checks to prevent slippage and broker errors:

1. **Option A: Depth Guard Check**: 
   * The bot verifies that valid depth levels exist in GOLDPETAL and GOLDMINI for the execution sides (e.g., Ask side for buying, Bid side for selling). 
   * If depth data is missing or empty for either leg, the trade is immediately aborted/skipped. LTP is never used to initialize orders.
2. **Quantity Check**: Ensures there is enough volume in the order book to fill the required quantities (`100 * qty` for Petal, `qty` for Mini).
3. **Bid-Ask Spread Check (Slippage Shield)**:
   * **GOLDPETAL**: The gap between the best Bid and best Ask price must be **$\le$ 15.0 INR**.
   * **GOLDMINI**: The gap between the best Bid and best Ask price must be **$\le$ 150.0 INR**.
   * *If the gap exceeds these limits, the trade is skipped/cancelled to protect you from bad fill prices.*

---

## ⚡ 4. Order Execution Flow

Trades are executed using **MARKET orders** directly, eliminating the sequential wait times and partial fill delays of limit orders.

### Execution Sequence:
1. **Spread Scanner**: The system continually tracks `depth_buy_spread` and `depth_sell_spread` using the volume-weighted average price (VWAP) computed up to the required order quantity.
2. **Instant Trigger**: Once a threshold is crossed, the bot triggers immediate market executions.
3. **Broker Execution**:
   * **Paper Trading Mode**: Orders are filled instantly at the computed VWAP prices.
   * **Live Trading Mode**: Direct market orders are dispatched concurrently to Angel One for both GOLDPETAL and GOLDMINI.
4. **Emergency Rollback**: If one leg executes successfully but the other fails or is rejected by the broker, the system instantly fires a reversing market order to close the filled leg. This ensures you are never left holding a naked, one-sided position.

---

## 💰 5. Profit & Loss (P&L) Calculations

P&L calculations are based on physical lot multipliers (`100` for Petal, `10` for Mini):

### Direction A: Spread Expansion (Buy GOLDPETAL, Sell GOLDMINI)
Entered when the spread is low (narrow) and expected to widen.
* **GOLDPETAL P&L**: $(\text{Current LTP} - \text{Entry Price}) \times 100.0 \times \text{Quantity}$
* **GOLDMINI P&L**: $(\text{Entry Price} - \text{Current LTP}) \times 10.0 \times \text{Quantity}$

### Direction B: Spread Contraction (Sell GOLDPETAL, Buy GOLDMINI)
Entered when the spread is high (wide) and expected to narrow.
* **GOLDPETAL P&L**: $(\text{Entry Price} - \text{Current LTP}) \times 100.0 \times \text{Quantity}$
* **GOLDMINI P&L**: $(\text{Current LTP} - \text{Entry Price}) \times 10.0 \times \text{Quantity}$

### Portfolio Summary:
* **Unrealized P&L**: $\text{GOLDPETAL P\&L} + \text{GOLDMINI P\&L}$
* **Realized P&L**: Total P&L of all completed trades minus transactional charges.
* **Used Margin**: Blocked margin is fixed at **50,000 INR per Quantity** when in-position.
* **Available Balance**: $\text{Total Capital} - \text{Used Margin} + \text{Total P\&L}$

---

## 📈 6. Simulated MCX Charges (Tax & Brokerage)

To make paper trading results match real-world profits, the bot deducts realistic MCX charges for each complete trade cycle:

1. **Brokerage**: Flat **Rs. 20** per order. A full cycle has 4 orders (2 entries + 2 exits) = **Rs. 80.00**.
2. **Exchange Transaction Charges**: **0.0021%** of Total Turnover (Buy Value + Sell Value).
3. **Commodity Transaction Tax (CTT)**: **0.01%** on the Sell-side Turnover.
4. **SEBI Turnover Fee**: Rs. 10 per crore (**0.0000001** of Total Turnover).
5. **Stamp Duty**: **0.002%** on Buy-side Turnover.
6. **GST**: **18%** on (Brokerage + Exchange Charges).

$$\text{Net P\&L} = \text{Gross Trade P\&L} - \text{Total Charges}$$

---

## 🤖 7. Automated Strategy Rules

The bot can execute and exit trades autonomously using these parameters:

| Strategy Parameter | Type | Action Logic |
| :--- | :--- | :--- |
| **Auto Entry (Expansion)** | Entry | Enters when `depth_buy_spread` $\le$ `entry_threshold` + `spread_buffer` |
| **Auto Entry (Contraction)** | Entry | Enters when `depth_sell_spread` $\ge$ `target_threshold` - `spread_buffer` *(if auto contraction enabled)* |
| **Auto Spread Exit** | Exit | *For Expansion:* Exits when `depth_sell_spread` $\ge$ `target_threshold` - `spread_buffer`<br>*For Contraction:* Exits when `depth_buy_spread` $\le$ `entry_threshold` + `spread_buffer` |
| **Auto Target P&L** | Exit | Square off position if total trade unrealized P&L $\ge$ `auto_target_val` (e.g. +5,000 INR) |
| **Auto Stop Loss P&L** | Exit | Square off position if total trade unrealized P&L $\le$ `auto_sl_val` (e.g. -3,000 INR) |
| **Auto Square-Off Time** | Exit | Automatically exits all open positions when local time $\ge$ `auto_square_off_time` (e.g., 23:30) |
